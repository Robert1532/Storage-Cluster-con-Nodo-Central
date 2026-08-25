"""
CONTRATO DEL PROTOCOLO — tarea 0.3.

Este archivo es el unico lugar donde se define como hablan cliente y servidor.
Si alguien necesita cambiar un nombre de campo, se cambia AQUI y se avisa al
equipo. Nadie arma un JSON a mano en su modulo.

FRAMING
-------
TCP es un flujo de bytes: no respeta los limites de tus mensajes. Si el cliente
manda dos METRIC seguidos, el servidor puede recibirlos pegados en un solo
recv(), o partidos por la mitad. Por eso cada mensaje es un JSON en UNA linea
terminada en '\\n', y para leer se usa LectorLineas, que acumula lo que llega
y solo entrega lineas completas.

Esta es la pregunta 3 del banco de defensa y es la que mas gente falla.

ESCRITURA CONCURRENTE
--------------------
sendall() sobre un socket bloqueante puede escribir de a partes y reintentar.
Si DOS hilos escriben en el MISMO socket a la vez, sus bytes se intercalan y el
receptor ve lineas mezcladas que no parsean. Pasa en los dos extremos:

  servidor : el despachador manda CMD mientras el hilo del cliente manda METRIC_OK
  cliente  : el hilo receptor manda ACK mientras el principal manda METRIC

Por eso enviar() exige un candado por socket. Es un Lock POR CONEXION, no uno
global: dos nodos distintos pueden escribir en paralelo sin estorbarse.

TIPOS DE MENSAJE
----------------
  cliente -> servidor : HELLO, METRIC, ACK
  servidor -> cliente : HELLO_OK, METRIC_OK, CMD, ERROR

Responsable: Robert (Datos y Coordinacion). Los demas importan, no editan.
"""
from __future__ import annotations

import json
import socket
import threading
from datetime import datetime
from typing import Any, Iterator

CODIFICACION = "utf-8"
FIN_LINEA = b"\n"

# Un METRIC real pesa ~300 bytes. 64 KB es un techo generosisimo, y evita que
# alguien que se conecte por telnet y no mande '\n' nos coma la memoria.
MAX_LINEA = 64 * 1024

# Acciones validas dentro de un CMD. Coinciden con el ENUM de mensajes.accion.
ACCION_MENSAJE = "MENSAJE"
ACCION_SET_INTERVAL = "SET_INTERVAL"
ACCIONES_VALIDAS = (ACCION_MENSAJE, ACCION_SET_INTERVAL)

# Estados validos de un nodo. Coinciden con el ENUM de nodos.estado.
ESTADO_ACTIVO = "ACTIVO"
ESTADO_NO_REPORTA = "NO_REPORTA"

# Tipos de disco. Coinciden con el ENUM de metricas.disco_tipo.
TIPO_SSD = "SSD"
TIPO_HDD = "HDD"
TIPO_DESCONOCIDO = "DESCONOCIDO"
TIPOS_DISCO = (TIPO_SSD, TIPO_HDD, TIPO_DESCONOCIDO)

# Limites que impone el esquema de la base. Se validan antes de enviar para que
# un dato raro no reviente el INSERT del otro lado.
MAX_NODE_ID = 32
MAX_REGION = 64
MAX_HOSTNAME = 128
MAX_SO = 64
MAX_TEXTO = 255


class ErrorProtocolo(Exception):
    """La otra punta mando algo que no respeta el contrato."""


def ahora_iso() -> str:
    """
    Timestamp ISO 8601 con milisegundos Y OFFSET de zona horaria.

    El offset no es un adorno: sin el, el receptor no puede saber a que hora
    real corresponde el dato y termina asumiendo su propia zona. Como el
    servidor puede correr en una maquina con TZ distinta a la del cliente (un
    contenedor en UTC, por ejemplo), esa suposicion desplaza todas las metricas
    varias horas y las consultas por rango de tiempo dejan de encontrarlas.

    Formato: 2026-08-25T14:03:11.482-04:00
    """
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


# --------------------------------------------------------------- constructores

def hello(node_id: str, region: str, hostname: str, so: str, intervalo: int) -> dict:
    return {
        "tipo": "HELLO",
        "node_id": str(node_id)[:MAX_NODE_ID],
        "region": str(region)[:MAX_REGION],
        "hostname": str(hostname)[:MAX_HOSTNAME],
        "so": str(so)[:MAX_SO],
        "intervalo": int(intervalo),
        "timestamp": ahora_iso(),
    }


def hello_ok(registrado: bool, nuevo: bool, intervalo: int) -> dict:
    # nuevo=True es la prueba en vivo del requisito 7.2 (alta automatica).
    return {
        "tipo": "HELLO_OK",
        "registrado": registrado,
        "nuevo": nuevo,
        "intervalo": int(intervalo),
        "timestamp": ahora_iso(),
    }


def error(motivo: str) -> dict:
    """El servidor rechaza al cliente y le dice por que, en vez de cortar mudo."""
    return {"tipo": "ERROR", "motivo": str(motivo)[:MAX_TEXTO], "timestamp": ahora_iso()}


def metric(node_id: str, disco: dict) -> dict:
    """
    disco debe traer EXACTAMENTE estas claves:
      nombre, tipo, total_gb, usado_gb, libre_gb, uso_pct,
      iops_lectura, iops_escritura, latencia_ms
    """
    return {
        "tipo": "METRIC",
        "node_id": str(node_id)[:MAX_NODE_ID],
        "timestamp": ahora_iso(),
        "disco": disco,
    }


def metric_ok() -> dict:
    return {"tipo": "METRIC_OK", "recibido": True, "timestamp": ahora_iso()}


def cmd(cmd_id: str, accion: str, texto: str | None = None,
        valor: int | None = None) -> dict:
    return {
        "tipo": "CMD",
        "cmd_id": str(cmd_id),
        "accion": accion,
        "texto": None if texto is None else str(texto)[:MAX_TEXTO],
        "valor": None if valor is None else int(valor),
        "timestamp": ahora_iso(),
    }


def ack(cmd_id: str, node_id: str) -> dict:
    # El cmd_id es lo que permite emparejar esta confirmacion con su mensaje.
    # Sin el, dos ACK seguidos son indistinguibles (pregunta 8 de la defensa).
    return {
        "tipo": "ACK",
        "cmd_id": str(cmd_id),
        "node_id": str(node_id)[:MAX_NODE_ID],
        "recibido_en": ahora_iso(),
    }


# ------------------------------------------------------------------- transporte

def enviar(sock: socket.socket, mensaje: dict,
           candado: threading.Lock | None = None) -> None:
    """
    Serializa y manda un mensaje con su salto de linea.

    `candado` es OBLIGATORIO en la practica siempre que mas de un hilo pueda
    escribir en este socket. Se acepta None solo para pruebas de un solo hilo.
    """
    datos = json.dumps(mensaje, ensure_ascii=False).encode(CODIFICACION) + FIN_LINEA
    if candado is None:
        sock.sendall(datos)
        return
    with candado:
        sock.sendall(datos)


class LectorLineas:
    """
    Acumula bytes de un socket y entrega mensajes JSON completos.

        for mensaje in LectorLineas(sock):
            procesar(mensaje)

    El bucle termina cuando la otra punta cierra la conexion. Una linea corrupta
    (JSON invalido, bytes que no son UTF-8, o un JSON que no es un objeto) se
    descarta y se sigue: un paquete malo no puede tumbar la conexion entera.

    Un solo consumidor por instancia: comparte el buffer interno.
    """

    def __init__(self, sock: socket.socket, tam_buffer: int = 4096) -> None:
        self.sock = sock
        self.tam_buffer = tam_buffer
        self._buffer = b""
        self.descartadas = 0

    def __iter__(self) -> Iterator[dict[str, Any]]:
        while True:
            datos = self.sock.recv(self.tam_buffer)
            if not datos:            # la otra punta cerro la conexion
                return
            self._buffer += datos

            if FIN_LINEA not in self._buffer and len(self._buffer) > MAX_LINEA:
                # Nos estan mandando basura sin fin de linea. Cortamos en vez
                # de crecer hasta quedarnos sin memoria.
                self._buffer = b""
                raise ErrorProtocolo(
                    f"Linea de mas de {MAX_LINEA} bytes sin salto de linea")

            while FIN_LINEA in self._buffer:
                linea, self._buffer = self._buffer.split(FIN_LINEA, 1)
                linea = linea.strip()
                if not linea:
                    continue
                try:
                    obj = json.loads(linea.decode(CODIFICACION))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    self.descartadas += 1
                    continue
                if not isinstance(obj, dict):
                    # "null", "123" o "[1,2]" son JSON validos pero no mensajes.
                    self.descartadas += 1
                    continue
                yield obj
