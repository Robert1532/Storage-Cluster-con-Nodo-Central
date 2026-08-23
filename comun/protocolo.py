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

TIPOS DE MENSAJE
----------------
  cliente -> servidor : HELLO, METRIC, ACK
  servidor -> cliente : HELLO_OK, METRIC_OK, CMD

Responsable: Robert (Datos y Coordinacion). Los demas importan, no editan.
"""
from __future__ import annotations

import json
import socket
from datetime import datetime
from typing import Any, Iterator

CODIFICACION = "utf-8"
FIN_LINEA = b"\n"

# Acciones validas dentro de un CMD. Tienen que coincidir con el ENUM de la
# columna mensajes.accion en MySQL.
ACCION_MENSAJE = "MENSAJE"
ACCION_SET_INTERVAL = "SET_INTERVAL"

# Estados validos de un nodo. Coinciden con el ENUM de nodos.estado.
ESTADO_ACTIVO = "ACTIVO"
ESTADO_NO_REPORTA = "NO_REPORTA"


def ahora_iso() -> str:
    """Timestamp en ISO 8601 con milisegundos. Formato unico para todo el equipo."""
    return datetime.now().isoformat(timespec="milliseconds")


# --------------------------------------------------------------- constructores

def hello(node_id: str, region: str, hostname: str, so: str, intervalo: int) -> dict:
    return {
        "tipo": "HELLO",
        "node_id": node_id,
        "region": region,
        "hostname": hostname,
        "so": so,
        "intervalo": intervalo,
        "timestamp": ahora_iso(),
    }


def hello_ok(registrado: bool, nuevo: bool, intervalo: int) -> dict:
    # nuevo=True es la prueba en vivo del requisito 7.2 (alta automatica).
    return {
        "tipo": "HELLO_OK",
        "registrado": registrado,
        "nuevo": nuevo,
        "intervalo": intervalo,
        "timestamp": ahora_iso(),
    }


def metric(node_id: str, disco: dict) -> dict:
    """
    disco debe traer EXACTAMENTE estas claves:
      nombre, tipo, total_gb, usado_gb, libre_gb, uso_pct,
      iops_lectura, iops_escritura, latencia_ms
    """
    return {
        "tipo": "METRIC",
        "node_id": node_id,
        "timestamp": ahora_iso(),
        "disco": disco,
    }


def metric_ok() -> dict:
    return {"tipo": "METRIC_OK", "recibido": True, "timestamp": ahora_iso()}


def cmd(cmd_id: str, accion: str, texto: str | None = None, valor: int | None = None) -> dict:
    return {
        "tipo": "CMD",
        "cmd_id": cmd_id,
        "accion": accion,
        "texto": texto,
        "valor": valor,
        "timestamp": ahora_iso(),
    }


def ack(cmd_id: str, node_id: str) -> dict:
    # El cmd_id es lo que permite emparejar esta confirmacion con su mensaje.
    # Sin el, dos ACK seguidos son indistinguibles (pregunta 8 de la defensa).
    return {
        "tipo": "ACK",
        "cmd_id": cmd_id,
        "node_id": node_id,
        "recibido_en": ahora_iso(),
    }


# ------------------------------------------------------------------- transporte

def enviar(sock: socket.socket, mensaje: dict) -> None:
    """Serializa y manda un mensaje con su salto de linea. Thread-safe por socket."""
    datos = json.dumps(mensaje, ensure_ascii=False).encode(CODIFICACION) + FIN_LINEA
    sock.sendall(datos)


class LectorLineas:
    """
    Acumula bytes de un socket y entrega mensajes JSON completos.

    Uso:
        lector = LectorLineas(sock)
        for mensaje in lector:
            procesar(mensaje)

    El bucle termina solo cuando el otro extremo cierra la conexion.
    """

    def __init__(self, sock: socket.socket, tam_buffer: int = 4096) -> None:
        self.sock = sock
        self.tam_buffer = tam_buffer
        self._buffer = b""

    def __iter__(self) -> Iterator[dict[str, Any]]:
        while True:
            datos = self.sock.recv(self.tam_buffer)
            if not datos:            # el otro lado cerro la conexion
                return
            self._buffer += datos
            while FIN_LINEA in self._buffer:
                linea, self._buffer = self._buffer.split(FIN_LINEA, 1)
                linea = linea.strip()
                if not linea:
                    continue
                try:
                    yield json.loads(linea.decode(CODIFICACION))
                except json.JSONDecodeError:
                    # Una linea corrupta no puede tumbar la conexion entera.
                    continue
