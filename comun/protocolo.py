"""
CONTRATO DEL PROTOCOLO — version 2.

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

QUE CAMBIO EN LA VERSION 2  (ver docs/ACTUALIZACIONES.md)
---------------------------------------------------------
1. RECURSOS FLEXIBLES. Un METRIC ya no lleva solo `disco`: lleva ademas una
   lista `recursos` donde cada elemento es {tipo, nombre, metricas}. El tipo
   puede ser DISCO, RAM, CPU, RED o CUSTOM, y `metricas` es un diccionario
   libre de numeros. Agregar una metrica nueva (temperatura, swap, colas de
   E/S) NO exige tocar la base ni el servidor: el cliente la manda y queda
   guardada. El bloque `disco` se mantiene tal cual por compatibilidad: es el
   primer disco, que es lo que exige el enunciado.

2. SINCRONIZACION TRAS UNA CAIDA. El cliente guarda sus muestras en una base
   local aunque no haya red. Al reconectar manda METRIC_BATCH con todo lo que
   el servidor se perdio, en orden, y el servidor contesta SYNC_OK con el
   ultimo `seq` aceptado. Cada muestra lleva un `seq` que crece siempre, asi
   una retransmision no duplica filas.

3. LA HORA LA PONE EL SERVIDOR. Un cliente puede tener el reloj mal, o
   cambiarlo a proposito. Por eso cada muestra viaja con `mono_ns`, que es el
   reloj MONOTONICO del cliente (no se puede retroceder ni ajustar), y el
   servidor calcula la hora real asi:

       edad_de_la_muestra = (mono_ns_del_envio - mono_ns_de_la_muestra) / 1e9
       timestamp_real     = hora_del_servidor - edad_de_la_muestra

   El `timestamp` del cliente sigue viajando, pero es solo informativo: sirve
   para detectar y registrar el desvio (evento RELOJ_DESVIADO), nunca para
   guardar el dato. Cambiar la hora del cliente no mueve ni una fila.

TIPOS DE MENSAJE
----------------
  cliente -> servidor : HELLO, METRIC, METRIC_BATCH, ACK, PONG
  servidor -> cliente : HELLO_OK, METRIC_OK, SYNC_OK, CMD, ERROR

Responsable: Robert (Datos y Coordinacion). Los demas importan, no editan.
"""
from __future__ import annotations

import json
import socket
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

CODIFICACION = "utf-8"
FIN_LINEA = b"\n"

# Version del contrato. Viaja en el HELLO y en el HELLO_OK: si algun dia
# conviven un cliente viejo y un servidor nuevo, cada uno sabe con quien habla
# en vez de fallar con un KeyError a la tercera metrica.
VERSION_PROTOCOLO = 2

# Un METRIC con varios recursos pesa ~1 KB. Un METRIC_BATCH de 100 muestras
# ronda los 100 KB, asi que el techo de 64 KB de la version 1 lo cortaria. Se
# sube a 4 MB, que sigue siendo un techo (evita que alguien que se conecte por
# telnet y no mande '\n' nos coma la memoria) y deja sitio de sobra.
MAX_LINEA = 4 * 1024 * 1024

# Muestras por lote de sincronizacion. Con 500 no entra en un solo recv comodo
# y ademas bloquea el hilo del servidor demasiado tiempo en un solo mensaje.
MAX_MUESTRAS_LOTE = 100

# Acciones validas dentro de un CMD. Coinciden con el ENUM de mensajes.accion.
ACCION_MENSAJE = "MENSAJE"
ACCION_SET_INTERVAL = "SET_INTERVAL"
ACCION_SET_RECURSOS = "SET_RECURSOS"      # que recursos debe reportar el nodo
ACCION_PING = "PING"                      # latido de aplicacion (half-open)
ACCION_SOLICITAR_SYNC = "SOLICITAR_SYNC"  # "mandame lo que te quedo pendiente"
ACCIONES_VALIDAS = (ACCION_MENSAJE, ACCION_SET_INTERVAL, ACCION_SET_RECURSOS,
                    ACCION_PING, ACCION_SOLICITAR_SYNC)

# Estados validos de un nodo. Coinciden con el ENUM de nodos.estado.
ESTADO_ACTIVO = "ACTIVO"
ESTADO_NO_REPORTA = "NO_REPORTA"

# Tipos de disco. Coinciden con el ENUM de metricas.disco_tipo.
TIPO_SSD = "SSD"
TIPO_HDD = "HDD"
TIPO_USB = "USB"
TIPO_DESCONOCIDO = "DESCONOCIDO"
TIPOS_DISCO = (TIPO_SSD, TIPO_HDD, TIPO_USB, TIPO_DESCONOCIDO)

# Tipos de recurso que entiende el servidor. CUSTOM es la puerta abierta: un
# cliente puede mandar algo que no estaba previsto y se guarda igual.
REC_DISCO = "DISCO"
REC_RAM = "RAM"
REC_CPU = "CPU"
REC_RED = "RED"
REC_CUSTOM = "CUSTOM"
TIPOS_RECURSO = (REC_DISCO, REC_RAM, REC_CPU, REC_RED, REC_CUSTOM)

# Limites que impone el esquema de la base. Se validan antes de enviar para que
# un dato raro no reviente el INSERT del otro lado.
MAX_NODE_ID = 32
MAX_REGION = 64
MAX_SEDE = 64
MAX_HOSTNAME = 128
MAX_SO = 64
MAX_TEXTO = 255
MAX_NOMBRE_RECURSO = 64
MAX_CLAVES_RECURSO = 40        # metricas por recurso; corta un dict absurdo
MAX_RECURSOS_MUESTRA = 32      # recursos por muestra


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

    OJO (v2): esta hora es la del CLIENTE y es solo informativa. La hora con la
    que se guarda una metrica la calcula el servidor a partir de mono_ns.
    """
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def mono_ns() -> int:
    """
    Reloj monotonico del proceso, en nanosegundos.

    Es la pieza central del requisito "no permitir el cambio de hora del
    cliente": time.monotonic_ns() NO se ve afectado por ajustes del reloj del
    sistema, ni por NTP, ni por que el usuario cambie la fecha a mano. Solo
    avanza. Sirve para medir CUANTO HACE que se tomo una muestra, que es lo
    unico que el servidor necesita del cliente para fecharla correctamente.

    Solo tiene sentido comparar dos valores del MISMO proceso: el origen es
    arbitrario. Por eso el cliente manda siempre el mono_ns del envio junto
    con el de cada muestra, y el servidor trabaja con la diferencia.
    """
    return time.monotonic_ns()


def desvio_de_reloj(iso_cliente: str) -> float | None:
    """
    Segundos que el reloj del cliente esta adelantado (positivo) o atrasado
    (negativo) respecto al de esta maquina. None si el texto no es una fecha.

    Lo usa el servidor para dejar el evento RELOJ_DESVIADO en la bitacora: el
    dato se guarda bien igual, pero queda constancia de que ese nodo tiene la
    hora mal, que es informacion util para un operador.
    """
    try:
        dt = datetime.fromisoformat(iso_cliente)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (dt - datetime.now(timezone.utc)).total_seconds()


# Una muestra no puede tener mas antiguedad que esto. Protege contra un cliente
# con el reloj monotonico raro (o manipulado) que diga que su muestra se tomo
# hace diez anos y ensucie el historico entero.
MAX_EDAD_MUESTRA_SEG = 30 * 24 * 3600


def fechar_muestra(mono_envio: object, mono_muestra: object,
                   recibido: datetime) -> datetime:
    """
    LA funcion del requisito "no permitir el cambio de hora del cliente".

    Convierte "hace cuanto se tomo esta muestra" en una hora real, usando SOLO
    el reloj del servidor:

        edad      = (mono_del_envio - mono_de_la_muestra) / 1e9
        timestamp = hora_del_servidor - edad

    Los dos mono_* vienen del MISMO proceso cliente, asi que su diferencia es
    tiempo real transcurrido aunque el origen sea arbitrario y aunque el reloj
    de pared de esa maquina este mal o lo hayan cambiado a mitad.

    Para una metrica en vivo la edad es de milisegundos y el resultado es
    "ahora". Para un lote que llego dos horas tarde, cada muestra queda fechada
    en el momento en que se tomo de verdad, repartida en esas dos horas.

    Si falta alguno de los dos valores (un cliente v1) o el numero no tiene
    sentido, se asume que la muestra es de ahora: es la suposicion menos
    danina, porque adelanta el dato unos segundos en vez de esconderlo fuera
    de la ventana de las consultas por tiempo.

    Vive aqui y no en el servidor porque es parte del CONTRATO — define que
    significan mono_ns y mono_envio_ns — y porque asi se puede probar sin
    levantar MySQL (scripts/prueba_offline.py).
    """
    try:
        edad = (int(mono_envio) - int(mono_muestra)) / 1_000_000_000
    except (TypeError, ValueError):
        return recibido
    if edad < 0 or edad > MAX_EDAD_MUESTRA_SEG:
        return recibido
    return recibido - timedelta(seconds=edad)


# ------------------------------------------------------------------ recursos

def _numero(valor: Any) -> float | int | None:
    """Acepta int/float finitos; descarta texto, None, NaN e infinitos."""
    if isinstance(valor, bool):          # bool es int en Python: no es metrica
        return None
    if not isinstance(valor, (int, float)):
        return None
    if valor != valor or valor in (float("inf"), float("-inf")):
        return None
    return valor


def recurso(tipo: str, nombre: str, metricas: dict,
            etiquetas: dict | None = None) -> dict:
    """
    Un recurso medido del nodo: un disco, la RAM, la CPU, una interfaz de red.

        recurso("RAM", "fisica", {"total_gb": 16.0, "usado_gb": 9.2,
                                  "uso_pct": 57.5})

    `metricas` es libre: cualquier clave con valor numerico se guarda. Eso es
    lo que hace flexible al sistema — para reportar algo nuevo no hay que
    tocar la base ni el servidor.

    `etiquetas` es para texto que describe el recurso pero no es una medida
    (tipo de disco, punto de montaje, modelo). Se guarda aparte para que las
    consultas numericas no tengan que filtrarlo.
    """
    if tipo not in TIPOS_RECURSO:
        tipo = REC_CUSTOM
    limpias: dict[str, float | int] = {}
    for clave, valor in (metricas or {}).items():
        if len(limpias) >= MAX_CLAVES_RECURSO:
            break
        num = _numero(valor)
        if num is not None:
            limpias[str(clave)[:MAX_NOMBRE_RECURSO]] = num
    etiq: dict[str, str] = {}
    for clave, valor in (etiquetas or {}).items():
        if len(etiq) >= MAX_CLAVES_RECURSO:
            break
        if valor is not None:
            etiq[str(clave)[:MAX_NOMBRE_RECURSO]] = str(valor)[:MAX_TEXTO]
    return {
        "tipo": tipo,
        "nombre": str(nombre)[:MAX_NOMBRE_RECURSO],
        "metricas": limpias,
        "etiquetas": etiq,
    }


def validar_recursos(crudos: Any) -> list[dict]:
    """
    Sanea la lista `recursos` que llega por la red antes de tocar la base.

    Lo que viene de un socket no se cree: un cliente mal escrito (o alguien
    conectado al puerto 5050 a mano) puede mandar 10.000 recursos, claves de
    un megabyte o valores que no son numeros. Aqui se corta todo eso, en vez
    de descubrirlo cuando MySQL rechace el INSERT y mate el hilo del nodo.
    """
    if not isinstance(crudos, list):
        return []
    salida: list[dict] = []
    for item in crudos[:MAX_RECURSOS_MUESTRA]:
        if not isinstance(item, dict):
            continue
        nombre = item.get("nombre")
        if not nombre:
            continue
        salida.append(recurso(
            str(item.get("tipo", REC_CUSTOM)).upper(),
            nombre,
            item.get("metricas") if isinstance(item.get("metricas"), dict) else {},
            item.get("etiquetas") if isinstance(item.get("etiquetas"), dict) else {},
        ))
    return salida


# --------------------------------------------------------------- constructores

def hello(node_id: str, region: str, hostname: str, so: str, intervalo: int,
          capacidades: list[str] | None = None, pendientes: int = 0,
          agente: str = "2.0.0", sede: str = "") -> dict:
    """
    Presentacion del nodo.

    `capacidades` dice que sabe medir este cliente (disco, ram, cpu, red). El
    servidor no tiene que adivinarlo ni tener una lista fija: un nodo que
    manana aprenda a medir la temperatura lo anuncia aqui.

    `pendientes` es cuantas muestras tiene guardadas sin enviar. El servidor lo
    registra en el log y lo muestra en el dashboard: es la manera de saber, en
    el momento de la reconexion, cuanto se perdio antes de recibirlo.

    `region` es el DEPARTAMENTO (hay nueve: las administraciones regionales del
    enunciado) y `sede` es la oficina concreta. El departamento de La Paz tiene
    dos sedes con servidor propio: La Paz y El Alto. Se agrupa por region y se
    distingue por sede.
    """
    return {
        "tipo": "HELLO",
        "v": VERSION_PROTOCOLO,
        "node_id": str(node_id)[:MAX_NODE_ID],
        "region": str(region)[:MAX_REGION],
        "sede": str(sede or region)[:MAX_SEDE],
        "hostname": str(hostname)[:MAX_HOSTNAME],
        "so": str(so)[:MAX_SO],
        "intervalo": int(intervalo),
        "agente": str(agente)[:32],
        "capacidades": [str(c)[:32] for c in (capacidades or ["disco"])][:16],
        "pendientes": int(pendientes),
        "timestamp": ahora_iso(),
        "mono_ns": mono_ns(),
    }


def hello_ok(registrado: bool, nuevo: bool, intervalo: int,
             recursos_pedidos: list[str] | None = None,
             desvio_seg: float | None = None) -> dict:
    """
    nuevo=True es la prueba en vivo del requisito 7.2 (alta automatica).

    `recursos` le dice al cliente que quiere el servidor que reporte. Asi el
    operador decide desde el dashboard si un nodo manda solo disco o tambien
    RAM y CPU, sin tocar el archivo de configuracion de esa maquina.

    `hora_servidor` y `desvio_seg` cierran el requisito del reloj: el cliente
    se entera de que su hora esta mal y lo deja en su propio log.
    """
    return {
        "tipo": "HELLO_OK",
        "v": VERSION_PROTOCOLO,
        "registrado": registrado,
        "nuevo": nuevo,
        "intervalo": int(intervalo),
        "recursos": list(recursos_pedidos or []),
        "sync": True,
        "hora_servidor": ahora_iso(),
        "desvio_seg": None if desvio_seg is None else round(float(desvio_seg), 3),
        "timestamp": ahora_iso(),
    }


def error(motivo: str) -> dict:
    """El servidor rechaza al cliente y le dice por que, en vez de cortar mudo."""
    return {"tipo": "ERROR", "motivo": str(motivo)[:MAX_TEXTO], "timestamp": ahora_iso()}


def muestra(seq: int, disco: dict, recursos: list[dict] | None = None,
            mono: int | None = None, marca: str | None = None) -> dict:
    """
    UNA medicion, sin envolver. Es lo que el cliente guarda en su base local y
    lo que viaja dentro de un METRIC o de un METRIC_BATCH.

    seq   : numero que crece siempre y no se reinicia (persiste en la base
            local del cliente). Es lo que permite al servidor descartar una
            retransmision sin insertar la fila dos veces.
    mono  : reloj monotonico del cliente en el instante de la medicion.
    marca : hora local del cliente, informativa.
    """
    return {
        "seq": int(seq),
        "timestamp": marca or ahora_iso(),
        "mono_ns": mono_ns() if mono is None else int(mono),
        "disco": disco,
        "recursos": recursos or [],
    }


def metric(node_id: str, disco: dict, seq: int = 0,
           recursos: list[dict] | None = None,
           mono: int | None = None, marca: str | None = None) -> dict:
    """
    Metrica en vivo: una sola muestra, recien tomada.

    `disco` debe traer EXACTAMENTE estas claves:
      nombre, tipo, total_gb, usado_gb, libre_gb, uso_pct,
      iops_lectura, iops_escritura, latencia_ms

    Se mantiene en la raiz del mensaje (y no dentro de `recursos`) porque es lo
    que consumen la tabla `metricas`, la vista v_cluster y el dashboard desde
    la version 1. Los discos ADICIONALES (el pendrive de Santa Cruz) van en
    `recursos` como tipo DISCO.
    """
    m = muestra(seq, disco, recursos, mono, marca)
    m["tipo"] = "METRIC"
    m["node_id"] = str(node_id)[:MAX_NODE_ID]
    m["mono_envio_ns"] = mono_ns()
    return m


def metric_batch(node_id: str, muestras: list[dict]) -> dict:
    """
    Lote de recuperacion: todo lo que el cliente midio mientras no habia red.

    `mono_envio_ns` es la referencia con la que el servidor fecha cada muestra:
    para cada una calcula (mono_envio_ns - mono_ns) y se lo resta a SU propia
    hora. Por eso el lote entero queda fechado bien aunque llegue horas tarde,
    y aunque el reloj del cliente este cambiado.
    """
    return {
        "tipo": "METRIC_BATCH",
        "v": VERSION_PROTOCOLO,
        "node_id": str(node_id)[:MAX_NODE_ID],
        "mono_envio_ns": mono_ns(),
        "timestamp": ahora_iso(),
        "muestras": muestras[:MAX_MUESTRAS_LOTE],
    }


def metric_ok(seq: int = 0) -> dict:
    """Confirmacion de una metrica en vivo. Devuelve el seq para que el cliente
    marque esa muestra como entregada en su base local."""
    return {"tipo": "METRIC_OK", "recibido": True, "seq": int(seq),
            "timestamp": ahora_iso()}


def sync_ok(hasta_seq: int, recibidas: int, descartadas: int = 0) -> dict:
    """
    Respuesta a un METRIC_BATCH. `hasta_seq` es el ultimo numero aceptado: el
    cliente borra de su base local todo lo que sea <= a ese valor y sigue con
    el siguiente lote. Es un ACK ACUMULATIVO, asi que un SYNC_OK perdido no
    pierde datos: el lote se reenvia entero la proxima vez.
    """
    return {
        "tipo": "SYNC_OK",
        "hasta_seq": int(hasta_seq),
        "recibidas": int(recibidas),
        "descartadas": int(descartadas),
        "timestamp": ahora_iso(),
    }


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


def pong(node_id: str) -> dict:
    """Respuesta al PING del servidor. Sirve para detectar una conexion medio
    abierta (cable cortado, wifi caido) sin esperar al watchdog."""
    return {"tipo": "PONG", "node_id": str(node_id)[:MAX_NODE_ID],
            "timestamp": ahora_iso(), "mono_ns": mono_ns()}


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

    def __init__(self, sock: socket.socket, tam_buffer: int = 16384) -> None:
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
