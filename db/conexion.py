"""
Conexiones a MySQL — tarea 3.2.  Responsable: Robert.

EL PROBLEMA QUE RESUELVE ESTE ARCHIVO
-------------------------------------
El servidor tiene un hilo por cada cliente conectado (9), mas el watchdog, mas
el despachador. Todos escriben en MySQL al mismo tiempo. Dos formas de hacerlo
mal:

  1. Una sola conexion global compartida entre hilos.
     -> mysql-connector NO es thread-safe a nivel de cursor: dos hilos usando la
        misma conexion producen "Commands out of sync" o resultados mezclados.

  2. Abrir y cerrar una conexion nueva en cada INSERT.
     -> con TLS contra Aiven, cada apertura es un handshake completo de medio
        segundo. Insostenible.

POR QUE NO USAMOS MySQLConnectionPool
-------------------------------------
La primera version usaba el pool de mysql-connector y contra Aiven daba 11
inserts en 20 segundos con 9 hilos. El motivo esta en su implementacion:
get_connection() toma un lock GLOBAL del proceso y, mientras lo tiene, hace un
PING al servidor. Contra una base local ese ping cuesta microsegundos. Contra
la nube cuesta un viaje de ida y vuelta completo, y como el lock es global los
nueve hilos hacen fila de a uno: el pool termina serializando justamente lo que
deberia paralelizar.

Aqui usamos una conexion POR HILO (threading.local): sin lock global, cada hilo
avanza a su ritmo.

  local  : practicamente igual que el pool
  Aiven  : entre 3 y 5 veces mas rapido, y de verdad concurrente

Nota honesta para la defensa: cnx.cursor() de mysql-connector tambien hace un
ping interno, asi que cada operacion cuesta un viaje extra. Lo dejamos porque
es justo lo que hace que la reconexion automatica funcione sin codigo nuestro:
si Aiven cerro la conexion por inactividad, ese ping falla, lo detectamos y
reabrimos. Lo que eliminamos es el LOCK GLOBAL, que es lo que serializaba.

IMPORTANTE: un hilo que termina debe cerrar su conexion, o queda colgada del
lado del servidor. Por eso existe cerrar_conexion_del_hilo(), y el servidor la
llama en el finally de atender_cliente().

REGLA DEL EQUIPO: nadie llama a mysql.connector.connect() fuera de este archivo.
"""
from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator

import mysql.connector
from mysql.connector import Error as MySQLError
from mysql.connector import errors as mysql_errors

from comun import config

log = logging.getLogger("db")

_CONFIG: dict[str, Any] = {
    "host": config.DB_HOST,
    "port": config.DB_PORT,
    "user": config.DB_USER,
    "password": config.DB_PASSWORD,
    "database": config.DB_NAME,
    "charset": "utf8mb4",
    "collation": "utf8mb4_0900_ai_ci",
    "autocommit": True,     # cada sentencia se confirma sola: no hay
                            # transacciones largas que bloqueen a otros hilos
    "time_zone": "+00:00",  # TODO el sistema guarda y compara en UTC.
                            # repositorio._a_datetime convierte la hora del
                            # cliente (que llega CON offset) antes de insertar.
                            # Si tocan esto, rompen los filtros por tiempo.
    "connection_timeout": 15,
    "use_pure": True,
}

if config.DB_SSL_CA:
    # Aiven (y cualquier MySQL gestionado) exige TLS y entrega su propio ca.pem.
    _CONFIG["ssl_ca"] = config.DB_SSL_CA
    _CONFIG["ssl_verify_cert"] = True
    _CONFIG["ssl_verify_identity"] = True   # ademas de la cadena, el hostname

# Nota sobre MySQL local sin TLS: el plugin por defecto de MySQL 8 es
# caching_sha2_password, que en la primera autenticacion de un usuario necesita
# canal seguro o la clave publica RSA del servidor. La implementacion en Python
# puro (use_pure=True) pide esa clave sola, asi que no hace falta configurar
# nada; probado contra MySQL 8.0.46 con un usuario recien creado. La opcion
# get_server_public_key existe pero SOLO para la extension en C, y pasarsela a
# use_pure=True lanza AttributeError.

_local = threading.local()
_abiertas = 0
_candado_contador = threading.Lock()

# Errores tras los cuales la conexion queda inservible: hay que descartarla.
# InternalError incluye "Unread result found", que deja la conexion envenenada
# para siempre si no se recicla.
_ERRORES_FATALES = (
    mysql_errors.OperationalError,
    mysql_errors.InterfaceError,
    mysql_errors.InternalError,
    mysql_errors.DatabaseError,
    OSError,
)


def conexiones_abiertas() -> int:
    with _candado_contador:
        return _abiertas


def _abrir() -> Any:
    global _abiertas
    cnx = mysql.connector.connect(**_CONFIG)
    _local.cnx = cnx
    with _candado_contador:
        _abiertas += 1
        actuales = _abiertas
    if actuales > config.DB_MAX_CONEXIONES:
        log.warning(
            "Hay %d conexiones abiertas y el limite configurado es %d. Si esto "
            "crece sin parar, algun hilo no llama a cerrar_conexion_del_hilo() "
            "al terminar.", actuales, config.DB_MAX_CONEXIONES)
    return cnx


def cerrar_conexion_del_hilo() -> None:
    """
    Cierra la conexion del hilo actual. Se llama cuando un hilo termina: en el
    servidor, al desconectarse un cliente; en la API, al terminar cada pedido.
    Sin esto, cada hilo que se va deja una conexion colgada y en Aiven se acaban
    rapido. Es seguro llamarla aunque el hilo no tenga conexion abierta.
    """
    global _abiertas
    cnx = getattr(_local, "cnx", None)
    if cnx is None:
        return
    _local.cnx = None
    try:
        cnx.close()
    except Exception:                                             # noqa: BLE001
        pass
    with _candado_contador:
        _abiertas = max(0, _abiertas - 1)


@contextmanager
def cursor(diccionario: bool = True) -> Iterator[Any]:
    """
    Presta el cursor de la conexion de ESTE hilo.

        with cursor() as cur:
            cur.execute("SELECT 1")
            fila = cur.fetchone()

    Si la conexion estaba caida (MySQL se reinicio, se corto la red, Aiven la
    cerro por inactividad), la reabre y reintenta UNA vez ANTES de ejecutar
    nada. Si el fallo ocurre a mitad de la consulta, la conexion se descarta y
    el error se propaga: reintentar ahi podria repetir un INSERT.

    diccionario=True devuelve filas como dict (fila["node_id"]) en vez de
    tuplas (fila[0]). Mas legible y mas facil de serializar a JSON.
    """
    cur = None
    for intento in (1, 2):
        cnx = getattr(_local, "cnx", None) or _abrir()
        try:
            # cursor() hace un ping interno: aqui es donde se detecta que la
            # conexion murio, y por eso el reintento vale la pena.
            cur = cnx.cursor(dictionary=diccionario)
            break
        except _ERRORES_FATALES as e:
            cerrar_conexion_del_hilo()
            if intento == 2:
                raise
            log.info("Conexion caida (%s). Reabriendo.", e)

    assert cur is not None
    try:
        yield cur
    except _ERRORES_FATALES as e:
        log.warning("Error de base (%s). Se descarta la conexion del hilo.", e)
        try:
            cur.close()
        except Exception:                                         # noqa: BLE001
            pass
        cerrar_conexion_del_hilo()
        raise
    else:
        try:
            cur.close()
        except Exception:                                         # noqa: BLE001
            # cur.close() lanza "Unread result found" si quedaron filas sin
            # consumir, y deja la conexion inutilizable. Se recicla.
            cerrar_conexion_del_hilo()


def probar_conexion() -> bool:
    """Chequeo rapido para el arranque. Falla temprano y con mensaje claro."""
    try:
        with cursor() as cur:
            cur.execute("SELECT VERSION() AS v")
            log.info("MySQL conectado: %s", cur.fetchone()["v"])
        return True
    except MySQLError as e:
        log.error("No se pudo conectar a MySQL: %s", e)
        return False


def medir_latencia(muestras: int = 5) -> float:
    """
    Milisegundos que cuesta UNA operacion completa contra la base, medida como
    la usa el resto del codigo: un `with cursor()` entero, no solo el execute.
    Incluye el ping interno de cursor(), que es real y se paga siempre.

    Sirve para saber si estan contra una base local (pocos ms) o contra la nube
    (100 ms o mas).
    """
    with cursor() as cur:                    # calienta: descarta la primera
        cur.execute("SELECT 1 AS x")
        cur.fetchall()

    t0 = time.perf_counter()
    for _ in range(muestras):
        with cursor() as cur:
            cur.execute("SELECT 1 AS x")
            cur.fetchall()
    return (time.perf_counter() - t0) / muestras * 1000
