"""
Pool de conexiones MySQL — tarea 3.2.  Responsable: Robert.

EL PROBLEMA QUE RESUELVE ESTE ARCHIVO
-------------------------------------
El servidor tiene un hilo por cada cliente conectado (9), mas el watchdog, mas
el despachador. Todos escriben en MySQL al mismo tiempo. Dos formas de hacerlo
mal:

  1. Una sola conexion global compartida entre hilos.
     -> mysql-connector NO es thread-safe a nivel de cursor: dos hilos usando la
        misma conexion producen "Commands out of sync" o resultados mezclados.

  2. Abrir y cerrar una conexion nueva en cada INSERT.
     -> el handshake TCP + autenticacion cuesta ~5-15 ms. A 9 nodos cada 10 s no
        se nota, pero en la prueba de carga si, y es un desperdicio evidente.

La solucion es un POOL: un conjunto de conexiones ya abiertas que los hilos
piden prestadas y devuelven. cnx.close() NO cierra la conexion, la devuelve al
pool. Por eso el context manager de abajo es obligatorio.

REGLA DEL EQUIPO: nadie llama a mysql.connector.connect() fuera de este archivo.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Iterator

from mysql.connector import Error as MySQLError
from mysql.connector import pooling

from comun import config

log = logging.getLogger("db")

# pool_size debe ser >= al numero de hilos que pueden pedir conexion a la vez:
#   9 clientes + watchdog + despachador + margen = 16 esta bien en local.
# Limite duro de mysql-connector: 32.
#
# CON MYSQL GESTIONADO (Aiven): bajar a 10. Corren DOS procesos (servidor de
# sockets y API) y cada uno arma su propio pool, asi que el consumo real es el
# doble del numero que pongan aqui. Aiven calcula el limite como
# 75 x GB_de_RAM + 1, o sea ~76 conexiones en el plan gratuito de 1 GB:
# 2 x 10 = 20 entra comodo, 2 x 16 = 32 tambien, pero conviene dejar margen
# para MySQL Workbench y para lo que abran ustedes a mano.
_ssl = {}
if config.DB_SSL_CA:
    # Aiven (y cualquier MySQL gestionado) exige TLS y entrega su propio ca.pem.
    _ssl = {
        "ssl_ca": config.DB_SSL_CA,
        "ssl_verify_cert": True,
    }
    log.info("Conexion TLS con CA en %s", config.DB_SSL_CA)

_pool = pooling.MySQLConnectionPool(
    pool_name="cns_pool",
    pool_size=config.DB_POOL_SIZE,
    pool_reset_session=True,
    host=config.DB_HOST,
    port=config.DB_PORT,
    user=config.DB_USER,
    password=config.DB_PASSWORD,
    database=config.DB_NAME,
    charset="utf8mb4",
    collation="utf8mb4_0900_ai_ci",
    autocommit=True,        # cada sentencia se confirma sola: no hay
                            # transacciones largas que bloqueen a otros hilos
    time_zone="+00:00",
    connection_timeout=10,  # mas alto que en local: el viaje a la nube tarda
    **_ssl,
)


@contextmanager
def cursor(diccionario: bool = True) -> Iterator[Any]:
    """
    Presta una conexion del pool y la devuelve pase lo que pase.

        with cursor() as cur:
            cur.execute("SELECT 1")
            fila = cur.fetchone()

    diccionario=True devuelve filas como dict (fila["node_id"]) en vez de
    tuplas (fila[0]). Mucho mas legible y mas facil de serializar a JSON.
    """
    cnx = _pool.get_connection()
    cur = cnx.cursor(dictionary=diccionario)
    try:
        yield cur
    finally:
        try:
            cur.close()
        finally:
            cnx.close()      # devuelve la conexion al pool, no la cierra


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
