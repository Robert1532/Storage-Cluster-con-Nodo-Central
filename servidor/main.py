"""
Nodo Central de Monitoreo — modulo M2.  Responsable: Edwin.

    python -m servidor.main

ESQUELETO: la estructura, los hilos y el manejo de la BD ya estan resueltos.
Lo que falta esta marcado con  # TODO Edwin.

Hilos que levanta este proceso:
    1. principal      -> accept() en bucle, un hilo nuevo por cliente
    2. atender_cliente-> uno por conexion; recibe METRIC y ACK
    3. watchdog       -> marca NO_REPORTA a los que dejaron de reportar
    4. despachador    -> lee mensajes PENDIENTE de la BD y los envia

CONCURRENCIA: el unico estado compartido es CONECTADOS (node_id -> socket).
Todo acceso pasa por CANDADO. Sepan senalar estas dos lineas en la defensa.
"""
from __future__ import annotations

import logging
import socket
import threading

from comun import config, protocolo
from db import repositorio as repo
from db.conexion import probar_conexion

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-18s] %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("servidor")

# --- ESTADO COMPARTIDO ENTRE HILOS ---------------------------------------
CONECTADOS: dict[str, socket.socket] = {}
CANDADO = threading.Lock()
APAGANDO = threading.Event()


# ============================================================ hilo por cliente

def atender_cliente(sock: socket.socket, direccion: tuple[str, int]) -> None:
    ip = direccion[0]
    node_id: str | None = None
    log.info("Conexion entrante desde %s", ip)

    try:
        for mensaje in protocolo.LectorLineas(sock):
            tipo = mensaje.get("tipo")

            # ---------------------------------------------- HELLO
            if tipo == "HELLO":
                node_id = mensaje["node_id"]

                with CANDADO:
                    if len(CONECTADOS) >= config.MAX_NODOS and node_id not in CONECTADOS:
                        log.warning("Cluster lleno (%d), rechazo %s",
                                    config.MAX_NODOS, node_id)
                        sock.close()
                        return
                    CONECTADOS[node_id] = sock

                es_nuevo, intervalo = repo.registrar_nodo(
                    node_id=node_id,
                    region=mensaje.get("region", "Desconocida"),
                    hostname=mensaje.get("hostname"),
                    so=mensaje.get("so"),
                    ip=ip,
                    intervalo=mensaje.get("intervalo", config.INTERVALO_DEFECTO_SEG),
                )
                protocolo.enviar(sock, protocolo.hello_ok(True, es_nuevo, intervalo))
                log.info("%s %s (%s) intervalo=%ss",
                         "ALTA AUTOMATICA de" if es_nuevo else "Reconecta",
                         node_id, mensaje.get("region"), intervalo)

            # ---------------------------------------------- METRIC
            elif tipo == "METRIC":
                if node_id is None:
                    continue                        # METRIC antes de HELLO: se ignora
                repo.guardar_metrica(node_id, mensaje["timestamp"], mensaje["disco"])
                protocolo.enviar(sock, protocolo.metric_ok())
                # TODO Edwin: si el nodo estaba en NO_REPORTA, llamar a
                #          repo.marcar_recuperado(node_id) y loguearlo.

            # ---------------------------------------------- ACK
            elif tipo == "ACK":
                repo.confirmar_ack(mensaje["cmd_id"])
                log.info("ACK de %s para %s", node_id, mensaje["cmd_id"])

    except (ConnectionResetError, OSError) as e:
        log.warning("Conexion perdida con %s: %s", node_id or ip, e)
    finally:
        if node_id:
            with CANDADO:
                CONECTADOS.pop(node_id, None)
            repo.registrar_evento(node_id, "DESCONEXION", f"Cierre desde {ip}")
            log.info("Desconectado %s (quedan %d)", node_id, len(CONECTADOS))
        sock.close()


# ==================================================================== watchdog

def watchdog() -> None:
    """Tarea 2.3. El umbral es por nodo: factor x su propio intervalo."""
    log.info("Watchdog activo (factor=%dx el intervalo de cada nodo)",
             config.FACTOR_TIMEOUT)
    while not APAGANDO.wait(config.PERIODO_WATCHDOG_SEG):
        try:
            for node_id in repo.marcar_nodos_caidos(config.FACTOR_TIMEOUT):
                log.warning("NO REPORTA: %s", node_id)
        except Exception as e:                                    # noqa: BLE001
            log.error("Error en watchdog: %s", e)


# ================================================================ despachador

def despachador() -> None:
    """
    Puente entre la API y los sockets. Lee los mensajes que el dashboard dejo
    en estado PENDIENTE y los envia al nodo si esta conectado.
    """
    log.info("Despachador activo (cada %ds)", config.PERIODO_DESPACHADOR_SEG)
    while not APAGANDO.wait(config.PERIODO_DESPACHADOR_SEG):
        try:
            for m in repo.mensajes_pendientes():
                with CANDADO:
                    sock = CONECTADOS.get(m["node_id"])
                if sock is None:
                    repo.marcar_fallido(m["cmd_id"], "Nodo no conectado")
                    log.warning("No se pudo enviar a %s: no esta conectado", m["node_id"])
                    continue
                protocolo.enviar(sock, protocolo.cmd(
                    m["cmd_id"], m["accion"], m.get("texto"), m.get("valor")))
                repo.marcar_enviado(m["cmd_id"])
                log.info("-> %s : %s", m["node_id"], m.get("texto") or m["accion"])
        except Exception as e:                                    # noqa: BLE001
            log.error("Error en despachador: %s", e)


# ======================================================================= main

def main() -> None:
    if not probar_conexion():
        log.error("Sin MySQL no arranca. Revisen el .env y que el servicio este arriba.")
        return

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((config.SOCKET_HOST, config.SOCKET_PORT))
    servidor.listen(config.MAX_NODOS + 5)
    log.info("Escuchando en %s:%d (max %d nodos)",
             config.SOCKET_HOST, config.SOCKET_PORT, config.MAX_NODOS)

    threading.Thread(target=watchdog, name="watchdog", daemon=True).start()
    threading.Thread(target=despachador, name="despachador", daemon=True).start()

    try:
        while True:
            sock, direccion = servidor.accept()
            threading.Thread(
                target=atender_cliente,
                args=(sock, direccion),
                name=f"cliente-{direccion[0]}",
                daemon=True,
            ).start()
    except KeyboardInterrupt:
        log.info("Apagando servidor...")
        APAGANDO.set()
    finally:
        servidor.close()


if __name__ == "__main__":
    main()
