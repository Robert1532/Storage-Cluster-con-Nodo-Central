"""
Nodo Central de Monitoreo — modulo M2.  Responsable: Edwin.

    python -m servidor.main

ESQUELETO: la estructura, los hilos, el manejo de errores y la base ya estan
resueltos. Lo que falta esta marcado con  # TODO Edwin.

Hilos que levanta este proceso:
    1. principal      -> accept() en bucle, un hilo nuevo por cliente
    2. atender_cliente-> uno por conexion; recibe METRIC y ACK
    3. watchdog       -> marca NO_REPORTA y RECUPERADO
    4. despachador    -> lee mensajes PENDIENTE de la BD y los envia

CONCURRENCIA — dos candados distintos, y hay que saber explicar la diferencia:

  CANDADO        protege el diccionario CONECTADOS. Es UNO para todo el
                 servidor, y solo se toma para leer o escribir el diccionario:
                 nunca mientras se habla con la red o con la base.

  candado del    protege UN socket. Es uno POR CONEXION. Dos hilos pueden
  socket         escribir en el mismo socket (el despachador manda un CMD
                 mientras el hilo del cliente manda METRIC_OK) y sin candado
                 sus bytes se intercalan: el cliente recibe una linea corrupta
                 y el mensaje se pierde en silencio.

Si en la defensa preguntan por condiciones de carrera, estas dos lineas son la
respuesta, y conviene senalarlas en el codigo.
"""
from __future__ import annotations

import logging
import socket
import threading

from comun import config, protocolo
from db import repositorio as repo
from db.conexion import cerrar_conexion_del_hilo, probar_conexion

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-18s] %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("servidor")

# --- ESTADO COMPARTIDO ENTRE HILOS ---------------------------------------
# node_id -> (socket, candado de escritura de ese socket)
CONECTADOS: dict[str, tuple[socket.socket, threading.Lock]] = {}
CANDADO = threading.Lock()
APAGANDO = threading.Event()


def _registrar_conectado(node_id: str, sock: socket.socket,
                         candado: threading.Lock) -> None:
    with CANDADO:
        CONECTADOS[node_id] = (sock, candado)


def _quitar_conectado(node_id: str, sock: socket.socket) -> int:
    """
    Quita el nodo SOLO si el socket guardado es el nuestro.

    Sin esa comprobacion pasa esto: un nodo pierde la red, su hilo queda
    bloqueado en recv sin enterarse, el nodo reconecta y un hilo nuevo guarda
    su socket. Cuando el hilo viejo por fin muere, borraria la entrada del
    socket NUEVO, que esta vivo — y a partir de ahi todos los mensajes a ese
    nodo se marcan FALLIDO "no esta conectado" mientras el nodo reporta
    normalmente.
    """
    with CANDADO:
        actual = CONECTADOS.get(node_id)
        if actual is not None and actual[0] is sock:
            del CONECTADOS[node_id]
        return len(CONECTADOS)


# ============================================================ hilo por cliente

def atender_cliente(sock: socket.socket, direccion: tuple[str, int]) -> None:
    ip = direccion[0]
    node_id: str | None = None
    candado_envio = threading.Lock()
    log.info("Conexion entrante desde %s", ip)

    # Sin timeout, una conexion "medio abierta" (cable desenchufado, firewall
    # que descarta) deja este hilo bloqueado en recv durante horas, ocupando
    # una plaza de las 9 y una conexion a MySQL. Se ajusta al recibir el HELLO,
    # cuando ya sabemos el intervalo del nodo.
    espera = config.FACTOR_TIMEOUT * config.INTERVALO_DEFECTO_SEG
    sock.settimeout(espera)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    try:
        for mensaje in protocolo.LectorLineas(sock):
            try:
                tipo = mensaje.get("tipo")

                # ---------------------------------------------- HELLO
                if tipo == "HELLO":
                    if node_id is not None:
                        continue                # ya se presento; se ignora
                    entrante = mensaje.get("node_id")
                    if not entrante:
                        protocolo.enviar(sock, protocolo.error("HELLO sin node_id"),
                                         candado_envio)
                        return

                    with CANDADO:
                        lleno = (len(CONECTADOS) >= config.MAX_NODOS
                                 and entrante not in CONECTADOS)
                    if lleno:
                        log.warning("Cluster lleno (%d), rechazo %s",
                                    config.MAX_NODOS, entrante)
                        # Se le dice por que, en vez de cortar mudo: si no, el
                        # cliente no distingue "lleno" de un fallo de red y
                        # reintenta para siempre.
                        protocolo.enviar(
                            sock,
                            protocolo.error(f"Cluster lleno ({config.MAX_NODOS} nodos)"),
                            candado_envio)
                        return

                    node_id = entrante
                    _registrar_conectado(node_id, sock, candado_envio)

                    es_nuevo, intervalo = repo.registrar_nodo(
                        node_id=node_id,
                        region=mensaje.get("region", "Desconocida"),
                        hostname=mensaje.get("hostname"),
                        so=mensaje.get("so"),
                        ip=ip,
                        intervalo=mensaje.get("intervalo", config.INTERVALO_DEFECTO_SEG),
                    )
                    sock.settimeout(config.FACTOR_TIMEOUT * intervalo)
                    protocolo.enviar(sock, protocolo.hello_ok(True, es_nuevo, intervalo),
                                     candado_envio)
                    log.info("%s %s (%s) intervalo=%ss",
                             "ALTA AUTOMATICA de" if es_nuevo else "Reconecta",
                             node_id, mensaje.get("region"), intervalo)

                # ---------------------------------------------- METRIC
                elif tipo == "METRIC":
                    if node_id is None:
                        continue                # METRIC antes de HELLO: se ignora
                    disco = mensaje.get("disco")
                    if not isinstance(disco, dict):
                        log.warning("METRIC de %s sin bloque 'disco'", node_id)
                        continue
                    repo.guardar_metrica(node_id, mensaje.get("timestamp", ""), disco)
                    protocolo.enviar(sock, protocolo.metric_ok(), candado_envio)

                # ---------------------------------------------- ACK
                elif tipo == "ACK":
                    cmd_id = mensaje.get("cmd_id")
                    if cmd_id:
                        repo.confirmar_ack(cmd_id)
                        log.info("ACK de %s para %s", node_id, cmd_id)

            except Exception as e:                                # noqa: BLE001
                # Un mensaje malo (un campo raro, un fallo puntual de la base)
                # no puede cerrar la sesion de un nodo: se registra y se sigue
                # con el siguiente.
                log.exception("Error procesando %s de %s: %s",
                              mensaje.get("tipo"), node_id or ip, e)

    except socket.timeout:
        log.warning("Sin datos de %s dentro del umbral; cierro la conexion",
                    node_id or ip)
    except protocolo.ErrorProtocolo as e:
        log.warning("Protocolo violado por %s: %s", node_id or ip, e)
    except OSError as e:
        log.warning("Conexion perdida con %s: %s", node_id or ip, e)
    except Exception as e:                                        # noqa: BLE001
        log.exception("Error inesperado atendiendo a %s: %s", node_id or ip, e)
    finally:
        # Cada paso va protegido: si el primero falla, los siguientes TIENEN
        # que ejecutarse igual. Es justo cuando la base da problemas cuando mas
        # falta hace liberar la conexion.
        if node_id:
            try:
                quedan = _quitar_conectado(node_id, sock)
                log.info("Desconectado %s (quedan %d)", node_id, quedan)
            except Exception:                                     # noqa: BLE001
                log.exception("No se pudo quitar %s de CONECTADOS", node_id)
            try:
                repo.registrar_evento(node_id, "DESCONEXION", f"Cierre desde {ip}")
            except Exception:                                     # noqa: BLE001
                log.exception("No se pudo registrar la desconexion de %s", node_id)
        try:
            sock.close()
        except OSError:
            pass
        # Este hilo muere aqui: hay que devolver su conexion a MySQL o queda
        # colgada del lado del servidor. Con Aiven, unos cuantos clientes que
        # se reconectan agotarian el limite de conexiones.
        cerrar_conexion_del_hilo()


# ==================================================================== watchdog

def watchdog() -> None:
    """
    Tarea 2.3. El umbral es por nodo: factor x su propio intervalo.

    Este hilo es el UNICO que cambia `nodos.estado`, en los dos sentidos. Tener
    un solo escritor del estado evita que el hilo del cliente y el watchdog se
    pisen y el nodo quede oscilando entre ACTIVO y NO_REPORTA.
    """
    log.info("Watchdog activo (factor=%dx el intervalo de cada nodo)",
             config.FACTOR_TIMEOUT)
    try:
        while not APAGANDO.wait(config.PERIODO_WATCHDOG_SEG):
            try:
                for node_id in repo.marcar_nodos_caidos(config.FACTOR_TIMEOUT):
                    log.warning("NO REPORTA: %s", node_id)
                for node_id in repo.marcar_nodos_recuperados(config.FACTOR_TIMEOUT):
                    log.info("RECUPERADO: %s", node_id)
            except Exception as e:                                # noqa: BLE001
                log.error("Error en watchdog: %s", e)
    finally:
        cerrar_conexion_del_hilo()


# ================================================================ despachador

def despachador() -> None:
    """
    Puente entre la API y los sockets. Lee los mensajes que el dashboard dejo
    en estado PENDIENTE y los envia al nodo si esta conectado.

    Se marca ENVIADO ANTES de mandarlo. Parece al reves, pero es a proposito:
    si el proceso muere entre el envio y la marca, el mensaje se reenviaria al
    arrancar de nuevo y el nodo lo recibiria dos veces. Marcando antes, un
    fallo produce una perdida (visible: se queda en ENVIADO sin ACK) en vez de
    un duplicado silencioso. Para mensajes de operacion, perder es mejor que
    duplicar.
    """
    log.info("Despachador activo (cada %ds)", config.PERIODO_DESPACHADOR_SEG)
    try:
        while not APAGANDO.wait(config.PERIODO_DESPACHADOR_SEG):
            try:
                pendientes = repo.mensajes_pendientes()
            except Exception as e:                                # noqa: BLE001
                log.error("No se pudieron leer los mensajes pendientes: %s", e)
                continue

            for m in pendientes:
                # try/except POR MENSAJE: un nodo con problemas no puede
                # bloquear los mensajes de los otros ocho.
                try:
                    with CANDADO:
                        destino = CONECTADOS.get(m["node_id"])
                    if destino is None:
                        repo.marcar_fallido(m["cmd_id"], "El nodo no esta conectado")
                        log.warning("No se pudo enviar a %s: no esta conectado",
                                    m["node_id"])
                        continue

                    sock, candado_envio = destino
                    repo.marcar_enviado(m["cmd_id"])
                    protocolo.enviar(sock, protocolo.cmd(
                        m["cmd_id"], m["accion"], m.get("texto"), m.get("valor")),
                        candado_envio)
                    log.info("-> %s : %s", m["node_id"], m.get("texto") or m["accion"])
                except OSError as e:
                    try:
                        repo.marcar_fallido(m["cmd_id"], f"Error de red: {e}")
                    except Exception:                             # noqa: BLE001
                        pass
                    log.warning("Fallo el envio a %s: %s", m["node_id"], e)
                except Exception as e:                            # noqa: BLE001
                    log.exception("Error despachando %s: %s", m.get("cmd_id"), e)
    finally:
        cerrar_conexion_del_hilo()


# ======================================================================= main

def _cerrar_todas_las_conexiones() -> None:
    with CANDADO:
        sockets = [s for s, _ in CONECTADOS.values()]
        CONECTADOS.clear()
    for s in sockets:
        try:
            s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            s.close()
        except OSError:
            pass


def main() -> None:
    config.asegurar_directorios()
    if not probar_conexion():
        log.error("Sin MySQL no arranca. Revisen el .env y que el servicio este arriba.")
        return

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((config.SOCKET_HOST, config.SOCKET_PORT))
    servidor.listen(config.MAX_NODOS + 5)
    log.info("Escuchando en %s:%d (max %d nodos)",
             config.SOCKET_HOST, config.SOCKET_PORT, config.MAX_NODOS)

    # NO son daemon: queremos poder esperarlos al apagar para que cierren sus
    # conexiones a MySQL en vez de que el interprete los mate a mitad.
    hilos_fondo = [
        threading.Thread(target=watchdog, name="watchdog"),
        threading.Thread(target=despachador, name="despachador"),
    ]
    for h in hilos_fondo:
        h.start()

    try:
        while not APAGANDO.is_set():
            sock, direccion = servidor.accept()
            threading.Thread(
                target=atender_cliente,
                args=(sock, direccion),
                name=f"cliente-{direccion[0]}",
                daemon=True,
            ).start()
    except KeyboardInterrupt:
        log.info("Apagando servidor...")
    finally:
        APAGANDO.set()
        try:
            servidor.close()
        except OSError:
            pass
        # Cerrar los sockets desbloquea a los hilos de cliente, que asi corren
        # su finally y devuelven su conexion a MySQL.
        _cerrar_todas_las_conexiones()
        for h in hilos_fondo:
            h.join(timeout=5)
        cerrar_conexion_del_hilo()
        log.info("Servidor detenido")


if __name__ == "__main__":
    main()
