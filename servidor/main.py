"""
Nodo Central de Monitoreo — modulo M2.  Responsable: Edwin.

    python -m servidor.main
    python -m servidor.probar_concurrencia   # prueba de 9 nodos + watchdog
    python -m servidor.probar_alta_automatica  # M2.2 / requisito 7.2 en caliente
    python -m servidor.probar_watchdog         # M2.3 / estado No Reporta
    python -m servidor.probar_mensajeria       # M2.4 / CMD + ACK + broadcast
    python -m servidor.probar_consolidados     # M2.5 / totales cluster + growth
    python -m servidor.probar_m26              # M2.6 / 9 nodos 10 min + 2 caidas
    python -m servidor.consolidados            # reporte consolidados + N/A
    python -m servidor.mensajeria --help       # encolar desde terminal (demo)

Tareas 2.1–2.6:
    2.1  accept loop multicliente, un hilo por conexion
    2.2  registro automatico al recibir HELLO (requisito 7.2)
    2.3  watchdog con umbral por nodo (factor x intervalo_seg)
    2.4  despachador de mensajes PENDIENTE hacia los sockets
    2.5  consolidados del cluster (v_cluster + crecimiento; ver consolidados.py)
    2.6  prueba de concurrencia en probar_m26.py (10 min, 2 caidas cronometradas)

Hilos que levanta este proceso:
    1. principal       -> accept() en bucle, un hilo nuevo por cliente
    2. atender_cliente -> uno por conexion; recibe HELLO, METRIC y ACK
    3. watchdog        -> marca NO_REPORTA y RECUPERADO
    4. despachador     -> lee mensajes PENDIENTE de la BD y los envia

CONCURRENCIA — dos candados distintos (DEFENSA: buscar comentarios # DEFENSA:):

  CANDADO            protege el diccionario CONECTADOS. Es UNO para todo el
                     servidor, y solo se toma para leer o escribir el
                     diccionario: nunca mientras se habla con la red o con
                     la base.

  candado_envio      protege UN socket. Es uno POR CONEXION (en atender_cliente).
                     Dos hilos pueden escribir en el mismo socket (el despachador
                     manda un CMD mientras el hilo del cliente manda METRIC_OK)
                     y sin candado sus bytes se intercalan.
"""
from __future__ import annotations

import logging
import signal
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
CANDADO = threading.Lock()          # DEFENSA: protege SOLO el dict CONECTADOS
APAGANDO = threading.Event()
_servidor_sock: socket.socket | None = None


def _cerrar_socket(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


def _intentar_registrar_conectado(node_id: str, sock: socket.socket,
                                  candado: threading.Lock) -> bool:
    """
    Comprueba cupo y registra la conexion en UNA sola seccion critica.

    DEFENSA: el lock cubre lectura de len(CONECTADOS) Y escritura del dict;
    sin eso, dos hilos que aceptan el cliente 9 y 10 a la vez podrian pasar
    ambos el chequeo de "lleno" antes de que cualquiera escriba.

    Devuelve False si el cluster esta lleno y este node_id no estaba ya dentro.
    """
    viejo: socket.socket | None = None
    with CANDADO:                       # DEFENSA: lock sobre estado compartido
        if len(CONECTADOS) >= config.MAX_NODOS and node_id not in CONECTADOS:
            return False
        anterior = CONECTADOS.get(node_id)
        if anterior is not None and anterior[0] is not sock:
            viejo = anterior[0]
        CONECTADOS[node_id] = (sock, candado)
    if viejo is not None:
        log.info("Reconexion de %s: cierro el socket anterior", node_id)
        _cerrar_socket(viejo)
    return True


def _cantidad_conectados() -> int:
    """Lectura del estado compartido; siempre bajo CANDADO."""
    with CANDADO:                       # DEFENSA: lock sobre estado compartido
        return len(CONECTADOS)


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
    with CANDADO:                       # DEFENSA: lock sobre estado compartido
        actual = CONECTADOS.get(node_id)
        if actual is not None and actual[0] is sock:
            del CONECTADOS[node_id]
        return len(CONECTADOS)


# ============================================================ hilo por cliente

def atender_cliente(sock: socket.socket, direccion: tuple[str, int]) -> None:
    ip = direccion[0]
    node_id: str | None = None
    candado_envio = threading.Lock()  # DEFENSA: lock por socket (escritura)
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
                # M2.2 / requisito 7.2: alta automatica en caliente.
                # Un node_id nuevo -> INSERT en nodos (ACTIVO) + evento
                # ALTA_AUTOMATICA, sin reiniciar servidor ni editar config.
                # El dashboard lo ve en el proximo refresh de /api/nodes.
                if tipo == "HELLO":
                    if node_id is not None:
                        continue                # ya se presento; se ignora
                    entrante = mensaje.get("node_id")
                    if not entrante:
                        protocolo.enviar(sock, protocolo.error("HELLO sin node_id"),
                                         candado_envio)
                        return

                    node_id = entrante
                    if not _intentar_registrar_conectado(node_id, sock, candado_envio):
                        log.warning("Cluster lleno (%d), rechazo %s",
                                    config.MAX_NODOS, entrante)
                        protocolo.enviar(
                            sock,
                            protocolo.error(f"Cluster lleno ({config.MAX_NODOS} nodos)"),
                            candado_envio)
                        return

                    try:
                        es_nuevo, intervalo = repo.registrar_nodo(
                            node_id=node_id,
                            region=mensaje.get("region", "Desconocida"),
                            hostname=mensaje.get("hostname"),
                            so=mensaje.get("so"),
                            ip=ip,
                            intervalo=mensaje.get("intervalo", config.INTERVALO_DEFECTO_SEG),
                        )
                    except Exception as e:                            # noqa: BLE001
                        # Si la base falla, hay que liberar el cupo en CONECTADOS
                        # y avisar al cliente; si no, queda un fantasma ocupando plaza.
                        _quitar_conectado(node_id, sock)
                        node_id = None
                        log.error("No se pudo registrar %s en la base: %s", entrante, e)
                        protocolo.enviar(
                            sock,
                            protocolo.error("No se pudo completar el alta automatica"),
                            candado_envio)
                        return

                    sock.settimeout(config.FACTOR_TIMEOUT * intervalo)
                    protocolo.enviar(sock, protocolo.hello_ok(True, es_nuevo, intervalo),
                                     candado_envio)
                    if es_nuevo:
                        log.info(
                            "*** ALTA AUTOMATICA (7.2): %s (%s) — aparece solo en "
                            "dashboard, evento ALTA_AUTOMATICA, estado ACTIVO ***",
                            node_id, mensaje.get("region"))
                    else:
                        log.info("Reconecta %s (%s) intervalo=%ss (conectados=%d)",
                                 node_id, mensaje.get("region"), intervalo,
                                 _cantidad_conectados())
                    threading.current_thread().name = f"cliente-{node_id}"

                # ---------------------------------------------- METRIC
                elif tipo == "METRIC":
                    if node_id is None:
                        continue                # METRIC antes de HELLO: se ignora
                    # La sesion manda: el node_id del JSON no puede sobreescribir
                    # la identidad fijada en el HELLO (evita mezclar datos).
                    metric_node = mensaje.get("node_id")
                    if metric_node and metric_node != node_id:
                        log.warning("METRIC con node_id ajeno (%s != %s); se ignora",
                                    metric_node, node_id)
                        continue
                    disco = mensaje.get("disco")
                    if not isinstance(disco, dict):
                        log.warning("METRIC de %s sin bloque 'disco'", node_id)
                        continue
                    repo.guardar_metrica(
                        node_id, mensaje.get("timestamp", ""), disco)
                    # M2.5: cada METRIC alimenta metricas -> v_cluster y growth
                    protocolo.enviar(sock, protocolo.metric_ok(), candado_envio)

                # ---------------------------------------------- ACK
                elif tipo == "ACK":
                    if node_id is None:
                        continue
                    cmd_id = mensaje.get("cmd_id")
                    ack_nodo = mensaje.get("node_id")
                    if ack_nodo and ack_nodo != node_id:
                        log.warning("ACK de %s con node_id distinto (%s); se ignora",
                                    node_id, ack_nodo)
                        continue
                    if cmd_id:
                        repo.confirmar_ack(cmd_id)
                        log.info(
                            "ACK (M2.4) de %s cmd_id=%s — emparejado en "
                            "mensajes.ack_en",
                            node_id, cmd_id,
                        )

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
    M2.3 / tarea 2.3 — vigilancia de nodos caidos (estado NO REPORTA).

    Un cable desconectado no manda despedida: este hilo revisa cada
    PERIODO_WATCHDOG_SEG segundos si ultimo_reporte supero el umbral:

        umbral = FACTOR_TIMEOUT x intervalo_seg del nodo  (default: 3 x intervalo)

    Si paso -> marca NO_REPORTA + evento en tabla eventos.
    Si el nodo vuelve a reportar -> ACTIVO + evento RECUPERADO (failover).

    Este hilo es el UNICO que escribe nodos.estado. Ni atender_cliente ni
    registrar_nodo tocan el estado: asi cada transicion queda en eventos.
    """
    factor = config.FACTOR_TIMEOUT
    periodo = config.PERIODO_WATCHDOG_SEG
    log.info(
        "Watchdog activo (M2.3): cada %ds, umbral = %dx intervalo_seg de cada nodo",
        periodo, factor,
    )
    try:
        while not APAGANDO.wait(periodo):
            try:
                for node_id in repo.marcar_nodos_caidos(factor):
                    log.warning(
                        "*** NO REPORTA (M2.3): %s — sin reportes dentro de "
                        "%dx su intervalo; evento NO_REPORTA en bitacora ***",
                        node_id, factor,
                    )
                for node_id in repo.marcar_nodos_recuperados(factor):
                    log.info(
                        "*** RECUPERADO (M2.3): %s — volvio a reportar; "
                        "ACTIVO + evento RECUPERADO (failover) ***",
                        node_id,
                    )
            except Exception as e:                                # noqa: BLE001
                # Un ciclo fallido no puede matar el watchdog: los otros nodos
                # siguen dependiendo de el para detectar caidas.
                log.error("Error en watchdog: %s", e)
    finally:
        cerrar_conexion_del_hilo()


# ================================================================ despachador

def _despachar_un_mensaje(m: dict) -> None:
    """
    M2.4 — envia un CMD a un nodo concreto.

    Flujo:
      1. Buscar socket en CONECTADOS (bajo CANDADO, sin I/O)
      2. marcar_enviado(cmd_id)  -> enviado_en en BD
      3. protocolo.cmd(..., cmd_id) por el socket
      4. El cliente responde ACK(cmd_id) -> confirmar_ack -> ack_en en BD
    """
    cmd_id = m["cmd_id"]
    node_id = m["node_id"]
    with CANDADO:                       # DEFENSA: lookup rapido en CONECTADOS
        destino = CONECTADOS.get(node_id)
    if destino is None:
        repo.marcar_fallido(cmd_id, "El nodo no esta conectado")
        log.warning("M2.4: no se envio a %s (desconectado) cmd_id=%s",
                    node_id, cmd_id)
        return

    sock, candado_envio = destino
    repo.marcar_enviado(cmd_id)
    protocolo.enviar(sock, protocolo.cmd(
        cmd_id, m["accion"], m.get("texto"), m.get("valor")),
        candado_envio)
    texto = m.get("texto") or m["accion"]
    log.info("M2.4 -> %s cmd_id=%s : %s", node_id, cmd_id, texto)


def despachador() -> None:
    """
    M2.4 / requisito 7.1 — mensajeria del servidor hacia los clientes.

    Puente API/dashboard -> sockets. Lee mensajes PENDIENTE de la BD y los
    manda como CMD (cada uno con cmd_id unico para emparejar el ACK).

    Unicast: una fila por node_id. Broadcast: N filas (una por nodo), cada
    una con su cmd_id; este hilo las despacha igual, una por ciclo.

    Se marca ENVIADO ANTES de mandarlo (ver docs/CAMBIOS.md).
    """
    log.info("Despachador activo (M2.4, cada %ds)", config.PERIODO_DESPACHADOR_SEG)
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
                    _despachar_un_mensaje(m)
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
    with CANDADO:                       # DEFENSA: lock sobre estado compartido
        sockets = [s for s, _ in CONECTADOS.values()]
        CONECTADOS.clear()
    for s in sockets:
        _cerrar_socket(s)


def _pedir_apagado(signum: int, _frame: object) -> None:
    """SIGINT/SIGTERM desbloquean accept() via APAGANDO + timeout del socket."""
    log.info("Senal %s recibida; apagando servidor...", signum)
    APAGANDO.set()
    global _servidor_sock
    if _servidor_sock is not None:
        try:
            _servidor_sock.close()
        except OSError:
            pass


def main() -> None:
    global _servidor_sock
    config.asegurar_directorios()
    if not probar_conexion():
        log.error("Sin MySQL no arranca. Revisen el .env y que el servicio este arriba.")
        return

    signal.signal(signal.SIGINT, _pedir_apagado)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _pedir_apagado)

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _servidor_sock = servidor
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((config.SOCKET_HOST, config.SOCKET_PORT))
    servidor.listen(config.MAX_NODOS + 5)
    servidor.settimeout(1.0)            # permite revisar APAGANDO entre accepts
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
        # Hilo principal: bind + listen + accept. Por cada TCP aceptado lanza
        # UN hilo daemon que solo atiende a ese cliente; el principal sigue
        # en accept() y no toca sockets de clientes ajenos.
        while not APAGANDO.is_set():
            try:
                sock, direccion = servidor.accept()
            except socket.timeout:
                continue
            except OSError:
                if APAGANDO.is_set():
                    break
                raise
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
        _servidor_sock = None
        # Cerrar los sockets desbloquea a los hilos de cliente, que asi corren
        # su finally y devuelven su conexion a MySQL.
        _cerrar_todas_las_conexiones()
        for h in hilos_fondo:
            h.join(timeout=5)
        cerrar_conexion_del_hilo()
        log.info("Servidor detenido")


if __name__ == "__main__":
    main()
