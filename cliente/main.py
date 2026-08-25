"""
Nodo Cliente (servidor regional) — modulo M1.  Responsable: Martin.

    python -m cliente.main --node-id CNS-LPZ-01 --region "La Paz"
    python -m cliente.main --config cliente/config.json

ESQUELETO: la reconexion, los dos hilos y el intervalo en caliente ya estan
resueltos. Lo marcado con # TODO Martin falta.

DOS HILOS POR SESION:
    principal -> envia METRIC cada N segundos
    receptor  -> escucha CMD del servidor, escribe el .log y responde ACK

POR QUE EXISTE LA CLASE Sesion
------------------------------
Cada conexion tiene su propio socket, su propio candado de escritura y su
propio indicador de "sigue viva". La primera version guardaba todo eso en el
objeto NodoCliente y lo reutilizaba en cada reconexion, con dos consecuencias:
el hilo receptor VIEJO seguia vivo (cerrar un socket desde otro hilo no
desbloquea un recv en curso) y, al despertar, apagaba con su `finally` la
conexion NUEVA. Con estado por sesion, un receptor zombi no puede tocar la
sesion actual.
"""
from __future__ import annotations

import argparse
import json
import logging
import platform
import socket
import threading
import time
from pathlib import Path

from cliente.metricas import leer_disco
from comun import config, protocolo

log = logging.getLogger("cliente")


class Sesion:
    """Todo lo que pertenece a UNA conexion y muere con ella."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.candado_envio = threading.Lock()   # el receptor manda ACK mientras
                                                # el principal manda METRIC
        self.viva = threading.Event()
        self.viva.set()

    def cerrar(self) -> None:
        self.viva.clear()
        try:
            # shutdown SI desbloquea un recv() en curso; close() solo no.
            # Sin esto el hilo receptor queda colgado para siempre cuando la
            # conexion es "medio abierta" (cable, firewall que descarta).
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


class NodoCliente:
    def __init__(self, node_id: str, region: str, host: str, puerto: int,
                 intervalo: int) -> None:
        self.node_id = node_id
        self.region = region
        self.host = host
        self.puerto = puerto
        self.intervalo = config.acotar_intervalo(intervalo)

        self.apagando = threading.Event()
        # Un Event, NO time.sleep(): asi SET_INTERVAL corta la espera al
        # instante en vez de aplicarse recien cuando el hilo despierta.
        self.despertador = threading.Event()
        self.rechazado = False

        config.asegurar_directorios()
        self.archivo_log = config.DIR_LOGS / f"cliente_{node_id}.log"

    # ------------------------------------------------------------- log local
    def registrar_en_log(self, texto: str) -> None:
        """Requisito 7.1: todo mensaje recibido se escribe en un archivo .log."""
        try:
            with open(self.archivo_log, "a", encoding="utf-8") as f:
                f.write(f"[{protocolo.ahora_iso()}] {texto}\n")
        except OSError as e:
            # Si el disco esta lleno o no hay permisos, se avisa y se sigue.
            # Ironico en un monitor de discos: sin esta guarda, el cliente
            # dejaria de responder justo cuando el disco se llena.
            log.error("No se pudo escribir el log: %s", e)

    # ------------------------------------------------------------- conexion
    def conectar(self) -> Sesion | None:
        espera = 1
        while not self.apagando.is_set():
            try:
                sock = socket.create_connection((self.host, self.puerto), timeout=10)
                sock.settimeout(None)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                sesion = Sesion(sock)
                protocolo.enviar(sock, protocolo.hello(
                    self.node_id, self.region, platform.node(),
                    f"{platform.system()} {platform.release()}", self.intervalo),
                    sesion.candado_envio)
                log.info("Conectado a %s:%s", self.host, self.puerto)
                return sesion
            except OSError as e:
                log.warning("Sin servidor (%s). Reintento en %ss", e, espera)
                if self.apagando.wait(espera):
                    return None
                espera = min(espera * 2, 30)      # backoff 1,2,4,8,16,30
        return None

    # ------------------------------------------------------- hilo receptor
    def escuchar(self, sesion: Sesion) -> None:
        try:
            for mensaje in protocolo.LectorLineas(sesion.sock):
                try:
                    self._procesar(sesion, mensaje)
                except Exception as e:                            # noqa: BLE001
                    # Un CMD raro no puede tumbar el hilo receptor: si lo hace,
                    # el nodo deja de responder mensajes sin que nadie se entere.
                    log.exception("Error procesando %s: %s", mensaje.get("tipo"), e)
        except protocolo.ErrorProtocolo as e:
            log.warning("El servidor violo el protocolo: %s", e)
        except OSError:
            pass                                   # conexion cortada: normal
        except Exception as e:                                    # noqa: BLE001
            log.exception("Error en el hilo receptor: %s", e)
        finally:
            sesion.viva.clear()
            # Despertar al hilo emisor: si no, con un intervalo de 60 s el
            # cliente tardaria hasta un minuto en enterarse de que se cayo.
            self.despertador.set()

    def _procesar(self, sesion: Sesion, mensaje: dict) -> None:
        tipo = mensaje.get("tipo")

        if tipo == "HELLO_OK":
            if mensaje.get("nuevo"):
                log.info("El servidor me dio de alta automaticamente")
            self.aplicar_intervalo(mensaje.get("intervalo", self.intervalo))

        elif tipo == "ERROR":
            # El servidor nos rechaza (cluster lleno, HELLO invalido). No tiene
            # sentido reintentar en bucle: se avisa y se termina.
            log.error("El servidor rechazo la conexion: %s", mensaje.get("motivo"))
            self.rechazado = True
            self.apagando.set()
            sesion.viva.clear()

        elif tipo == "CMD":
            cmd_id = mensaje.get("cmd_id")
            # El ACK va PRIMERO. Si se manda al final, cualquier fallo al
            # aplicar el comando (disco lleno al escribir el log, un valor
            # invalido) deja el mensaje sin confirmar para siempre.
            if cmd_id:
                protocolo.enviar(sesion.sock,
                                 protocolo.ack(cmd_id, self.node_id),
                                 sesion.candado_envio)

            accion = mensaje.get("accion")
            if accion == protocolo.ACCION_SET_INTERVAL:
                nuevo = mensaje.get("valor")
                if self.aplicar_intervalo(nuevo):
                    self.registrar_en_log(f"CMD SET_INTERVAL -> {nuevo}s")
                else:
                    self.registrar_en_log(f"CMD SET_INTERVAL rechazado: valor {nuevo!r}")
                    log.warning("SET_INTERVAL con valor invalido: %r", nuevo)
            elif accion == protocolo.ACCION_MENSAJE:
                texto = mensaje.get("texto")
                self.registrar_en_log(f"CMD MENSAJE: {texto}")
                log.info("Mensaje del servidor: %s", texto)
            else:
                self.registrar_en_log(f"CMD desconocido: {accion!r}")
                log.warning("Accion desconocida: %r", accion)

        elif tipo == "METRIC_OK":
            pass                          # confirmacion normal, no se loguea

    def aplicar_intervalo(self, segundos: object) -> bool:
        """Devuelve True si el valor era valido y se aplico."""
        try:
            pedido = int(segundos)                                # type: ignore[arg-type]
        except (TypeError, ValueError):
            return False
        if not (config.INTERVALO_MIN_SEG <= pedido <= config.INTERVALO_MAX_SEG):
            return False
        if pedido != self.intervalo:
            log.info("Intervalo: %ss -> %ss", self.intervalo, pedido)
            self.intervalo = pedido
            self.despertador.set()                # corta la espera en curso
        return True

    # -------------------------------------------------------------- bucle
    def ejecutar(self) -> None:
        while not self.apagando.is_set():
            sesion = self.conectar()
            if sesion is None:
                return

            receptor = threading.Thread(target=self.escuchar, args=(sesion,),
                                        name="receptor", daemon=True)
            receptor.start()

            leer_disco()                          # siembra el delta de IOPS
            # Esperar un intervalo antes del primer envio: si no, el delta se
            # calcula sobre unos milisegundos y el primer METRIC reporta un
            # pico de IOPS que no existio.
            self.despertador.clear()
            self.despertador.wait(self.intervalo)

            while sesion.viva.is_set() and not self.apagando.is_set():
                self.despertador.clear()
                try:
                    protocolo.enviar(sesion.sock,
                                     protocolo.metric(self.node_id, leer_disco()),
                                     sesion.candado_envio)
                except OSError:
                    log.warning("Se corto el envio. Reconectando...")
                    sesion.viva.clear()
                    break
                except Exception as e:                            # noqa: BLE001
                    # El TODO de mas abajo lo exige: el cliente NO puede morir
                    # con un traceback. Se salta esta muestra y se sigue.
                    log.exception("Error preparando la metrica: %s", e)
                self.despertador.wait(self.intervalo)

            sesion.cerrar()
            receptor.join(timeout=5)
            if receptor.is_alive():
                log.warning("El hilo receptor no termino a tiempo")
        # TODO Martin: probar apagando el servidor a mitad de sesion, y tambien
        #              desenchufando la red (que es distinto: no llega FIN).
        #              El cliente NO puede morir con traceback en ninguno de
        #              los dos casos: debe reintentar solo.

    def detener(self) -> None:
        self.apagando.set()
        self.despertador.set()


def main() -> None:
    p = argparse.ArgumentParser(description="Nodo cliente del Storage Cluster CNS")
    p.add_argument("--config", help="Ruta a un config.json")
    p.add_argument("--node-id")
    p.add_argument("--region")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--puerto", type=int, default=config.SOCKET_PORT)
    p.add_argument("--intervalo", type=int, default=config.INTERVALO_DEFECTO_SEG)
    a = p.parse_args()

    if a.config:
        try:
            datos = json.loads(Path(a.config).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            p.error(f"No se pudo leer {a.config}: {e}")
        a.node_id = datos.get("node_id", a.node_id)
        a.region = datos.get("region", a.region)
        a.host = datos.get("host", a.host)
        a.puerto = datos.get("puerto", a.puerto)
        a.intervalo = datos.get("intervalo", a.intervalo)

    if not a.node_id or not a.region:
        p.error("Faltan --node-id y --region (o un --config que los traiga)")

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{a.node_id}] %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    nodo = NodoCliente(a.node_id, a.region, a.host, a.puerto, a.intervalo)
    try:
        nodo.ejecutar()
    except KeyboardInterrupt:
        nodo.detener()
        log.info("Cliente detenido")
    raise SystemExit(1 if nodo.rechazado else 0)


if __name__ == "__main__":
    main()
