"""
Nodo Cliente (servidor regional) — modulo M1.  Responsable: Martin.

    python -m cliente.main --node-id CNS-LPZ-01 --region "La Paz"
    python -m cliente.main --config cliente/config.json

ESQUELETO: la reconexion, los dos hilos y el intervalo en caliente ya estan
resueltos. Lo marcado con # TODO Martin falta.

DOS HILOS:
    principal -> envia METRIC cada N segundos
    receptor  -> escucha CMD del servidor, escribe el .log y responde ACK
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


class NodoCliente:
    def __init__(self, node_id: str, region: str, host: str, puerto: int,
                 intervalo: int) -> None:
        self.node_id = node_id
        self.region = region
        self.host = host
        self.puerto = puerto
        self.intervalo = intervalo

        self.sock: socket.socket | None = None
        self.conectado = threading.Event()
        self.apagando = threading.Event()
        # Un Event, NO time.sleep(): asi SET_INTERVAL corta la espera al
        # instante en vez de aplicarse recien cuando el hilo despierta.
        self.despertador = threading.Event()

        self.archivo_log = config.DIR_LOGS / f"cliente_{node_id}.log"

    # ------------------------------------------------------------- log local
    def registrar_en_log(self, texto: str) -> None:
        """Requisito 7.1: todo mensaje recibido se escribe en un archivo .log."""
        marca = protocolo.ahora_iso()
        with open(self.archivo_log, "a", encoding="utf-8") as f:
            f.write(f"[{marca}] {texto}\n")

    # ------------------------------------------------------------- conexion
    def conectar(self) -> bool:
        espera = 1
        while not self.apagando.is_set():
            try:
                self.sock = socket.create_connection((self.host, self.puerto), timeout=10)
                self.sock.settimeout(None)
                protocolo.enviar(self.sock, protocolo.hello(
                    self.node_id, self.region, platform.node(),
                    f"{platform.system()} {platform.release()}", self.intervalo))
                self.conectado.set()
                log.info("Conectado a %s:%s", self.host, self.puerto)
                return True
            except OSError as e:
                log.warning("Sin servidor (%s). Reintento en %ss", e, espera)
                time.sleep(espera)
                espera = min(espera * 2, 30)      # backoff 1,2,4,8,16,30
        return False

    # ------------------------------------------------------- hilo receptor
    def escuchar(self) -> None:
        try:
            for mensaje in protocolo.LectorLineas(self.sock):
                tipo = mensaje.get("tipo")

                if tipo == "HELLO_OK":
                    if mensaje.get("nuevo"):
                        log.info("El servidor me dio de alta automaticamente")
                    self.aplicar_intervalo(mensaje.get("intervalo", self.intervalo))

                elif tipo == "CMD":
                    accion = mensaje.get("accion")
                    if accion == protocolo.ACCION_SET_INTERVAL:
                        self.aplicar_intervalo(int(mensaje["valor"]))
                        self.registrar_en_log(f"CMD SET_INTERVAL -> {mensaje['valor']}s")
                    else:
                        self.registrar_en_log(f"CMD MENSAJE: {mensaje.get('texto')}")
                        log.info("Mensaje del servidor: %s", mensaje.get("texto"))
                    # ACK: sin esto el servidor nunca marca CONFIRMADO
                    protocolo.enviar(self.sock, protocolo.ack(mensaje["cmd_id"], self.node_id))

                elif tipo == "METRIC_OK":
                    pass                          # confirmacion normal, no se loguea
        except (OSError, AttributeError):
            pass
        finally:
            self.conectado.clear()

    def aplicar_intervalo(self, segundos: int) -> None:
        if segundos > 0 and segundos != self.intervalo:
            log.info("Intervalo: %ss -> %ss", self.intervalo, segundos)
            self.intervalo = segundos
            self.despertador.set()                # corta la espera en curso

    # -------------------------------------------------------------- bucle
    def ejecutar(self) -> None:
        while not self.apagando.is_set():
            if not self.conectar():
                return
            threading.Thread(target=self.escuchar, name="receptor", daemon=True).start()

            leer_disco()                          # siembra el delta de IOPS
            while self.conectado.is_set() and not self.apagando.is_set():
                try:
                    protocolo.enviar(self.sock, protocolo.metric(self.node_id, leer_disco()))
                except OSError:
                    log.warning("Se corto el envio. Reconectando...")
                    self.conectado.clear()
                    break
                self.despertador.clear()
                self.despertador.wait(self.intervalo)

            try:
                self.sock.close()
            except OSError:
                pass
        # TODO Martin: probar apagando el servidor a mitad de sesion. El cliente
        #          NO puede morir con traceback: debe reintentar solo.


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
        datos = json.loads(Path(a.config).read_text(encoding="utf-8"))
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
        nodo.apagando.set()
        log.info("Cliente detenido")


if __name__ == "__main__":
    main()
