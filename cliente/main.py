"""
Nodo Cliente (servidor regional) — modulo M1 (v2).  Responsable: Martin.

    python -m cliente.main --node-id CNS-LPZ-01 --region "La Paz"
    python -m cliente.main --config cliente/config.json
    python -m cliente.main --node-id X --region Y --recursos disco,ram,cpu
    python -m cliente.main --node-id X --region Y --caos 30   # demo de fallo
                                                              # intermitente

DOS HILOS POR SESION:
    principal -> guarda la muestra, sincroniza lo atrasado y envia METRIC
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

LO NUEVO DE LA VERSION 2
------------------------
1. NADA SE PIERDE. Cada medicion se escribe primero en la base local del nodo
   (cliente/almacen.py) y recien despues se intenta enviar. Sin red, la
   medicion sigue ahi.

2. SINCRONIZACION AL VOLVER. Cuando la conexion se restablece, el cliente le
   manda al servidor todo lo que se perdio, en lotes y en orden, y espera el
   SYNC_OK antes de darlo por entregado. El backlog se drena EN PARALELO con
   las metricas en vivo: primero un lote de atrasadas, despues la de ahora. Asi
   el dashboard vuelve a tener tiempo real de inmediato y el hueco se rellena
   por detras.

3. EL RELOJ DEL NODO NO DECIDE NADA. Cada muestra viaja con `mono_ns`, el reloj
   monotonico del proceso, que no se puede retrasar ni ajustar. El servidor
   fecha con ESO y con su propia hora. Ademas el cliente vigila su propio reloj
   y anota en su bitacora si alguien se lo cambia.

4. MIDE MAS QUE EL DISCO. RAM, CPU, red y todas las unidades (incluido el
   pendrive que alguien enchufe). Que reportar se decide en la config del nodo
   o desde el dashboard con CMD SET_RECURSOS, sin tocar esta maquina.
"""
from __future__ import annotations

import argparse
import json
import logging
import platform
import random
import socket
import threading
import time
from pathlib import Path

from cliente.almacen import AlmacenLocal
from cliente.metricas import (capacidades, detectar_cambios_discos, leer_todo,
                              sembrar)
from comun import config, protocolo

log = logging.getLogger("cliente")

VERSION_AGENTE = "2.0.0"


class Sesion:
    """Todo lo que pertenece a UNA conexion y muere con ella."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.candado_envio = threading.Lock()   # el receptor manda ACK mientras
                                                # el principal manda METRIC
        self.viva = threading.Event()
        self.viva.set()
        # Se levanta cuando llega un SYNC_OK. El hilo principal espera aqui
        # despues de mandar un lote: sin esa espera mandaria los 200 lotes de
        # golpe y el servidor tendria que bufferearlos todos.
        self.sync_respondido = threading.Event()
        self.sync_hasta = 0

    def cerrar(self) -> None:
        self.viva.clear()
        self.sync_respondido.set()              # no dejar a nadie esperando
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
                 intervalo: int, recursos: list[str] | None = None,
                 caos: int = 0, sede: str = "") -> None:
        self.node_id = node_id
        # region = DEPARTAMENTO (la regional del enunciado).
        # sede    = la oficina concreta. El departamento de La Paz tiene dos
        #           sedes con servidor propio: La Paz y El Alto.
        self.region = region
        self.sede = sede or region
        self.host = host
        self.puerto = puerto
        self.intervalo = config.acotar_intervalo(intervalo)
        self.recursos = list(recursos or config.RECURSOS_DEFECTO)
        self.caos = max(0, int(caos))

        self.apagando = threading.Event()
        # Un Event, NO time.sleep(): asi SET_INTERVAL corta la espera al
        # instante en vez de aplicarse recien cuando el hilo despierta.
        self.despertador = threading.Event()
        self.rechazado = False

        config.asegurar_directorios()
        self.archivo_log = config.DIR_LOGS / f"cliente_{node_id}.log"
        self.almacen = AlmacenLocal(node_id)

        # Referencia para detectar que alguien cambio la hora de ESTA maquina.
        self._ref_wall = time.time()
        self._ref_mono = time.monotonic()

    # ------------------------------------------------------------- log local
    def registrar_en_log(self, texto: str, tipo: str = "MENSAJE") -> None:
        """
        Requisito 7.1: todo mensaje recibido se escribe en un archivo .log.

        Se escribe en los DOS sitios: el .log de texto porque el enunciado lo
        pide con esas palabras, y la bitacora de la base local porque un
        archivo de texto no se puede consultar por rango ni sobrevive bien a un
        corte de luz a mitad de escritura.
        """
        try:
            with open(self.archivo_log, "a", encoding="utf-8") as f:
                f.write(f"[{protocolo.ahora_iso()}] {texto}\n")
        except OSError as e:
            # Si el disco esta lleno o no hay permisos, se avisa y se sigue.
            # Ironico en un monitor de discos: sin esta guarda, el cliente
            # dejaria de responder justo cuando el disco se llena.
            log.error("No se pudo escribir el log: %s", e)
        self.almacen.anotar(tipo, texto)

    # ------------------------------------------------------- vigilancia reloj
    def revisar_reloj(self) -> None:
        """
        Detecta que alguien cambio la hora del sistema en ESTA maquina.

        La comparacion es entre dos relojes: el de pared (time.time(), que se
        puede mover) y el monotonico (time.monotonic(), que no). Si la
        diferencia entre ambos salta, es porque el de pared se movio.

        NO se corrige nada ni se deja de medir: las metricas ya viajan fechadas
        por el reloj monotonico, asi que un cambio de hora no puede ensuciar ni
        una fila del historico. Esto es para que quede constancia en el nodo, y
        para que el operador entienda por que el servidor le marca desvio.
        """
        esperado = self._ref_wall + (time.monotonic() - self._ref_mono)
        salto = time.time() - esperado
        if abs(salto) > 2.0:
            self._ref_wall = time.time()
            self._ref_mono = time.monotonic()
            log.warning("Alguien cambio la hora del sistema (%+.1f s). "
                        "Las metricas NO se ven afectadas: las fecha el "
                        "servidor con el reloj monotonico.", salto)
            self.registrar_en_log(
                f"Cambio de hora del sistema detectado ({salto:+.1f} s). "
                f"Las metricas se fechan con el reloj del servidor.",
                tipo="RELOJ_CAMBIADO")

    # ------------------------------------------------------------- conexion
    def conectar(self) -> Sesion | None:
        espera = 1.0
        while not self.apagando.is_set():
            try:
                sock = socket.create_connection((self.host, self.puerto), timeout=10)
                sock.settimeout(None)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                sesion = Sesion(sock)
                protocolo.enviar(sock, protocolo.hello(
                    self.node_id, self.region, platform.node(),
                    f"{platform.system()} {platform.release()}", self.intervalo,
                    sede=self.sede,
                    capacidades=capacidades(),
                    pendientes=self.almacen.contar_pendientes(),
                    agente=VERSION_AGENTE),
                    sesion.candado_envio)
                log.info("Conectado a %s:%s", self.host, self.puerto)
                return sesion
            except OSError as e:
                log.warning("Sin servidor (%s). Reintento en %.1fs", e, espera)
                if self.apagando.wait(espera):
                    return None
                # Backoff exponencial CON JITTER. Sin el jitter, nueve nodos
                # que se cayeron juntos (se reinicio el servidor) reintentan
                # todos en el mismo instante, una y otra vez: nueve conexiones
                # simultaneas cada vez, en vez de repartidas. Es el "thundering
                # herd" y con esta linea desaparece.
                espera = min(espera * 2, 30) * (0.7 + random.random() * 0.6)
                espera = min(max(espera, 1.0), 45.0)
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
            sesion.sync_respondido.set()
            # Despertar al hilo emisor: si no, con un intervalo de 60 s el
            # cliente tardaria hasta un minuto en enterarse de que se cayo.
            self.despertador.set()

    def _procesar(self, sesion: Sesion, mensaje: dict) -> None:
        tipo = mensaje.get("tipo")

        if tipo == "HELLO_OK":
            if mensaje.get("nuevo"):
                log.info("El servidor me dio de alta automaticamente")
                self.registrar_en_log("Alta automatica en el servidor",
                                      tipo="ALTA")
            self.aplicar_intervalo(mensaje.get("intervalo", self.intervalo))
            self.aplicar_recursos(mensaje.get("recursos"), avisar=False)

            desvio = mensaje.get("desvio_seg")
            if desvio is not None and abs(float(desvio)) > config.UMBRAL_RELOJ_SEG:
                log.warning("El servidor dice que mi reloj esta %+.1f s "
                            "desviado. Las metricas las fecha el servidor.",
                            float(desvio))
                self.registrar_en_log(
                    f"Reloj local desviado {float(desvio):+.1f} s respecto al "
                    f"servidor", tipo="RELOJ_DESVIADO")

        elif tipo == "ERROR":
            # El servidor nos rechaza (cluster lleno, HELLO invalido). No tiene
            # sentido reintentar en bucle: se avisa y se termina.
            log.error("El servidor rechazo la conexion: %s", mensaje.get("motivo"))
            self.registrar_en_log(f"Rechazado: {mensaje.get('motivo')}",
                                  tipo="RECHAZADO")
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

            elif accion == protocolo.ACCION_SET_RECURSOS:
                # Requisito de flexibilidad: el operador decide desde el
                # dashboard que mide este nodo, sin entrar a esta maquina.
                self.aplicar_recursos(
                    [x.strip() for x in str(mensaje.get("texto") or "").split(",")])

            elif accion == protocolo.ACCION_PING:
                protocolo.enviar(sesion.sock, protocolo.pong(self.node_id),
                                 sesion.candado_envio)

            elif accion == protocolo.ACCION_SOLICITAR_SYNC:
                # El servidor pide explicitamente lo atrasado. Se despierta al
                # hilo principal, que es el unico que manda lotes: dos hilos
                # drenando el mismo buffer se pisarian los SYNC_OK.
                self.registrar_en_log("CMD SOLICITAR_SYNC", tipo="SYNC")
                self.despertador.set()

            elif accion == protocolo.ACCION_MENSAJE:
                texto = mensaje.get("texto")
                self.registrar_en_log(f"CMD MENSAJE: {texto}")
                log.info("Mensaje del servidor: %s", texto)

            else:
                self.registrar_en_log(f"CMD desconocido: {accion!r}")
                log.warning("Accion desconocida: %r", accion)

        elif tipo == "METRIC_OK":
            # Confirmacion de una metrica EN VIVO. Es lo que permite marcarla
            # entregada en la base local: hasta que llega, la muestra sigue
            # contando como pendiente y se reenviaria tras una caida.
            seq = int(mensaje.get("seq") or 0)
            if seq:
                self.almacen.marcar_entregadas(seq)

        elif tipo == "SYNC_OK":
            # Confirmacion de un LOTE. hasta_seq es acumulativo: todo lo que
            # sea menor o igual quedo guardado en el servidor.
            sesion.sync_hasta = int(mensaje.get("hasta_seq") or 0)
            if sesion.sync_hasta:
                self.almacen.marcar_entregadas(sesion.sync_hasta)
            sesion.sync_respondido.set()

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

    def aplicar_recursos(self, pedidos: object, avisar: bool = True) -> bool:
        """
        Cambia que mide este nodo. Un nombre desconocido se descarta con aviso;
        si no queda ninguno valido, se conserva lo que habia — dejar a un nodo
        sin medir nada por un CMD mal escrito seria peor que ignorarlo.
        """
        if not isinstance(pedidos, list):
            return False
        validos = [str(r).strip().lower() for r in pedidos if str(r).strip()]
        validos = [r for r in validos if r in config.RECURSOS_DISPONIBLES]
        if not validos or validos == self.recursos:
            return False
        anterior = self.recursos
        self.recursos = validos
        log.info("Recursos a reportar: %s -> %s",
                 ",".join(anterior), ",".join(validos))
        if avisar:
            self.registrar_en_log(f"CMD SET_RECURSOS -> {','.join(validos)}",
                                  tipo="RECURSOS")
        return True

    # ------------------------------------------------------- sincronizacion
    def _enviar_lote(self, sesion: Sesion) -> int:
        """
        Manda UN lote de muestras atrasadas y espera su SYNC_OK.

        Devuelve cuantas quedaron confirmadas (0 si no habia nada o si el
        servidor no contesto a tiempo).

        Se espera la confirmacion antes del siguiente lote a proposito: es
        control de flujo. Un nodo que estuvo un dia sin red tiene 8.600
        muestras; mandarlas todas de golpe le mete al servidor 8.600 INSERT en
        un solo mensaje mientras los otros ocho nodos siguen reportando.
        """
        lote = self.almacen.pendientes(config.SYNC_TAM_LOTE)
        if not lote:
            return 0

        sesion.sync_respondido.clear()
        sesion.sync_hasta = 0
        try:
            protocolo.enviar(sesion.sock,
                             protocolo.metric_batch(self.node_id, lote),
                             sesion.candado_envio)
        except OSError as e:
            log.warning("Se corto el envio del lote: %s", e)
            sesion.viva.clear()
            return 0

        # 30 s es generoso: el servidor tiene que insertar hasta 100 filas y
        # contestar. Si no llega, no se pierde nada — el lote sigue pendiente
        # en la base local y se reintenta en la proxima vuelta.
        if not sesion.sync_respondido.wait(30):
            log.warning("El servidor no confirmo el lote a tiempo")
            return 0
        return len(lote) if sesion.sync_hasta else 0

    def sincronizar(self, sesion: Sesion, max_lotes: int = 3) -> int:
        """
        Drena hasta `max_lotes` de golpe. Se llama al conectar y despues una
        vez por ciclo, para que el hueco se rellene sin frenar el tiempo real.
        """
        total = 0
        for _ in range(max_lotes):
            if not sesion.viva.is_set() or self.apagando.is_set():
                break
            enviadas = self._enviar_lote(sesion)
            if not enviadas:
                break
            total += enviadas
            if config.SYNC_PAUSA_SEG:
                time.sleep(config.SYNC_PAUSA_SEG)
        return total

    # -------------------------------------------------------------- bucle
    def ejecutar(self) -> None:
        while not self.apagando.is_set():
            sesion = self.conectar()
            if sesion is None:
                return

            receptor = threading.Thread(target=self.escuchar, args=(sesion,),
                                        name="receptor", daemon=True)
            receptor.start()

            sembrar()                             # siembra los deltas de IOPS,
                                                  # CPU y red

            # Lo primero al recuperar la red: contarle al servidor lo que se
            # perdio. Este es el "proceso de sincronizacion" del enunciado y es
            # lo que hace que una caida deje un hueco temporal y no permanente.
            atrasadas = self.almacen.contar_pendientes()
            if atrasadas:
                log.info("Sincronizando: %d muestras pendientes de la caida "
                         "anterior", atrasadas)
                self.registrar_en_log(
                    f"Reconexion: sincronizando {atrasadas} muestras guardadas "
                    f"mientras no habia red", tipo="SYNC")
                recuperadas = self.sincronizar(sesion, max_lotes=10)
                log.info("Sincronizadas %d muestras (quedan %d)",
                         recuperadas, self.almacen.contar_pendientes())

            # Esperar un intervalo antes del primer envio: si no, el delta se
            # calcula sobre unos milisegundos y el primer METRIC reporta un
            # pico de IOPS que no existio.
            self.despertador.clear()
            self.despertador.wait(self.intervalo)

            ciclos = 0
            while sesion.viva.is_set() and not self.apagando.is_set():
                self.despertador.clear()
                ciclos += 1
                self.revisar_reloj()

                # 1. MEDIR Y GUARDAR. Esto pasa SIEMPRE, y pasa ANTES de
                #    intentar enviar. Si el envio falla, el dato ya esta a
                #    salvo: es toda la diferencia con la version 1.
                try:
                    disco, recursos = leer_todo(self.recursos)
                    mono = protocolo.mono_ns()
                    marca = protocolo.ahora_iso()
                    seq = self.almacen.guardar(disco, recursos, mono, marca)
                except Exception as e:                            # noqa: BLE001
                    # El cliente NO puede morir con un traceback. Se salta esta
                    # muestra y se sigue con la siguiente.
                    log.exception("Error preparando la metrica: %s", e)
                    self.despertador.wait(self.intervalo)
                    continue

                # 2. Cambios en las unidades: un pendrive que aparece o se va.
                #    Se anota aunque no haya red; el servidor saca la misma
                #    conclusion por su cuenta cuando le lleguen los datos.
                for tipo_ev, detalle in detectar_cambios_discos():
                    log.info("%s: %s", tipo_ev, detalle)
                    self.registrar_en_log(detalle, tipo=tipo_ev)

                # 3. Drenar un lote de atrasadas antes de la de ahora, para que
                #    el hueco se rellene sin frenar el tiempo real.
                if self.almacen.contar_pendientes() > 1:
                    self.sincronizar(sesion, max_lotes=1)

                # 4. Enviar la de ahora. Si falla, no se pierde: quedo en la
                #    base local con entregada=0.
                try:
                    protocolo.enviar(
                        sesion.sock,
                        protocolo.metric(self.node_id, disco, seq=seq,
                                         recursos=recursos, mono=mono,
                                         marca=marca),
                        sesion.candado_envio)
                except OSError:
                    log.warning("Se corto el envio. La muestra queda guardada. "
                                "Reconectando...")
                    sesion.viva.clear()
                    break
                except Exception as e:                            # noqa: BLE001
                    log.exception("Error enviando la metrica: %s", e)

                # 5. Mantenimiento barato, cada 20 ciclos.
                if ciclos % 20 == 0:
                    try:
                        self.almacen.podar()
                    except Exception as e:                        # noqa: BLE001
                        log.warning("No se pudo podar la base local: %s", e)

                # 6. Modo caos: corta la conexion a proposito, para poder
                #    demostrar en vivo el fallo intermitente y la
                #    sincronizacion posterior sin desenchufar un cable.
                if self.caos and random.randint(1, 100) <= self.caos:
                    log.warning("[CAOS] Corto la conexion a proposito")
                    self.registrar_en_log("Corte simulado (modo caos)",
                                          tipo="CAOS")
                    sesion.viva.clear()
                    break

                self.despertador.wait(self.intervalo)

            sesion.cerrar()
            receptor.join(timeout=5)
            if receptor.is_alive():
                log.warning("El hilo receptor no termino a tiempo")
            if not self.apagando.is_set():
                pend = self.almacen.contar_pendientes()
                self.registrar_en_log(
                    f"Conexion perdida. {pend} muestras guardadas para "
                    f"sincronizar al volver", tipo="DESCONEXION")

    def detener(self) -> None:
        self.apagando.set()
        self.despertador.set()
        try:
            self.almacen.cerrar()
        except Exception:                                         # noqa: BLE001
            pass


def main() -> None:
    p = argparse.ArgumentParser(description="Nodo cliente del Storage Cluster CNS")
    p.add_argument("--config", help="Ruta a un config.json")
    p.add_argument("--node-id")
    p.add_argument("--region", help="departamento (una de las 9 regionales)")
    p.add_argument("--sede", help="oficina concreta dentro del departamento "
                                  "(ej: El Alto). Por defecto, el departamento")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--puerto", type=int, default=config.SOCKET_PORT)
    p.add_argument("--intervalo", type=int, default=config.INTERVALO_DEFECTO_SEG)
    p.add_argument("--recursos",
                   help="que medir, separado por comas "
                        f"({', '.join(config.RECURSOS_DISPONIBLES)})")
    p.add_argument("--caos", type=int, default=0, metavar="PCT",
                   help="corta la conexion con esta probabilidad por ciclo; "
                        "sirve para demostrar el fallo intermitente")
    a = p.parse_args()

    if a.config:
        try:
            datos = json.loads(Path(a.config).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            p.error(f"No se pudo leer {a.config}: {e}")
        a.node_id = datos.get("node_id", a.node_id)
        a.region = datos.get("region", a.region)
        a.sede = datos.get("sede", a.sede)
        a.host = datos.get("host", a.host)
        a.puerto = datos.get("puerto", a.puerto)
        a.intervalo = datos.get("intervalo", a.intervalo)
        if not a.recursos and datos.get("recursos"):
            a.recursos = ",".join(datos["recursos"])

    if not a.node_id or not a.region:
        p.error("Faltan --node-id y --region (o un --config que los traiga)")

    recursos = ([x.strip().lower() for x in a.recursos.split(",")]
                if a.recursos else list(config.RECURSOS_DEFECTO))
    desconocidos = [r for r in recursos if r not in config.RECURSOS_DISPONIBLES]
    if desconocidos:
        p.error(f"Recursos desconocidos: {', '.join(desconocidos)}. "
                f"Validos: {', '.join(config.RECURSOS_DISPONIBLES)}")

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{a.node_id}] %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    sede = a.sede or config.sede_de(a.node_id, a.region)
    nodo = NodoCliente(a.node_id, a.region, a.host, a.puerto, a.intervalo,
                       recursos=recursos, caos=a.caos, sede=sede)
    lista = ["disco"] + [r for r in nodo.recursos if r != "disco"]
    log.info("Nodo %s  ·  departamento %s  ·  sede %s",
             a.node_id, a.region, sede)
    log.info("Mide: %s  ·  base local: %s  ·  la hora la pone el servidor",
             ", ".join(lista), nodo.almacen.ruta.name)
    try:
        nodo.ejecutar()
    except KeyboardInterrupt:
        log.info("Cliente detenido")
    finally:
        nodo.detener()
    raise SystemExit(1 if nodo.rechazado else 0)


if __name__ == "__main__":
    main()
