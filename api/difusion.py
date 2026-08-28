"""
Difusion por WebSocket — tarea 4.2 (v2).  Responsable: Robert.

EL PROBLEMA QUE RESUELVE
------------------------
El dashboard de la version 1 preguntaba. Cada navegador abierto pedia
/api/nodes, /api/cluster y un /api/history POR NODO, cada pocos segundos. Con
tres pantallas abiertas y nueve nodos eso son mas de treinta consultas a MySQL
cada cinco segundos, y aun asi el operador veia el dato hasta cinco segundos
tarde. Y si queria estar seguro, apretaba F5.

Ahora es al reves: la API mira la base UNA vez por segundo, para todos, y
EMPUJA el estado a los navegadores conectados. Consecuencias:

  * la carga sobre MySQL deja de depender de cuanta gente esta mirando;
  * el operador ve el cambio de estado de un nodo en menos de un segundo;
  * no hay que apretar F5 nunca.

SOLO SE MANDA LO QUE CAMBIO
---------------------------
Difundir el estado completo cada segundo son unos 8 KB por navegador por
segundo, casi siempre identicos. Por eso se calcula una huella de lo que se
leyo: si es la misma que la anterior, se manda un latido de 40 bytes en vez del
estado entero. El latido no es opcional — es lo que permite al navegador
distinguir "no cambio nada" de "se corto la conexion", que es exactamente el
problema que tenia el dashboard viejo cuando la API se caia y el seguia
mostrando numeros viejos como si nada.

SI EL WEBSOCKET NO ESTA, EL DASHBOARD SIGUE FUNCIONANDO
-------------------------------------------------------
Los endpoints REST no se tocaron. El dashboard usa el WebSocket cuando puede y
se cae al polling REST cuando no (proxy que no soporta upgrade, la API que se
reinicio, una red rara). Ver dashboard/index.html.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any

import anyio.to_thread
from fastapi import WebSocket
from fastapi.encoders import jsonable_encoder

from comun import config
from db import repositorio as repo
from db.conexion import cerrar_conexion_del_hilo

log = logging.getLogger("difusion")


class GestorWS:
    """
    Las conexiones WebSocket abiertas y la tarea de fondo que las alimenta.

    Se usa un asyncio.Lock y no un threading.Lock: todo esto vive en el bucle
    de eventos de la API, en un solo hilo. Mezclar un lock de hilos ahi seria
    bloquear el bucle entero.
    """

    def __init__(self) -> None:
        self.clientes: set[WebSocket] = set()
        self._candado = asyncio.Lock()
        self._tarea: asyncio.Task | None = None
        self._huella = ""
        self._hay_clientes = asyncio.Event()

    # -------------------------------------------------------- ciclo de vida
    def arrancar(self) -> None:
        if self._tarea is None or self._tarea.done():
            self._tarea = asyncio.create_task(self._bucle(), name="difusion-ws")

    async def detener(self) -> None:
        if self._tarea is not None:
            self._tarea.cancel()
            try:
                await self._tarea
            except (asyncio.CancelledError, Exception):           # noqa: BLE001
                pass
            self._tarea = None
        async with self._candado:
            clientes = list(self.clientes)
            self.clientes.clear()
        for ws in clientes:
            try:
                await ws.close()
            except Exception:                                     # noqa: BLE001
                pass

    # ------------------------------------------------------------- clientes
    async def conectar(self, ws: WebSocket) -> bool:
        """
        Acepta un navegador. Devuelve False si ya hay demasiados.

        El tope existe porque cada cliente cuesta memoria y una copia del
        payload en cada difusion. Sin el, una pestana que se reabre en bucle
        por un error de JavaScript puede dejar cientos de sockets abiertos.
        """
        async with self._candado:
            if len(self.clientes) >= config.WS_MAX_CLIENTES:
                return False
            self.clientes.add(ws)
            self._hay_clientes.set()
        return True

    async def desconectar(self, ws: WebSocket) -> None:
        async with self._candado:
            self.clientes.discard(ws)
            if not self.clientes:
                self._hay_clientes.clear()
                # Se olvida la huella: el proximo que entre tiene que recibir
                # el estado completo, no un latido de "no cambio nada".
                self._huella = ""

    async def difundir(self, texto: str) -> None:
        """
        Manda el mismo texto a todos. Se serializa UNA vez y se reparte: con
        cincuenta pestanas abiertas, serializar por cliente seria cincuenta
        veces el mismo trabajo.

        Un cliente que falla se descarta en el momento. Si no, un navegador que
        se cerro mal deja una excepcion por segundo, para siempre.
        """
        async with self._candado:
            clientes = list(self.clientes)
        caidos = []
        for ws in clientes:
            try:
                await ws.send_text(texto)
            except Exception:                                     # noqa: BLE001
                caidos.append(ws)
        if caidos:
            async with self._candado:
                for ws in caidos:
                    self.clientes.discard(ws)
                if not self.clientes:
                    self._hay_clientes.clear()
                    self._huella = ""

    # ---------------------------------------------------------------- bucle
    async def _bucle(self) -> None:
        log.info("Difusion WebSocket activa (cada %.1f s, solo con clientes "
                 "conectados)", config.PERIODO_WS_SEG)
        while True:
            try:
                # Sin nadie mirando NO se consulta la base. Es la diferencia
                # entre una API que descansa de noche y una que consulta MySQL
                # 86.400 veces por dia para nadie.
                await self._hay_clientes.wait()
                estado = await anyio.to_thread.run_sync(leer_estado)
                texto = json.dumps(estado, ensure_ascii=False, default=str)
                huella = hashlib.sha1(texto.encode("utf-8")).hexdigest()

                if huella != self._huella:
                    self._huella = huella
                    await self.difundir(texto)
                else:
                    await self.difundir(json.dumps(
                        {"tipo": "latido", "ts": estado.get("ts")}))
            except asyncio.CancelledError:
                raise
            except Exception as e:                                # noqa: BLE001
                # Un fallo de la base no puede matar la difusion: los
                # navegadores se quedarian con datos viejos sin enterarse.
                log.warning("Ciclo de difusion fallido: %s", e)
                await self.difundir(json.dumps(
                    {"tipo": "error", "detalle": "La API no pudo leer la base"}))
            await asyncio.sleep(config.PERIODO_WS_SEG)


def leer_estado() -> dict[str, Any]:
    """
    UNA lectura de la base con todo lo que el dashboard necesita.

    Corre en un hilo del pool (por eso cierra su conexion al terminar: ver
    db/conexion.py). Se lee todo junto a proposito — tres consultas por ciclo
    para todos los navegadores, en vez de tres por navegador.
    """
    try:
        nodos = repo.listar_nodos()
        cluster = repo.resumen_cluster()
        regionales = repo.listar_regionales()
        recursos = repo.recursos_actuales()
        # La distribucion sale de los nodos que ya se leyeron, sin consulta
        # extra: es un conteo en Python sobre diez filas.
        distribucion = repo.distribucion_uso(5, nodos)
        eventos = repo.listar_eventos(limite=15)
        return jsonable_encoder({
            "tipo": "estado",
            "ts": _ahora(),
            "cluster": cluster,
            "nodos": nodos,
            "regionales": regionales,
            # La ultima medicion de RAM, CPU, red y de cada unidad de disco.
            # Van dentro del mismo empujon a proposito: si el dashboard las
            # pidiera aparte volveria a hacer polling justo para lo nuevo.
            "recursos": recursos,
            "distribucion": distribucion,
            "eventos": eventos,
        })
    finally:
        cerrar_conexion_del_hilo()


def _ahora() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


gestor = GestorWS()
