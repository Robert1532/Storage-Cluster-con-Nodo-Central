"""
Base de datos LOCAL del nodo cliente — modulo M1.7 (v2).  Responsable: Martin.

    python -m cliente.almacen --node-id CNS-LPZ-01      # ver que tiene guardado

POR QUE UN CLIENTE NECESITA SU PROPIA BASE
------------------------------------------
En la version 1, si el nodo perdia la red, sus mediciones se perdian: el
cliente medía, no podia enviar, y tiraba el dato. El dashboard mostraba un
hueco, y ese hueco era permanente. Para un sistema que monitorea historiales
clinicos eso no sirve: justo cuando algo va mal es cuando mas falta hace saber
que estaba pasando en el disco.

Ahora cada nodo guarda TODA muestra en un SQLite propio (datos/cliente_<id>.db)
ANTES de intentar enviarla. Si hay red, se manda y se marca entregada. Si no la
hay, se queda ahi. Cuando la red vuelve, el cliente le manda al servidor todo lo
que se perdio, en orden, y recien ahi lo da por entregado.

Es el patron "store and forward", el mismo que usan los agentes de monitoreo de
verdad (Zabbix, Prometheus con su WAL, los agentes de Datadog).

POR QUE SQLite Y NO UN ARCHIVO
------------------------------
Porque un archivo de texto no sobrevive a un corte de luz a mitad de escritura,
no se puede consultar por rango, y borrar las primeras N lineas obliga a
reescribirlo entero. SQLite da transacciones, un indice sobre lo pendiente, y
poda con un DELETE. Ademas viene en la biblioteca estandar de Python: no agrega
ni una dependencia.

DOS COSAS QUE SE GUARDAN, NO UNA
--------------------------------
  muestras : el comportamiento del disco (y de la RAM, la CPU, la red) en el
             tiempo. Se conserva un tiempo aunque ya se haya entregado, porque
             es el historial local del nodo.
  bitacora : que le paso al nodo — mensajes recibidos del servidor, cortes,
             reconexiones, cambios de intervalo. Es la version consultable del
             archivo .log que pide el requisito 7.1 (el .log de texto se sigue
             escribiendo igual: el enunciado lo pide explicitamente).

CONCURRENCIA
------------
El hilo principal escribe muestras, el hilo receptor escribe en la bitacora.
Una conexion SQLite no es segura entre hilos por defecto, asi que se abre con
check_same_thread=False y TODA operacion pasa por un unico candado. Es un
archivo local: la contencion es de microsegundos.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from pathlib import Path
from typing import Any

from comun import config, protocolo

log = logging.getLogger("almacen")

ESQUEMA = """
-- seq es INTEGER PRIMARY KEY AUTOINCREMENT a proposito: con AUTOINCREMENT,
-- SQLite guarda la marca mas alta en sqlite_sequence y NUNCA reutiliza un
-- numero, aunque se borren filas al podar. Eso es justo lo que necesita el
-- servidor para descartar duplicados: un seq repetido con datos distintos
-- seria peor que perder la muestra.
CREATE TABLE IF NOT EXISTS muestras (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    mono_ns    INTEGER NOT NULL,
    t_local    TEXT    NOT NULL,
    entregada  INTEGER NOT NULL DEFAULT 0,
    datos      TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_pendientes ON muestras(entregada, seq);

CREATE TABLE IF NOT EXISTS bitacora (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    t_local TEXT NOT NULL,
    mono_ns INTEGER NOT NULL,
    tipo    TEXT NOT NULL,
    detalle TEXT
);
CREATE INDEX IF NOT EXISTS ix_bitacora ON bitacora(id DESC);

CREATE TABLE IF NOT EXISTS estado (
    clave TEXT PRIMARY KEY,
    valor TEXT
);
"""


class AlmacenLocal:
    """La base local de UN nodo. Una instancia por proceso cliente."""

    def __init__(self, node_id: str, ruta: Path | None = None,
                 max_muestras: int | None = None,
                 retencion_horas: int | None = None) -> None:
        config.asegurar_directorios()
        self.node_id = node_id
        self.ruta = ruta or (config.DIR_DATOS / f"cliente_{node_id}.db")
        self.max_muestras = max_muestras or config.BUFFER_MAX_MUESTRAS
        self.retencion_horas = retencion_horas or config.BUFFER_RETENCION_HORAS
        self._candado = threading.Lock()
        self._cnx = sqlite3.connect(str(self.ruta), check_same_thread=False)
        self._cnx.row_factory = sqlite3.Row
        self.modo_diario = self._elegir_diario()
        with self._candado:
            self._cnx.executescript(ESQUEMA)
            self._cnx.commit()

    def _elegir_diario(self) -> str:
        """
        WAL si se puede, y si no, el diario clasico.

        WAL es mejor (un lector no bloquea al escritor, y aguanta mejor un corte
        de luz), pero NECESITA memoria compartida entre procesos, y eso no
        existe en un sistema de archivos de red: si la carpeta del proyecto esta
        en un recurso compartido, una unidad de red o una carpeta sincronizada,
        activar WAL falla con "disk I/O error" y el cliente no arranca.

        Pasa de verdad, y pasaria justo en la demo si alguien clona el proyecto
        en una carpeta de red. Se intenta WAL, y si el sistema de archivos no lo
        soporta se cae a DELETE, que funciona en cualquier lado. Se pierde algo
        de concurrencia; no se pierde ni un dato.
        """
        for modo in ("WAL", "DELETE"):
            try:
                self._cnx.execute(f"PRAGMA journal_mode = {modo}").fetchone()
                self._cnx.execute("PRAGMA synchronous = NORMAL")
                # No basta con que el PRAGMA no lance: WAL puede aceptarse y
                # fallar recien en la primera escritura. Se prueba de verdad.
                self._cnx.execute("CREATE TABLE IF NOT EXISTS _prueba (x INTEGER)")
                self._cnx.execute("DROP TABLE IF EXISTS _prueba")
                self._cnx.commit()
                if modo == "DELETE":
                    log.warning(
                        "El sistema de archivos de %s no soporta WAL (suele "
                        "pasar en carpetas de red). Se usa el diario clasico.",
                        self.ruta.parent)
                return modo
            except sqlite3.Error:
                continue
        return "DESCONOCIDO"

    # ------------------------------------------------------------- muestras
    def guardar(self, disco: dict, recursos: list[dict] | None = None,
                mono: int | None = None, t_local: str | None = None) -> int:
        """
        Guarda una medicion y devuelve su `seq`.

        SE GUARDA SIEMPRE, haya red o no. Esa es la diferencia con la version 1
        y es lo que hace posible la sincronizacion: cuando el cliente descubre
        que no hay red, el dato YA esta a salvo en disco.
        """
        mono = protocolo.mono_ns() if mono is None else int(mono)
        t_local = t_local or protocolo.ahora_iso()
        cuerpo = json.dumps({"disco": disco, "recursos": recursos or []},
                            ensure_ascii=False)
        with self._candado:
            cur = self._cnx.execute(
                "INSERT INTO muestras (mono_ns, t_local, entregada, datos) "
                "VALUES (?, ?, 0, ?)", (mono, t_local, cuerpo))
            self._cnx.commit()
            return int(cur.lastrowid)

    def pendientes(self, limite: int | None = None) -> list[dict[str, Any]]:
        """
        Lo que el servidor todavia no confirmo, de mas viejo a mas nuevo.

        El ORDEN IMPORTA: el SYNC_OK del servidor es acumulativo ("acepte hasta
        el seq N"). Si se mandaran desordenadas, confirmar hasta N daria por
        entregadas muestras anteriores que nunca se enviaron.
        """
        limite = limite or config.SYNC_TAM_LOTE
        with self._candado:
            filas = self._cnx.execute(
                "SELECT seq, mono_ns, t_local, datos FROM muestras "
                "WHERE entregada = 0 ORDER BY seq ASC LIMIT ?", (limite,)
            ).fetchall()
        salida = []
        for f in filas:
            try:
                cuerpo = json.loads(f["datos"])
            except ValueError:
                # Una fila corrupta no puede bloquear la sincronizacion entera:
                # se marca entregada para que no vuelva a salir y se sigue.
                self.marcar_entregadas(int(f["seq"]))
                continue
            salida.append(protocolo.muestra(
                seq=int(f["seq"]),
                disco=cuerpo.get("disco") or {},
                recursos=cuerpo.get("recursos") or [],
                mono=int(f["mono_ns"]),
                marca=f["t_local"],
            ))
        return salida

    def contar_pendientes(self) -> int:
        with self._candado:
            fila = self._cnx.execute(
                "SELECT COUNT(*) AS n FROM muestras WHERE entregada = 0").fetchone()
            return int(fila["n"])

    def marcar_entregadas(self, hasta_seq: int) -> int:
        """
        El servidor confirmo hasta `hasta_seq`. Se marcan, NO se borran: la
        muestra sigue siendo el historial local del nodo hasta que la poda se
        la lleve por antiguedad.
        """
        with self._candado:
            cur = self._cnx.execute(
                "UPDATE muestras SET entregada = 1 "
                "WHERE entregada = 0 AND seq <= ?", (int(hasta_seq),))
            self._cnx.commit()
            return cur.rowcount

    def ultimo_seq(self) -> int:
        with self._candado:
            fila = self._cnx.execute(
                "SELECT COALESCE(MAX(seq), 0) AS s FROM muestras").fetchone()
            return int(fila["s"])

    # ------------------------------------------------------------- bitacora
    def anotar(self, tipo: str, detalle: str | None = None) -> None:
        """Requisito 7.1, en version consultable. El .log de texto se escribe
        igual desde cliente/main.py; esto permite ademas responder "que paso
        entre las 14:00 y las 15:00" sin abrir el archivo a mano."""
        try:
            with self._candado:
                self._cnx.execute(
                    "INSERT INTO bitacora (t_local, mono_ns, tipo, detalle) "
                    "VALUES (?, ?, ?, ?)",
                    (protocolo.ahora_iso(), protocolo.mono_ns(),
                     str(tipo)[:64], None if detalle is None else str(detalle)[:1000]))
                self._cnx.commit()
        except sqlite3.Error as e:
            # Si el disco esta lleno o la base esta bloqueada, se avisa y se
            # sigue. Ironico en un monitor de discos: sin esta guarda, el
            # cliente dejaria de funcionar justo cuando el disco se llena.
            log.error("No se pudo anotar en la bitacora local: %s", e)

    def ultimas_anotaciones(self, limite: int = 50) -> list[dict[str, Any]]:
        with self._candado:
            filas = self._cnx.execute(
                "SELECT t_local, tipo, detalle FROM bitacora "
                "ORDER BY id DESC LIMIT ?", (int(limite),)).fetchall()
        return [dict(f) for f in filas]

    # ---------------------------------------------------------------- estado
    def leer_estado(self, clave: str, defecto: str = "") -> str:
        with self._candado:
            fila = self._cnx.execute(
                "SELECT valor FROM estado WHERE clave = ?", (clave,)).fetchone()
            return fila["valor"] if fila else defecto

    def guardar_estado(self, clave: str, valor: str) -> None:
        with self._candado:
            self._cnx.execute(
                "INSERT INTO estado (clave, valor) VALUES (?, ?) "
                "ON CONFLICT(clave) DO UPDATE SET valor = excluded.valor",
                (clave, str(valor)))
            self._cnx.commit()

    # ------------------------------------------------------------------ poda
    def podar(self) -> int:
        """
        Evita que la base local crezca sin techo si el nodo pasa dias sin
        servidor. Se borra en este orden:

          1. lo ENTREGADO que sea mas viejo que la retencion configurada
          2. si aun asi hay mas muestras que el maximo, lo mas viejo entregado
          3. si TODAVIA hay exceso, lo mas viejo aunque NO este entregado

        El paso 3 duele y es a proposito: si el nodo lleva una semana sin red,
        en algun momento hay que elegir entre perder lo mas viejo o llenarle el
        disco al hospital. Se pierde lo mas viejo, y queda anotado en la
        bitacora para que nadie descubra el hueco por casualidad.
        """
        limite_seg = self.retencion_horas * 3600
        corte_mono = protocolo.mono_ns() - limite_seg * 1_000_000_000
        borradas = 0
        with self._candado:
            cur = self._cnx.execute(
                "DELETE FROM muestras WHERE entregada = 1 AND mono_ns < ?",
                (corte_mono,))
            borradas += cur.rowcount

            total = int(self._cnx.execute(
                "SELECT COUNT(*) AS n FROM muestras").fetchone()["n"])
            sobran = total - self.max_muestras
            if sobran > 0:
                cur = self._cnx.execute(
                    "DELETE FROM muestras WHERE seq IN ("
                    "  SELECT seq FROM muestras WHERE entregada = 1 "
                    "  ORDER BY seq ASC LIMIT ?)", (sobran,))
                borradas += cur.rowcount
                sobran -= cur.rowcount

            perdidas = 0
            if sobran > 0:
                cur = self._cnx.execute(
                    "DELETE FROM muestras WHERE seq IN ("
                    "  SELECT seq FROM muestras ORDER BY seq ASC LIMIT ?)",
                    (sobran,))
                perdidas = cur.rowcount
                borradas += perdidas

            self._cnx.execute(
                "DELETE FROM bitacora WHERE id IN ("
                "  SELECT id FROM bitacora ORDER BY id DESC LIMIT -1 OFFSET 5000)")
            self._cnx.commit()

        if perdidas:
            log.warning("Buffer lleno: se descartaron %d muestras SIN ENVIAR "
                        "(las mas viejas)", perdidas)
            self.anotar("BUFFER_LLENO",
                        f"{perdidas} muestras sin enviar descartadas por limite "
                        f"({self.max_muestras})")
        return borradas

    # -------------------------------------------------------------- resumen
    def resumen(self) -> dict[str, Any]:
        with self._candado:
            f = self._cnx.execute(
                "SELECT COUNT(*) AS total, "
                "       SUM(entregada = 0) AS pendientes, "
                "       MIN(t_local) AS desde, MAX(t_local) AS hasta "
                "  FROM muestras").fetchone()
        try:
            tam = self.ruta.stat().st_size
        except OSError:
            tam = 0
        return {
            "node_id": self.node_id,
            "archivo": str(self.ruta),
            "tamano_kb": round(tam / 1024, 1),
            "muestras": int(f["total"] or 0),
            "pendientes": int(f["pendientes"] or 0),
            "desde": f["desde"],
            "hasta": f["hasta"],
        }

    def cerrar(self) -> None:
        with self._candado:
            try:
                self._cnx.commit()
                self._cnx.close()
            except sqlite3.Error:
                pass


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="Inspeccionar la base local de un nodo")
    p.add_argument("--node-id", required=True)
    p.add_argument("--bitacora", type=int, default=10,
                   help="cuantas anotaciones mostrar")
    a = p.parse_args()

    al = AlmacenLocal(a.node_id)
    r = al.resumen()
    print(f"Base local de {r['node_id']}")
    print(f"  archivo     : {r['archivo']} ({r['tamano_kb']} KB)")
    print(f"  muestras    : {r['muestras']}  (pendientes de enviar: {r['pendientes']})")
    print(f"  desde/hasta : {r['desde']}  ->  {r['hasta']}")
    print(f"\nUltimas {a.bitacora} anotaciones:")
    for b in al.ultimas_anotaciones(a.bitacora):
        print(f"  [{b['t_local']}] {b['tipo']}: {b['detalle'] or ''}")
    al.cerrar()


if __name__ == "__main__":
    main()
