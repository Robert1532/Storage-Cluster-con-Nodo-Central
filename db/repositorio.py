"""
Capa de acceso a datos — tareas 3.1, 3.2 y 3.3.  Responsable: Robert.

Todo el SQL del proyecto vive aqui. El servidor de sockets y la API llaman a
estas funciones; ninguno escribe SQL por su cuenta. Asi, cuando cambie una
columna, se toca un solo archivo.

DOS REGLAS QUE SE APLICAN EN TODO EL ARCHIVO
--------------------------------------------
1. Lo que viene de la red se sanea antes de tocar la base. MySQL 8 corre en
   modo estricto: un ENUM con un valor inesperado, un numero fuera de rango o
   un texto mas largo que la columna no son avisos, son ERRORES que matarian el
   hilo del cliente. El cliente esta al otro lado de un socket: no confiamos.

2. Lo que tenga que ser atomico se resuelve en UNA sentencia. Con autocommit
   activo, un SELECT seguido de un INSERT son dos transacciones distintas y dos
   hilos pueden colarse en el medio.

Funciones agrupadas por quien las usa:
    SERVIDOR : registrar_nodo, guardar_metrica, registrar_evento,
               marcar_nodos_caidos, marcar_nodos_recuperados,
               mensajes_pendientes, marcar_enviado, marcar_fallido,
               confirmar_ack, actualizar_intervalo
    API      : listar_nodos, resumen_cluster, historial, crecimiento,
               disponibilidad, listar_eventos, existe_nodo, crear_mensaje,
               listar_mensajes, obtener_mensaje
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from comun import config, protocolo
from db.conexion import cursor

# Limites que impone el esquema (db/schema.sql). Si cambian una columna,
# cambien tambien esto.
_MAX_TEXTO = 255
_MAX_NODE_ID = 32
_MAX_REGION = 64
_MAX_HOSTNAME = 128
_MAX_SO = 64
_MAX_IP = 45
_MAX_DISCO = 64
_MAX_GB = 99_999_999.99        # DECIMAL(10,2)
_MAX_PCT = 999.99              # DECIMAL(5,2)
_MAX_LATENCIA = 99_999.999     # DECIMAL(8,3)
_MAX_IOPS = 4_294_967_295      # INT UNSIGNED


# ------------------------------------------------------------------ saneadores

def _texto(valor: Any, largo: int, defecto: str | None = None) -> str | None:
    if valor is None:
        return defecto
    texto = str(valor).strip()
    return texto[:largo] if texto else defecto


def _decimal(valor: Any, maximo: float, defecto: float = 0.0) -> float:
    """Numero acotado a [0, maximo]. Un NaN o un texto caen al defecto."""
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return defecto
    if numero != numero:                       # NaN
        return defecto
    return max(0.0, min(maximo, numero))


def _entero(valor: Any, maximo: int, defecto: int = 0) -> int:
    """
    Entero acotado a [0, maximo]. Los IOPS se calculan como diferencia entre dos
    lecturas de contadores; si el disco o el sistema se reinician, esa
    diferencia sale negativa. Aqui se corta.
    """
    try:
        numero = int(valor)
    except (TypeError, ValueError):
        return defecto
    return max(0, min(maximo, numero))


def _a_datetime(iso: str) -> datetime:
    """
    El cliente manda ISO 8601 CON offset de zona; la base trabaja en UTC.

    POR QUE ESTO IMPORTA (bug real que ya nos mordio):
    la conexion fija time_zone='+00:00', asi que NOW() en MySQL devuelve UTC.
    Bolivia es UTC-4. Si se guarda la hora local tal cual, toda metrica queda
    cuatro horas "en el pasado" para la base y cualquier consulta con
    "WHERE timestamp >= NOW() - INTERVAL 24 HOUR" no devuelve NADA aunque la
    tabla este llena.

    Por eso protocolo.ahora_iso() incluye el offset: asi la conversion no
    depende de en que zona corra el servidor. Un timestamp SIN offset (cliente
    viejo) se interpreta como UTC, que es la suposicion menos danina: adelanta
    el dato como mucho unas horas, en vez de esconderlo del filtro de tiempo.
    """
    if isinstance(iso, datetime):
        # v2: el servidor ya calculo la hora real a partir del reloj monotonico
        # del cliente. Si viene con zona, se pasa a UTC; si no, ya es UTC.
        return (iso.astimezone(timezone.utc).replace(tzinfo=None)
                if iso.tzinfo else iso)
    try:
        dt = datetime.fromisoformat(iso)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc).replace(tzinfo=None)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


# ============================================================== NODOS / ALTA

def registrar_nodo(node_id: str, region: str, hostname: str | None,
                   so: str | None, ip: str | None,
                   intervalo: int,
                   sede: str | None = None,
                   agente: str | None = None,
                   capacidades: list[str] | None = None,
                   desvio_reloj: float | None = None,
                   pendientes: int = 0) -> tuple[bool, int, list[str]]:
    """
    Alta automatica de cliente — requisito 7.2 (vale 10%).

    Si el node_id no existe, lo inserta y deja el evento ALTA_AUTOMATICA.
    Si ya existia, actualiza sus datos y deja el evento CONEXION.

    Devuelve (es_nuevo, intervalo_vigente, recursos_que_debe_reportar).

    v2: se guardan tambien la version del agente, que sabe medir ese nodo
    (`capacidades`), cuanto miente su reloj y cuantas muestras trae pendientes
    de sincronizar. `recursos_pedidos` es lo que el SERVIDOR quiere que mande:
    en el alta se siembra con lo que el nodo dijo saber medir, y despues el
    operador lo cambia desde el dashboard sin tocar esa maquina.

    ATOMICIDAD: es un solo INSERT ... ON DUPLICATE KEY UPDATE, no un SELECT
    seguido de un INSERT. Con dos hilos (un cliente que reconecta antes de que
    se cierre su sesion anterior, o dos clientes mal configurados con el mismo
    node_id) los dos verian "no existe" y el segundo INSERT reventaria con un
    error 1062 que mata el hilo del servidor.

    rowcount vale 1 si inserto y 2 si actualizo: asi sabemos si es nuevo sin
    consultar antes.

    El intervalo que devuelve es el de la BASE, no el que mando el cliente: si
    un operador lo cambio desde el dashboard, el cliente lo adopta al
    reconectar. Esa es la mitad "desde el servidor" del requisito 7.3.

    NO toca `estado` al reconectar, solo `ultimo_reporte`. Es a proposito: el
    unico que cambia el estado es el watchdog. Si aqui pusieramos ACTIVO
    directamente, un nodo que estaba NO_REPORTA volveria a ACTIVO sin dejar el
    evento RECUPERADO, y la bitacora tendria caidas sin recuperaciones. Como el
    watchdog corre cada pocos segundos y mira ultimo_reporte, la recuperacion
    se registra igual de rapido y queda documentada.
    """
    node_id = _texto(node_id, _MAX_NODE_ID) or "SIN-ID"
    region = _texto(region, _MAX_REGION) or "Desconocida"
    sede = _texto(sede, _MAX_REGION) or region
    hostname = _texto(hostname, _MAX_HOSTNAME)
    so = _texto(so, _MAX_SO)
    ip = _texto(ip, _MAX_IP)
    intervalo = config.acotar_intervalo(intervalo)
    agente = _texto(agente, 32)
    caps = _texto(",".join(str(c) for c in (capacidades or [])), _MAX_TEXTO)
    desvio = None if desvio_reloj is None else round(float(desvio_reloj), 3)

    with cursor() as cur:
        cur.execute(
            """INSERT INTO nodos
                   (node_id, region, sede, hostname, sistema_operativo, ip,
                    estado, intervalo_seg, ultimo_reporte,
                    agente_version, capacidades, recursos_pedidos,
                    ultima_reconexion, pendientes_sync, desvio_reloj_seg)
               VALUES (%s, %s, %s, %s, %s, %s, 'ACTIVO', %s, NOW(3),
                       %s, %s, %s, NOW(3), %s, %s)
               ON DUPLICATE KEY UPDATE
                   region            = VALUES(region),
                   sede              = VALUES(sede),
                   hostname          = VALUES(hostname),
                   sistema_operativo = VALUES(sistema_operativo),
                   ip                = VALUES(ip),
                   ultimo_reporte    = NOW(3),
                   ultima_reconexion = NOW(3),
                   agente_version    = VALUES(agente_version),
                   capacidades       = VALUES(capacidades),
                   pendientes_sync   = VALUES(pendientes_sync),
                   desvio_reloj_seg  = VALUES(desvio_reloj_seg)""",
            (node_id, region, sede, hostname, so, ip, intervalo,
             agente, caps, caps, int(pendientes), desvio),
        )
        es_nuevo = cur.rowcount == 1

        cur.execute(
            "INSERT INTO eventos (node_id, tipo, detalle) VALUES (%s, %s, %s)",
            (node_id,
             "ALTA_AUTOMATICA" if es_nuevo else "CONEXION",
             (f"Alta automatica de {region} desde {ip}" if es_nuevo
              else f"Reconexion desde {ip}")),
        )

        cur.execute("SELECT intervalo_seg, recursos_pedidos FROM nodos "
                    "WHERE node_id = %s", (node_id,))
        fila = cur.fetchone()
        vigente = int(fila["intervalo_seg"]) if fila else intervalo
        pedidos = [r for r in (fila["recursos_pedidos"] or "").split(",")
                   if r] if fila else []

    return es_nuevo, vigente, (pedidos or list(config.RECURSOS_DEFECTO))


def existe_nodo(node_id: str) -> bool:
    with cursor() as cur:
        cur.execute("SELECT 1 AS x FROM nodos WHERE node_id = %s", (node_id,))
        return cur.fetchone() is not None


def registrar_evento(node_id: str, tipo: str, detalle: str | None = None) -> None:
    with cursor() as cur:
        cur.execute(
            "INSERT INTO eventos (node_id, tipo, detalle) VALUES (%s, %s, %s)",
            (_texto(node_id, _MAX_NODE_ID), tipo, _texto(detalle, _MAX_TEXTO)),
        )


def actualizar_intervalo(node_id: str, segundos: int) -> None:
    """Persistir el intervalo nuevo — requisito 7.3."""
    segundos = config.acotar_intervalo(segundos)
    with cursor() as cur:
        cur.execute("UPDATE nodos SET intervalo_seg = %s WHERE node_id = %s",
                    (segundos, node_id))
        cur.execute(
            "INSERT INTO eventos (node_id, tipo, detalle) VALUES (%s, 'CAMBIO_INTERVALO', %s)",
            (node_id, f"Intervalo cambiado a {segundos} s"),
        )


# ================================================================== METRICAS

def guardar_metrica(node_id: str, timestamp, disco: dict,
                    seq: int = 0, origen: str = "VIVO",
                    t_cliente=None, tocar_reporte: bool = True) -> None:
    """
    Inserta UNA fila nueva. Nunca un UPDATE: la tabla es un historico.
    Ademas refresca nodos.ultimo_reporte, que es lo que mira el watchdog.

    NO toca `estado`. Las dos transiciones (ACTIVO -> NO_REPORTA y la vuelta)
    las decide el watchdog, en un unico lugar: asi cada cambio de estado deja
    su evento en la bitacora y no hay dos sitios peleandose por la misma
    columna.

    Todos los valores del disco se acotan a lo que aguanta el esquema antes de
    insertarlos.
    """
    tipo = _texto(disco.get("tipo"), 16, protocolo.TIPO_DESCONOCIDO)
    if tipo not in protocolo.TIPOS_DISCO:
        tipo = protocolo.TIPO_DESCONOCIDO

    with cursor() as cur:
        cur.execute(
            """INSERT INTO metricas
                   (node_id, timestamp, disco_nombre, disco_tipo,
                    total_gb, usado_gb, libre_gb, uso_pct,
                    iops_lectura, iops_escritura, latencia_ms,
                    seq, origen, t_cliente)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                _texto(node_id, _MAX_NODE_ID),
                _a_datetime(timestamp),
                _texto(disco.get("nombre"), _MAX_DISCO),
                tipo,
                _decimal(disco.get("total_gb"), _MAX_GB),
                _decimal(disco.get("usado_gb"), _MAX_GB),
                _decimal(disco.get("libre_gb"), _MAX_GB),
                _decimal(disco.get("uso_pct"), _MAX_PCT),
                _entero(disco.get("iops_lectura"), _MAX_IOPS),
                _entero(disco.get("iops_escritura"), _MAX_IOPS),
                _decimal(disco.get("latencia_ms"), _MAX_LATENCIA),
                max(0, int(seq or 0)),
                origen if origen in ("VIVO", "SYNC") else "VIVO",
                _a_datetime(t_cliente) if t_cliente else None,
            ),
        )
        if tocar_reporte:
            cur.execute(
                "UPDATE nodos SET ultimo_reporte = NOW(3) WHERE node_id = %s",
                (node_id,))


# ================================================ RECURSOS FLEXIBLES  (v2)
#
# Todo lo que un nodo sabe medir y no es el primer disco: la RAM, la CPU, las
# interfaces de red y los discos ADICIONALES (el pendrive que alguien enchufa
# en la laptop de Santa Cruz). La gracia es que agregar una medida nueva no
# obliga a tocar ni esta capa ni el esquema: el diccionario `metricas` entra
# entero en una columna JSON, y MySQL materializa solo las tres medidas que se
# consultan siempre (ver el comentario de la tabla en db/schema.sql).

def guardar_recursos(node_id: str, timestamp, recursos: list[dict],
                     seq: int = 0, origen: str = "VIVO") -> int:
    """
    Inserta de una vez todos los recursos de UNA muestra. Devuelve cuantos.

    executemany y no un execute por recurso: un nodo que reporta disco + 2
    particiones + RAM + CPU + red son seis viajes de ida y vuelta a MySQL cada
    diez segundos, por nueve nodos. Contra Aiven eso solo ya es medio segundo
    de red por ciclo.
    """
    if not recursos:
        return 0
    momento = _a_datetime(timestamp)
    origen = origen if origen in ("VIVO", "SYNC") else "VIVO"
    filas = []
    for r in recursos:
        tipo = _texto(r.get("tipo"), 16, protocolo.REC_CUSTOM)
        if tipo not in protocolo.TIPOS_RECURSO:
            tipo = protocolo.REC_CUSTOM
        nombre = _texto(r.get("nombre"), _MAX_DISCO)
        if not nombre:
            continue
        metricas = r.get("metricas") or {}
        etiquetas = r.get("etiquetas") or {}
        if not isinstance(metricas, dict) or not isinstance(etiquetas, dict):
            continue
        filas.append((
            _texto(node_id, _MAX_NODE_ID), momento, tipo, nombre,
            json.dumps(metricas, ensure_ascii=False),
            json.dumps(etiquetas, ensure_ascii=False) if etiquetas else None,
            origen, max(0, int(seq or 0)),
        ))
    if not filas:
        return 0
    with cursor() as cur:
        cur.executemany(
            """INSERT INTO recursos
                   (node_id, timestamp, tipo, nombre, metricas, etiquetas,
                    origen, seq)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            filas,
        )
    return len(filas)


def ultima_seq(node_id: str) -> int:
    """Ultimo numero de muestra que el servidor acepto de este nodo."""
    with cursor() as cur:
        cur.execute("SELECT ultima_seq FROM nodos WHERE node_id = %s", (node_id,))
        fila = cur.fetchone()
        return int(fila["ultima_seq"]) if fila else 0


def guardar_lote(node_id: str, muestras: list[dict]) -> tuple[int, int]:
    """
    SINCRONIZACION TRAS UNA CAIDA — el corazon del requisito nuevo.

    `muestras` ya viene FECHADA POR EL SERVIDOR: cada elemento trae
    {seq, timestamp (datetime), t_cliente, disco, recursos}. Aqui no se
    interpreta ninguna hora del cliente.

    Devuelve (insertadas, descartadas_por_duplicadas).

    IDEMPOTENCIA: se descarta todo lo que tenga seq <= nodos.ultima_seq. Si el
    cliente no recibio el SYNC_OK y reenvia el mismo lote, la segunda vez no
    entra ninguna fila. Sin esto, una reconexion con mala suerte duplica horas
    de historico y el growth rate sale al doble.

    El avance de ultima_seq va con la condicion en el WHERE, no leyendo antes:
    dos hilos del mismo nodo (el viejo que aun no murio y el nuevo) no pueden
    hacerlo retroceder.
    """
    if not muestras:
        return 0, 0

    tope = ultima_seq(node_id)
    nuevas = [m for m in muestras if int(m.get("seq") or 0) > tope]
    descartadas = len(muestras) - len(nuevas)
    if not nuevas:
        return 0, descartadas

    filas_metricas = []
    for m in nuevas:
        d = m.get("disco") or {}
        tipo = _texto(d.get("tipo"), 16, protocolo.TIPO_DESCONOCIDO)
        if tipo not in protocolo.TIPOS_DISCO:
            tipo = protocolo.TIPO_DESCONOCIDO
        filas_metricas.append((
            _texto(node_id, _MAX_NODE_ID),
            _a_datetime(m.get("timestamp")),
            _texto(d.get("nombre"), _MAX_DISCO),
            tipo,
            _decimal(d.get("total_gb"), _MAX_GB),
            _decimal(d.get("usado_gb"), _MAX_GB),
            _decimal(d.get("libre_gb"), _MAX_GB),
            _decimal(d.get("uso_pct"), _MAX_PCT),
            _entero(d.get("iops_lectura"), _MAX_IOPS),
            _entero(d.get("iops_escritura"), _MAX_IOPS),
            _decimal(d.get("latencia_ms"), _MAX_LATENCIA),
            max(0, int(m.get("seq") or 0)),
            "SYNC",
            _a_datetime(m["t_cliente"]) if m.get("t_cliente") else None,
        ))

    with cursor() as cur:
        cur.executemany(
            """INSERT INTO metricas
                   (node_id, timestamp, disco_nombre, disco_tipo,
                    total_gb, usado_gb, libre_gb, uso_pct,
                    iops_lectura, iops_escritura, latencia_ms,
                    seq, origen, t_cliente)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            filas_metricas,
        )

    for m in nuevas:
        guardar_recursos(node_id, m.get("timestamp"), m.get("recursos") or [],
                         seq=int(m.get("seq") or 0), origen="SYNC")

    mayor = max(int(m.get("seq") or 0) for m in nuevas)
    with cursor() as cur:
        cur.execute(
            """UPDATE nodos
                  SET ultima_seq     = %s,
                      ultimo_reporte = NOW(3)
                WHERE node_id = %s AND ultima_seq < %s""",
            (mayor, node_id, mayor),
        )
    return len(nuevas), descartadas


def avanzar_seq(node_id: str, seq: int) -> None:
    """Avanza el contador tras una metrica EN VIVO (no un lote)."""
    seq = max(0, int(seq or 0))
    if seq <= 0:
        return
    with cursor() as cur:
        cur.execute(
            "UPDATE nodos SET ultima_seq = %s WHERE node_id = %s AND ultima_seq < %s",
            (seq, node_id, seq),
        )


def recursos_actuales(node_id: str | None = None) -> list[dict[str, Any]]:
    """Ultima medicion de cada recurso. Alimenta el panel de RAM/CPU/discos."""
    sql = """SELECT node_id, tipo, nombre, timestamp, metricas, etiquetas,
                    origen, total_gb, usado_gb, uso_pct
               FROM v_recursos_ultimo"""
    params: tuple = ()
    if node_id:
        sql += " WHERE node_id = %s"
        params = (node_id,)
    sql += " ORDER BY node_id, tipo, nombre"
    with cursor() as cur:
        cur.execute(sql, params)
        filas = cur.fetchall()
    # El driver devuelve las columnas JSON ya como dict cuando puede, y como
    # texto cuando la version del conector no lo hace. Se normaliza aqui para
    # que la API no tenga que preguntarse cual de las dos le toco.
    for f in filas:
        for campo in ("metricas", "etiquetas"):
            valor = f.get(campo)
            if isinstance(valor, (str, bytes, bytearray)):
                try:
                    f[campo] = json.loads(valor)
                except (ValueError, TypeError):
                    f[campo] = {}
            elif valor is None:
                f[campo] = {}
    return filas


def historial_recurso(node_id: str, tipo: str, nombre: str,
                      horas: int = 24, limite: int = 500) -> list[dict[str, Any]]:
    """Serie temporal de UN recurso concreto (la RAM de un nodo, por ejemplo).

    Mismo cuidado con el orden que en historial(): se toman los `limite` mas
    NUEVOS y despues se reordenan para dibujar."""
    with cursor() as cur:
        cur.execute(
            """SELECT * FROM (
                   SELECT timestamp, total_gb, usado_gb, uso_pct, origen
                     FROM recursos
                    WHERE node_id = %s AND tipo = %s AND nombre = %s
                      AND timestamp >= NOW() - INTERVAL %s HOUR
                    ORDER BY timestamp DESC, id DESC
                    LIMIT %s
               ) AS ultimos ORDER BY ultimos.timestamp ASC""",
            (node_id, tipo, nombre, horas, limite),
        )
        return cur.fetchall()


def actualizar_recursos_pedidos(node_id: str, recursos: list[str]) -> None:
    """Requisito de flexibilidad: el operador decide QUE mide cada nodo, desde
    el dashboard, sin entrar a esa maquina."""
    texto = _texto(",".join(str(r).strip().lower() for r in recursos), _MAX_TEXTO)
    with cursor() as cur:
        cur.execute("UPDATE nodos SET recursos_pedidos = %s WHERE node_id = %s",
                    (texto, node_id))
        cur.execute(
            "INSERT INTO eventos (node_id, tipo, detalle) "
            "VALUES (%s, 'CAMBIO_RECURSOS', %s)",
            (node_id, f"Recursos a reportar: {texto}"),
        )


def actualizar_pendientes(node_id: str, cantidad: int) -> None:
    """Cuantas muestras le quedan al nodo sin sincronizar. El dashboard lo
    muestra como 'recuperando N muestras'."""
    with cursor() as cur:
        cur.execute("UPDATE nodos SET pendientes_sync = %s WHERE node_id = %s",
                    (max(0, int(cantidad)), node_id))


def guardar_desvio_reloj(node_id: str, desvio_seg: float) -> None:
    """
    Deja constancia de que ese nodo tiene la hora cambiada.

    NO corrige nada ni rechaza el dato: la metrica ya se guardo con la hora del
    servidor. Esto es para el operador, que necesita saber que esa maquina
    tiene el reloj mal antes de que alguien mire un log suyo y se confunda.
    """
    with cursor() as cur:
        cur.execute("UPDATE nodos SET desvio_reloj_seg = %s WHERE node_id = %s",
                    (round(float(desvio_seg), 3), node_id))
        cur.execute(
            "INSERT INTO eventos (node_id, tipo, detalle) "
            "VALUES (%s, 'RELOJ_DESVIADO', %s)",
            (node_id, f"El reloj del nodo difiere {desvio_seg:+.1f} s del servidor; "
                      f"se usa la hora del servidor"),
        )


# ====================================== DESCONEXION E INTERMITENCIA  (v2)

def registrar_desconexion(node_id: str, motivo: str) -> None:
    """
    "Se desconecto de la red el ..." — con fecha y con motivo.

    Antes esto solo existia como una fila mas en `eventos` y el dashboard no
    tenia de donde sacarlo sin recorrer la bitacora. Ahora queda tambien en la
    fila del nodo, que es una lectura por clave primaria.

    caidas_recientes se incrementa aqui y lo reinicia el watchdog cuando pasa
    la ventana: es lo que distingue "se cayo" de "se esta cayendo todo el rato".
    """
    motivo = _texto(motivo, _MAX_TEXTO) or "Conexion cerrada"
    with cursor() as cur:
        cur.execute(
            """UPDATE nodos
                  SET ultima_desconexion = NOW(3),
                      motivo_desconexion = %s,
                      caidas_recientes   = LEAST(caidas_recientes + 1, 60000)
                WHERE node_id = %s""",
            (motivo, node_id),
        )
        cur.execute(
            "INSERT INTO eventos (node_id, tipo, detalle) VALUES (%s, 'DESCONEXION', %s)",
            (node_id, motivo),
        )


def marcar_intermitentes(ventana_min: int, umbral: int) -> list[str]:
    """
    Un nodo que se cae y vuelve N veces en la ventana esta fallando de forma
    INTERMITENTE, que es un problema distinto a estar caido.

    Se cuenta sobre `eventos` y no sobre el contador de la fila porque la
    ventana tiene que deslizarse: si no, un nodo que tuvo tres cortes ayer
    quedaria marcado para siempre.

    Devuelve los que ACABAN de cambiar de estado, para poder loguearlos una
    sola vez en vez de en cada ciclo del watchdog.
    """
    ventana_min = max(1, int(ventana_min))
    umbral = max(2, int(umbral))
    nuevos: list[str] = []
    with cursor() as cur:
        cur.execute(
            """SELECT n.node_id, n.intermitente,
                      (SELECT COUNT(*) FROM eventos e
                        WHERE e.node_id = n.node_id
                          AND e.tipo IN ('DESCONEXION','NO_REPORTA')
                          AND e.timestamp >= NOW() - INTERVAL %s MINUTE) AS caidas
                 FROM nodos n""",
            (ventana_min,),
        )
        filas = cur.fetchall()

        for f in filas:
            debe = 1 if int(f["caidas"]) >= umbral else 0
            if debe == int(f["intermitente"] or 0):
                cur.execute(
                    "UPDATE nodos SET caidas_recientes = %s WHERE node_id = %s",
                    (int(f["caidas"]), f["node_id"]))
                continue
            cur.execute(
                "UPDATE nodos SET intermitente = %s, caidas_recientes = %s "
                "WHERE node_id = %s",
                (debe, int(f["caidas"]), f["node_id"]))
            if debe:
                cur.execute(
                    "INSERT INTO eventos (node_id, tipo, detalle) "
                    "VALUES (%s, 'INTERMITENTE', %s)",
                    (f["node_id"],
                     f"{f['caidas']} cortes en los ultimos {ventana_min} min"))
                nuevos.append(f["node_id"])
    return nuevos


def listar_regionales() -> list[dict[str, Any]]:
    """
    Consolidado POR REGIONAL, no por maquina.

    Existe porque el enunciado habla de nueve administraciones regionales, y La
    Paz tiene DOS servidores. "Cuanto almacenamiento tiene La Paz" es la suma
    de sus dos nodos, no la de uno.
    """
    with cursor() as cur:
        cur.execute("SELECT * FROM v_regionales ORDER BY region")
        return cur.fetchall()


def ultima_metrica_de(node_id: str) -> dict[str, Any] | None:
    """La ultima medicion del primer disco de un nodo. La usa el servidor para
    detectar que la capacidad cambio (un disco que crecio, un pendrive)."""
    with cursor() as cur:
        cur.execute("SELECT * FROM v_ultima_metrica WHERE node_id = %s", (node_id,))
        return cur.fetchone()


def discos_conocidos(node_id: str) -> dict[str, float]:
    """
    Que unidades se le vieron por ultima vez a este nodo, con su capacidad.

    Es contra esto que el servidor compara cada muestra para detectar que
    enchufaron un pendrive (DISCO_AGREGADO), que lo sacaron (DISCO_REMOVIDO) o
    que una unidad cambio de tamano (CAPACIDAD_CAMBIADA).
    """
    with cursor() as cur:
        cur.execute(
            """SELECT nombre, total_gb FROM v_recursos_ultimo
                WHERE node_id = %s AND tipo = 'DISCO'""",
            (node_id,))
        return {f["nombre"]: float(f["total_gb"] or 0) for f in cur.fetchall()}


# ================================================================== WATCHDOG

def marcar_nodos_caidos(factor_timeout: int) -> list[str]:
    """
    Watchdog — tarea 2.3. Marca NO_REPORTA a todo nodo cuyo ultimo reporte sea
    mas viejo que factor_timeout x su propio intervalo.

    El umbral es POR NODO, no global: si un nodo reporta cada 30 s y otro cada
    5 s, no pueden compartir el mismo timeout. Ese detalle suele preguntarse.

    ATOMICIDAD: el UPDATE repite la condicion en su WHERE. Sin eso, entre el
    SELECT que arma la lista y el UPDATE puede llegar el reporte del nodo, y lo
    marcariamos caido justo cuando acaba de revivir — dejando ademas un evento
    NO_REPORTA falso que infla los failover_events que se muestran en la
    defensa. rowcount==1 confirma que el cambio ocurrio de verdad.

    Devuelve la lista de node_id que acaban de cambiar de estado.
    """
    caidos: list[str] = []
    with cursor() as cur:
        cur.execute(
            """SELECT node_id FROM nodos
                WHERE estado = 'ACTIVO'
                  AND ultimo_reporte IS NOT NULL
                  AND TIMESTAMPDIFF(SECOND, ultimo_reporte, NOW())
                      > intervalo_seg * %s""",
            (factor_timeout,),
        )
        candidatos = [f["node_id"] for f in cur.fetchall()]

        for node_id in candidatos:
            # v2: la caida silenciosa (cable cortado, wifi caido) tambien
            # tiene que dejar la FECHA de desconexion en la fila del nodo. Sin
            # esto, el dashboard solo puede decir "no reporta" y no "se
            # desconecto de la red el jueves a las 14:32".
            cur.execute(
                """UPDATE nodos
                      SET estado = 'NO_REPORTA',
                          ultima_desconexion = COALESCE(ultima_desconexion,
                                                        ultimo_reporte, NOW(3)),
                          motivo_desconexion = COALESCE(motivo_desconexion,
                              'Dejo de reportar (sin cierre de conexion)'),
                          caidas_recientes = LEAST(caidas_recientes + 1, 60000)
                    WHERE node_id = %s
                      AND estado = 'ACTIVO'
                      AND TIMESTAMPDIFF(SECOND, ultimo_reporte, NOW())
                          > intervalo_seg * %s""",
                (node_id, factor_timeout),
            )
            if cur.rowcount == 1:
                cur.execute(
                    """INSERT INTO eventos (node_id, tipo, detalle)
                       VALUES (%s, 'NO_REPORTA', 'Sin reportes dentro del umbral')""",
                    (node_id,),
                )
                caidos.append(node_id)
    return caidos


def marcar_nodos_recuperados(factor_timeout: int) -> list[str]:
    """
    La otra mitad del watchdog: un nodo NO_REPORTA que vuelve a reportar dentro
    del umbral pasa a ACTIVO y deja el evento RECUPERADO.

    Tener las dos transiciones en el mismo hilo y con el mismo criterio evita
    que el estado quede oscilando entre dos escritores.
    """
    recuperados: list[str] = []
    with cursor() as cur:
        cur.execute(
            """SELECT node_id FROM nodos
                WHERE estado = 'NO_REPORTA'
                  AND ultimo_reporte IS NOT NULL
                  AND TIMESTAMPDIFF(SECOND, ultimo_reporte, NOW())
                      <= intervalo_seg * %s""",
            (factor_timeout,),
        )
        candidatos = [f["node_id"] for f in cur.fetchall()]

        for node_id in candidatos:
            cur.execute(
                """UPDATE nodos
                      SET estado = 'ACTIVO',
                          ultima_reconexion = NOW(3),
                          motivo_desconexion = NULL
                    WHERE node_id = %s
                      AND estado = 'NO_REPORTA'
                      AND TIMESTAMPDIFF(SECOND, ultimo_reporte, NOW())
                          <= intervalo_seg * %s""",
                (node_id, factor_timeout),
            )
            if cur.rowcount == 1:
                cur.execute(
                    """INSERT INTO eventos (node_id, tipo, detalle)
                       VALUES (%s, 'RECUPERADO', 'El nodo volvio a reportar')""",
                    (node_id,),
                )
                recuperados.append(node_id)
    return recuperados


# ============================================== MENSAJES  (bus API <-> socket)

def crear_mensaje(node_id: str, accion: str = protocolo.ACCION_MENSAJE,
                  texto: str | None = None, valor: int | None = None) -> str:
    """
    La llama la API cuando el dashboard manda algo. Deja la fila en PENDIENTE;
    el despachador del servidor de sockets la recoge en <= 1 segundo.
    Devuelve el cmd_id para que el dashboard pueda seguir su estado.
    """
    if accion not in protocolo.ACCIONES_VALIDAS:
        accion = protocolo.ACCION_MENSAJE
    cmd_id = str(uuid.uuid4())
    with cursor() as cur:
        cur.execute(
            """INSERT INTO mensajes (cmd_id, node_id, accion, texto, valor, estado)
               VALUES (%s, %s, %s, %s, %s, 'PENDIENTE')""",
            (cmd_id, _texto(node_id, _MAX_NODE_ID), accion,
             _texto(texto, _MAX_TEXTO), None if valor is None else int(valor)),
        )
    return cmd_id


def mensajes_pendientes(limite: int = 50) -> list[dict[str, Any]]:
    with cursor() as cur:
        cur.execute(
            """SELECT cmd_id, node_id, accion, texto, valor
                 FROM mensajes
                WHERE estado = 'PENDIENTE'
                ORDER BY creado_en ASC, id ASC
                LIMIT %s""",
            (limite,),
        )
        return cur.fetchall()


def marcar_enviado(cmd_id: str) -> None:
    with cursor() as cur:
        cur.execute(
            "UPDATE mensajes SET estado='ENVIADO', enviado_en=NOW(3) WHERE cmd_id=%s",
            (cmd_id,),
        )


def marcar_fallido(cmd_id: str, motivo: str = "") -> None:
    """El motivo se guarda: un FALLIDO sin explicacion no sirve de nada."""
    with cursor() as cur:
        cur.execute(
            "UPDATE mensajes SET estado='FALLIDO', detalle=%s WHERE cmd_id=%s",
            (_texto(motivo, _MAX_TEXTO), cmd_id),
        )


def confirmar_ack(cmd_id: str) -> None:
    """Llega el ACK del cliente. El cmd_id es lo que lo empareja con su mensaje."""
    with cursor() as cur:
        cur.execute(
            "UPDATE mensajes SET estado='CONFIRMADO', ack_en=NOW(3) WHERE cmd_id=%s",
            (cmd_id,),
        )


def obtener_mensaje(cmd_id: str) -> dict[str, Any] | None:
    with cursor() as cur:
        cur.execute(
            """SELECT cmd_id, node_id, accion, texto, valor, estado, detalle,
                      creado_en, enviado_en, ack_en,
                      TIMESTAMPDIFF(MICROSECOND, enviado_en, ack_en)/1000 AS rtt_ms
                 FROM mensajes WHERE cmd_id = %s""",
            (cmd_id,),
        )
        return cur.fetchone()


def listar_mensajes(node_id: str | None = None, limite: int = 50) -> list[dict[str, Any]]:
    sql = """SELECT cmd_id, node_id, accion, texto, valor, estado, detalle,
                    creado_en, enviado_en, ack_en,
                    TIMESTAMPDIFF(MICROSECOND, enviado_en, ack_en)/1000 AS rtt_ms
               FROM mensajes"""
    params: tuple = ()
    if node_id:
        sql += " WHERE node_id = %s"
        params = (node_id,)
    sql += " ORDER BY creado_en DESC, id DESC LIMIT %s"
    params += (limite,)
    with cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


# ===================================================== CONSULTAS PARA LA API

def listar_nodos() -> list[dict[str, Any]]:
    """Alimenta la tabla de los 9 servidores del dashboard."""
    with cursor() as cur:
        cur.execute("SELECT * FROM v_nodos_estado ORDER BY node_id")
        return cur.fetchall()


def resumen_cluster() -> dict[str, Any]:
    """
    Alimenta el panel de KPIs. v_cluster es una agregacion sin GROUP BY, asi
    que siempre devuelve exactamente una fila, aunque no haya ningun nodo.
    """
    with cursor() as cur:
        cur.execute("SELECT * FROM v_cluster")
        return cur.fetchone() or {}


def historial(node_id: str, horas: int = 24, limite: int = 500) -> list[dict[str, Any]]:
    """
    Serie temporal de un nodo, para el grafico.

    OJO CON EL ORDEN: hay que tomar los `limite` puntos MAS NUEVOS y recien
    despues ordenarlos de viejo a nuevo para dibujarlos. Un
    "ORDER BY timestamp ASC LIMIT 500" directo recorta por el principio de la
    ventana: con 9 nodos cada 10 s son 8.640 filas en 24 h, asi que el grafico
    mostraria las primeras 500 y terminaria hace 22 horas — sin fallar, sin
    avisar, dibujando una curva plausible y vieja.
    """
    with cursor() as cur:
        cur.execute(
            """SELECT * FROM (
                   SELECT timestamp, usado_gb, libre_gb, uso_pct,
                          iops_lectura, iops_escritura, latencia_ms
                     FROM metricas
                    WHERE node_id = %s
                      AND timestamp >= NOW() - INTERVAL %s HOUR
                    ORDER BY timestamp DESC, id DESC
                    LIMIT %s
               ) AS ultimos
               ORDER BY ultimos.timestamp ASC""",
            (node_id, horas, limite),
        )
        return cur.fetchall()


def crecimiento(horas: int = 24) -> list[dict[str, Any]]:
    """
    Growth rate en GB/dia por nodo — indicador que pide el enunciado.

    Toma el primer y el ultimo valor de cada nodo dentro de la ventana con
    funciones de ventana de MySQL 8, en UNA sola consulta. Divide entre las
    horas realmente observadas y no entre 24 fijo, asi el numero sirve aunque
    lleven dos horas de historico.
    """
    with cursor() as cur:
        cur.execute(
            """SELECT DISTINCT
                      node_id,
                      FIRST_VALUE(usado_gb)  OVER w_asc  AS usado_ini,
                      FIRST_VALUE(usado_gb)  OVER w_desc AS usado_fin,
                      FIRST_VALUE(timestamp) OVER w_asc  AS t_ini,
                      FIRST_VALUE(timestamp) OVER w_desc AS t_fin
                 FROM metricas
                WHERE timestamp >= NOW() - INTERVAL %s HOUR
               WINDOW
                 w_asc  AS (PARTITION BY node_id ORDER BY timestamp ASC, id ASC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                 w_desc AS (PARTITION BY node_id ORDER BY timestamp DESC, id DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)""",
            (horas,),
        )
        filas = cur.fetchall()

    resultado = []
    for f in filas:
        segundos = (f["t_fin"] - f["t_ini"]).total_seconds()
        delta = float(f["usado_fin"]) - float(f["usado_ini"])
        gb_dia = (delta / segundos * 86400) if segundos > 0 else 0.0
        resultado.append({
            "node_id": f["node_id"],
            "delta_gb": round(delta, 2),
            "horas_observadas": round(segundos / 3600, 2),
            "growth_gb_dia": round(gb_dia, 3),
        })
    resultado.sort(key=lambda r: r["node_id"])
    return resultado


def _tamano_bucket(horas: int, puntos: int) -> int:
    """
    Segundos por punto para que una ventana de `horas` entre en `puntos`.

    Sin agrupar, 24 h de 10 nodos cada 10 s son 86.400 filas: el navegador
    tendria que dibujar 86.400 segmentos para una linea de 800 pixeles de
    ancho. Se agrupa en el servidor, que es donde estan los datos.
    """
    return max(30, int(horas * 3600 / max(10, puntos)))


def historial_cluster(horas: int = 24, puntos: int = 120) -> list[dict[str, Any]]:
    """
    Utilizacion GLOBAL del cluster en el tiempo: una sola serie.

    POR QUE UNA Y NO DIEZ
    La version anterior dibujaba una linea por nodo en el mismo grafico. Con
    diez nodos eso es un plato de espaguetis: los colores dejan de
    distinguirse, y la pregunta que el grafico tiene que responder —"esta
    subiendo el uso del cluster?"— no se lee. Ahora el grafico grande muestra
    el total, y cada nodo tiene su propia mini-linea en su tarjeta.

    POR QUE PROMEDIO POR NODO Y DESPUES SUMA
    Dentro de un bucket un nodo puede haber reportado tres veces y otro una.
    Sumar directo contaria al primero tres veces y la capacidad del cluster
    daria el triple. Se promedia por nodo dentro del bucket y recien despues
    se suma entre nodos.
    """
    seg = _tamano_bucket(horas, puntos)
    with cursor() as cur:
        cur.execute(
            """SELECT t,
                      ROUND(SUM(usado), 2) AS usado_gb,
                      ROUND(SUM(total), 2) AS total_gb,
                      COUNT(*)             AS nodos
                 FROM (SELECT node_id,
                              FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(timestamp) / %s) * %s) AS t,
                              AVG(usado_gb) AS usado,
                              AVG(total_gb) AS total
                         FROM metricas
                        WHERE timestamp >= NOW() - INTERVAL %s HOUR
                        GROUP BY node_id, t) AS por_nodo
                GROUP BY t
                ORDER BY t""",
            (seg, seg, horas),
        )
        filas = cur.fetchall()
    for f in filas:
        total = float(f["total_gb"] or 0)
        f["uso_pct"] = round(float(f["usado_gb"] or 0) / total * 100, 2) if total else 0.0
    return filas


def sparklines(horas: int = 6, puntos: int = 30) -> dict[str, list[float]]:
    """
    Una mini-serie de utilizacion por nodo, para dibujarla dentro de su tarjeta.

    Una sola consulta para todos los nodos. La alternativa —un GET por nodo
    desde el navegador— eran diez peticiones cada vez que se refresca, y con
    tres pantallas abiertas, treinta.
    """
    seg = _tamano_bucket(horas, puntos)
    with cursor() as cur:
        cur.execute(
            """SELECT node_id,
                      FLOOR(UNIX_TIMESTAMP(timestamp) / %s) AS bucket,
                      ROUND(AVG(uso_pct), 2) AS pct
                 FROM metricas
                WHERE timestamp >= NOW() - INTERVAL %s HOUR
                GROUP BY node_id, bucket
                ORDER BY node_id, bucket""",
            (seg, horas),
        )
        filas = cur.fetchall()
    salida: dict[str, list[float]] = {}
    for f in filas:
        salida.setdefault(f["node_id"], []).append(float(f["pct"] or 0))
    return salida


def distribucion_uso(tramos: int = 5,
                     nodos: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """
    Histograma: cuantos nodos hay en cada tramo de utilizacion.

    Responde de un vistazo algo que la lista de tarjetas no responde: "esta el
    cluster equilibrado, o hay unos pocos nodos llenos y el resto vacios?".
    Con nueve regionales eso decide si hace falta comprar discos o mover datos.

    El calculo es en Python y no en SQL a proposito: son diez filas, ya estan
    en memoria por listar_nodos(), y en SQL habria que meter un CASE de cinco
    ramas que hay que tocar cada vez que alguien quiera otro tramo.

    `nodos` permite pasar la lista ya leida. La difusion por WebSocket la lee
    una vez por ciclo: sin este parametro, calcular el histograma duplicaria
    esa consulta cada segundo, para todos los navegadores.
    """
    ancho = 100 / max(1, tramos)
    cubos = [{"desde": round(i * ancho), "hasta": round((i + 1) * ancho),
              "nodos": 0, "node_ids": []} for i in range(tramos)]
    for n in (listar_nodos() if nodos is None else nodos):
        if n.get("uso_pct") is None or n.get("estado") != "ACTIVO":
            continue
        pct = float(n["uso_pct"])
        # El 100% cae en el ultimo tramo, no en uno inexistente.
        i = min(tramos - 1, int(pct / ancho))
        cubos[i]["nodos"] += 1
        cubos[i]["node_ids"].append(n["node_id"])
    return cubos


def disponibilidad(horas: int = 24) -> list[dict[str, Any]]:
    """
    Disponibilidad por nodo dentro de una ventana: proporcion de reportes
    recibidos sobre los esperados. Meta del enunciado: >= 99.9%.

    Detalles que importan para poder defender el numero:
      - Se acota a una ventana. Contar toda la historia contra el intervalo
        ACTUAL da un numero sin sentido en cuanto alguien cambie el intervalo
        desde el dashboard (requisito 7.3).
      - Para un nodo recien dado de alta se usa su tiempo de vida real, no la
        ventana entera: si no, un nodo con 30 segundos de vida sale con 0.2%.
      - No se recorta a 100%. Un valor por encima significa que el nodo reporta
        mas seguido que su intervalo configurado, y esconderlo detras de un
        min(100, x) haria que la meta del 99.9% se cumpla por saturacion.
    """
    with cursor() as cur:
        cur.execute(
            """SELECT n.node_id,
                      n.intervalo_seg,
                      LEAST(TIMESTAMPDIFF(SECOND, n.primer_registro, NOW()),
                            %s * 3600)                       AS segundos_ventana,
                      (SELECT COUNT(*) FROM metricas m
                        WHERE m.node_id = n.node_id
                          AND m.timestamp >= NOW() - INTERVAL %s HOUR) AS reportes
                 FROM nodos n
                ORDER BY n.node_id""",
            (horas, horas),
        )
        filas = cur.fetchall()

    salida = []
    for f in filas:
        intervalo = max(1, int(f["intervalo_seg"]))
        ventana = max(0, int(f["segundos_ventana"] or 0))
        esperados = max(1, ventana // intervalo)
        pct = int(f["reportes"]) / esperados * 100
        salida.append({
            "node_id": f["node_id"],
            "reportes": int(f["reportes"]),
            "esperados": esperados,
            "disponibilidad_pct": round(pct, 2),
        })
    return salida


def listar_eventos(limite: int = 100, node_id: str | None = None) -> list[dict[str, Any]]:
    sql = "SELECT node_id, timestamp, tipo, detalle FROM eventos"
    params: tuple = ()
    if node_id:
        sql += " WHERE node_id = %s"
        params = (node_id,)
    sql += " ORDER BY timestamp DESC, id DESC LIMIT %s"
    params += (limite,)
    with cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()
