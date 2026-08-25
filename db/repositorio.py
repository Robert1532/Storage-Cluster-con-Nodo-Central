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
                   intervalo: int) -> tuple[bool, int]:
    """
    Alta automatica de cliente — requisito 7.2 (vale 10%).

    Si el node_id no existe, lo inserta y deja el evento ALTA_AUTOMATICA.
    Si ya existia, actualiza sus datos y deja el evento CONEXION.

    Devuelve (es_nuevo, intervalo_vigente).

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
    hostname = _texto(hostname, _MAX_HOSTNAME)
    so = _texto(so, _MAX_SO)
    ip = _texto(ip, _MAX_IP)
    intervalo = config.acotar_intervalo(intervalo)

    with cursor() as cur:
        cur.execute(
            """INSERT INTO nodos
                   (node_id, region, hostname, sistema_operativo, ip,
                    estado, intervalo_seg, ultimo_reporte)
               VALUES (%s, %s, %s, %s, %s, 'ACTIVO', %s, NOW(3))
               ON DUPLICATE KEY UPDATE
                   region            = VALUES(region),
                   hostname          = VALUES(hostname),
                   sistema_operativo = VALUES(sistema_operativo),
                   ip                = VALUES(ip),
                   ultimo_reporte    = NOW(3)""",
            (node_id, region, hostname, so, ip, intervalo),
        )
        es_nuevo = cur.rowcount == 1

        cur.execute(
            "INSERT INTO eventos (node_id, tipo, detalle) VALUES (%s, %s, %s)",
            (node_id,
             "ALTA_AUTOMATICA" if es_nuevo else "CONEXION",
             (f"Alta automatica de {region} desde {ip}" if es_nuevo
              else f"Reconexion desde {ip}")),
        )

        cur.execute("SELECT intervalo_seg FROM nodos WHERE node_id = %s", (node_id,))
        fila = cur.fetchone()
        vigente = int(fila["intervalo_seg"]) if fila else intervalo

    return es_nuevo, vigente


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

def guardar_metrica(node_id: str, timestamp: str, disco: dict) -> None:
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
                    iops_lectura, iops_escritura, latencia_ms)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
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
            ),
        )
        cur.execute("UPDATE nodos SET ultimo_reporte = NOW(3) WHERE node_id = %s",
                    (node_id,))


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
            cur.execute(
                """UPDATE nodos SET estado = 'NO_REPORTA'
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
                """UPDATE nodos SET estado = 'ACTIVO'
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
