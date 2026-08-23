"""
Capa de acceso a datos — tareas 3.1, 3.2 y 3.3.  Responsable: Robert.

Todo el SQL del proyecto vive aqui. El servidor de sockets y la API llaman a
estas funciones; ninguno escribe SQL por su cuenta. Asi, cuando cambie una
columna, se toca un solo archivo.

Funciones agrupadas por quien las usa:
    SERVIDOR : registrar_nodo, guardar_metrica, registrar_evento,
               marcar_nodos_caidos, mensajes_pendientes, marcar_enviado,
               confirmar_ack, actualizar_intervalo
    API      : listar_nodos, resumen_cluster, historial, crecimiento,
               listar_eventos, crear_mensaje, listar_mensajes
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from db.conexion import cursor


# ============================================================== NODOS / ALTA

def registrar_nodo(node_id: str, region: str, hostname: str | None,
                   so: str | None, ip: str | None,
                   intervalo: int) -> tuple[bool, int]:
    """
    Alta automatica de cliente — requisito 7.2 (vale 10%).

    Si el node_id no existe, lo inserta y deja el evento ALTA_AUTOMATICA.
    Si ya existia, solo actualiza sus datos y lo pone ACTIVO.

    Devuelve (es_nuevo, intervalo_vigente).

    El intervalo que devuelve es el de la BASE, no el que mando el cliente: si
    un operador cambio el intervalo de ese nodo desde el dashboard, el cliente
    tiene que adoptarlo al reconectar. Esa es la mitad "desde el servidor" del
    requisito 7.3.
    """
    with cursor() as cur:
        cur.execute("SELECT intervalo_seg FROM nodos WHERE node_id = %s", (node_id,))
        fila = cur.fetchone()

        if fila is None:
            cur.execute(
                """INSERT INTO nodos
                       (node_id, region, hostname, sistema_operativo, ip,
                        estado, intervalo_seg, ultimo_reporte)
                   VALUES (%s, %s, %s, %s, %s, 'ACTIVO', %s, NOW(3))""",
                (node_id, region, hostname, so, ip, intervalo),
            )
            cur.execute(
                """INSERT INTO eventos (node_id, tipo, detalle)
                   VALUES (%s, 'ALTA_AUTOMATICA', %s)""",
                (node_id, f"Alta automatica de {region} desde {ip}"),
            )
            return True, intervalo

        cur.execute(
            """UPDATE nodos
                  SET region = %s, hostname = %s, sistema_operativo = %s,
                      ip = %s, estado = 'ACTIVO', ultimo_reporte = NOW(3)
                WHERE node_id = %s""",
            (region, hostname, so, ip, node_id),
        )
        cur.execute(
            "INSERT INTO eventos (node_id, tipo, detalle) VALUES (%s, 'CONEXION', %s)",
            (node_id, f"Reconexion desde {ip}"),
        )
        return False, int(fila["intervalo_seg"])


def registrar_evento(node_id: str, tipo: str, detalle: str | None = None) -> None:
    with cursor() as cur:
        cur.execute(
            "INSERT INTO eventos (node_id, tipo, detalle) VALUES (%s, %s, %s)",
            (node_id, tipo, detalle),
        )


def actualizar_intervalo(node_id: str, segundos: int) -> None:
    """Persistir el intervalo nuevo — requisito 7.3."""
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
    """
    with cursor() as cur:
        cur.execute(
            """INSERT INTO metricas
                   (node_id, timestamp, disco_nombre, disco_tipo,
                    total_gb, usado_gb, libre_gb, uso_pct,
                    iops_lectura, iops_escritura, latencia_ms)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                node_id,
                _a_datetime(timestamp),
                disco.get("nombre"),
                disco.get("tipo", "DESCONOCIDO"),
                disco["total_gb"], disco["usado_gb"], disco["libre_gb"],
                disco["uso_pct"],
                disco.get("iops_lectura", 0), disco.get("iops_escritura", 0),
                disco.get("latencia_ms", 0),
            ),
        )
        cur.execute(
            """UPDATE nodos
                  SET ultimo_reporte = NOW(3),
                      estado = 'ACTIVO'
                WHERE node_id = %s""",
            (node_id,),
        )


# ================================================================== WATCHDOG

def marcar_nodos_caidos(factor_timeout: int) -> list[str]:
    """
    Watchdog — tarea 2.3. Marca NO_REPORTA a todo nodo cuyo ultimo reporte sea
    mas viejo que factor_timeout x su propio intervalo.

    El umbral es POR NODO, no global: si un nodo reporta cada 30 s y otro cada
    5 s, no pueden compartir el mismo timeout. Ese detalle suele preguntarse.

    Devuelve la lista de node_id que acaban de cambiar de estado, para que el
    servidor deje el evento y lo loguee.
    """
    with cursor() as cur:
        cur.execute(
            """SELECT node_id FROM nodos
                WHERE estado = 'ACTIVO'
                  AND ultimo_reporte IS NOT NULL
                  AND TIMESTAMPDIFF(SECOND, ultimo_reporte, NOW())
                      > intervalo_seg * %s""",
            (factor_timeout,),
        )
        caidos = [f["node_id"] for f in cur.fetchall()]

        for node_id in caidos:
            cur.execute("UPDATE nodos SET estado = 'NO_REPORTA' WHERE node_id = %s",
                        (node_id,))
            cur.execute(
                """INSERT INTO eventos (node_id, tipo, detalle)
                   VALUES (%s, 'NO_REPORTA', 'Sin reportes dentro del umbral')""",
                (node_id,),
            )
        return caidos


def marcar_recuperado(node_id: str) -> None:
    with cursor() as cur:
        cur.execute("UPDATE nodos SET estado = 'ACTIVO' WHERE node_id = %s", (node_id,))
        cur.execute(
            """INSERT INTO eventos (node_id, tipo, detalle)
               VALUES (%s, 'RECUPERADO', 'El nodo volvio a reportar')""",
            (node_id,),
        )


# ============================================== MENSAJES  (bus API <-> socket)

def crear_mensaje(node_id: str, accion: str = "MENSAJE",
                  texto: str | None = None, valor: int | None = None) -> str:
    """
    La llama la API cuando el dashboard manda algo. Deja la fila en PENDIENTE;
    el despachador del servidor de sockets la recoge en <= 1 segundo.
    Devuelve el cmd_id para que el dashboard pueda seguir su estado.
    """
    cmd_id = str(uuid.uuid4())
    with cursor() as cur:
        cur.execute(
            """INSERT INTO mensajes (cmd_id, node_id, accion, texto, valor, estado)
               VALUES (%s, %s, %s, %s, %s, 'PENDIENTE')""",
            (cmd_id, node_id, accion, texto, valor),
        )
    return cmd_id


def mensajes_pendientes(limite: int = 50) -> list[dict[str, Any]]:
    with cursor() as cur:
        cur.execute(
            """SELECT cmd_id, node_id, accion, texto, valor
                 FROM mensajes
                WHERE estado = 'PENDIENTE'
                ORDER BY creado_en ASC
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
    with cursor() as cur:
        cur.execute("UPDATE mensajes SET estado='FALLIDO' WHERE cmd_id=%s", (cmd_id,))


def confirmar_ack(cmd_id: str) -> None:
    """Llega el ACK del cliente. El cmd_id es lo que lo empareja con su mensaje."""
    with cursor() as cur:
        cur.execute(
            "UPDATE mensajes SET estado='CONFIRMADO', ack_en=NOW(3) WHERE cmd_id=%s",
            (cmd_id,),
        )


def listar_mensajes(node_id: str | None = None, limite: int = 50) -> list[dict[str, Any]]:
    sql = """SELECT cmd_id, node_id, accion, texto, valor, estado,
                    creado_en, enviado_en, ack_en,
                    TIMESTAMPDIFF(MICROSECOND, enviado_en, ack_en)/1000 AS rtt_ms
               FROM mensajes"""
    params: tuple = ()
    if node_id:
        sql += " WHERE node_id = %s"
        params = (node_id,)
    sql += " ORDER BY creado_en DESC LIMIT %s"
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
    """Alimenta el panel de KPIs. Una sola fila."""
    with cursor() as cur:
        cur.execute("SELECT * FROM v_cluster")
        return cur.fetchone() or {}


def historial(node_id: str, horas: int = 24, limite: int = 500) -> list[dict[str, Any]]:
    """Serie temporal de un nodo, para el grafico."""
    with cursor() as cur:
        cur.execute(
            """SELECT timestamp, usado_gb, libre_gb, uso_pct,
                      iops_lectura, iops_escritura, latencia_ms
                 FROM metricas
                WHERE node_id = %s
                  AND timestamp >= NOW() - INTERVAL %s HOUR
                ORDER BY timestamp ASC
                LIMIT %s""",
            (node_id, horas, limite),
        )
        return cur.fetchall()


def crecimiento(horas: int = 24) -> list[dict[str, Any]]:
    """
    Growth rate en GB/dia por nodo — indicador que pide el enunciado.

    Usa funciones de ventana de MySQL 8 (FIRST_VALUE) para tomar el primer y el
    ultimo valor de cada nodo dentro de la ventana en UNA sola consulta.
    Si el histórico es mas corto que la ventana, extrapola con el tiempo real
    transcurrido, por eso divide entre las horas efectivas y no entre 24 fijo.
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
                 w_asc  AS (PARTITION BY node_id ORDER BY timestamp ASC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                 w_desc AS (PARTITION BY node_id ORDER BY timestamp DESC
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
    return resultado


def disponibilidad() -> list[dict[str, Any]]:
    """
    Disponibilidad aproximada por nodo: proporcion de reportes recibidos sobre
    los esperados desde el primer registro. Meta del enunciado: >= 99.9%.
    """
    with cursor() as cur:
        cur.execute(
            """SELECT n.node_id,
                      n.intervalo_seg,
                      COUNT(m.id) AS reportes,
                      TIMESTAMPDIFF(SECOND, n.primer_registro, NOW()) AS segundos_vida
                 FROM nodos n
                 LEFT JOIN metricas m ON m.node_id = n.node_id
                GROUP BY n.node_id, n.intervalo_seg, n.primer_registro"""
        )
        filas = cur.fetchall()

    salida = []
    for f in filas:
        esperados = max(1, int(f["segundos_vida"] or 0) // max(1, int(f["intervalo_seg"])))
        pct = min(100.0, (int(f["reportes"]) / esperados) * 100)
        salida.append({"node_id": f["node_id"], "disponibilidad_pct": round(pct, 2)})
    return salida


def listar_eventos(limite: int = 100, node_id: str | None = None) -> list[dict[str, Any]]:
    sql = "SELECT node_id, timestamp, tipo, detalle FROM eventos"
    params: tuple = ()
    if node_id:
        sql += " WHERE node_id = %s"
        params = (node_id,)
    sql += " ORDER BY timestamp DESC LIMIT %s"
    params += (limite,)
    with cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


# ------------------------------------------------------------------ utilidades

def _a_datetime(iso: str) -> datetime:
    """El cliente manda ISO 8601; MySQL quiere un datetime de Python."""
    try:
        return datetime.fromisoformat(iso)
    except (TypeError, ValueError):
        return datetime.now()
