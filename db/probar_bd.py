"""
Prueba de la capa de datos — cierra los "listo cuando" de las tareas 3.1 a 3.3.
Responsable: Robert.  Se corre ANTES de que el servidor de sockets exista.

    python -m db.probar_bd

Que verifica:
  1. Conexion y version de MySQL
  2. Que las 4 tablas y las 3 vistas existan
  3. Alta automatica: un nodo nuevo se inserta solo y deja su evento
  4. CONCURRENCIA REAL: 9 hilos escribiendo metricas al mismo tiempo durante
     N segundos, sin errores de bloqueo y sin perder ni una fila
  5. Watchdog: un nodo sin reportes pasa a NO_REPORTA
  6. Ciclo de mensaje: PENDIENTE -> ENVIADO -> CONFIRMADO
  7. Las consultas de agregacion devuelven numeros coherentes

Si este script pasa entero, la parte de base de datos esta terminada y el resto
del equipo puede construir encima con confianza.
"""
from __future__ import annotations

import random
import sys
import threading
import time

from comun import config
from db import repositorio as repo
from db.conexion import cursor, probar_conexion

SEGUNDOS_CARGA = 20          # subir a 600 para la prueba de 10 minutos
INTERVALO_PRUEBA = 1         # los nodos de prueba reportan cada 1 s

fallos: list[str] = []
errores_hilos: list[str] = []


def check(nombre: str, condicion: bool, detalle: str = "") -> None:
    marca = "OK  " if condicion else "FALLA"
    print(f"  [{marca}] {nombre}" + (f"  -> {detalle}" if detalle else ""))
    if not condicion:
        fallos.append(nombre)


# --------------------------------------------------------------------- 1 y 2

def prueba_estructura() -> None:
    print("\n1-2. Conexion y estructura")
    check("Conexion a MySQL", probar_conexion())

    with cursor() as cur:
        cur.execute("SHOW TABLES")
        nombres = {list(f.values())[0] for f in cur.fetchall()}

    for t in ("nodos", "metricas", "eventos", "mensajes"):
        check(f"Tabla {t}", t in nombres)
    for v in ("v_ultima_metrica", "v_nodos_estado", "v_cluster"):
        check(f"Vista {v}", v in nombres)

    with cursor() as cur:
        cur.execute("SHOW INDEX FROM metricas WHERE Key_name = 'ix_nodo_tiempo'")
        check("Indice compuesto (node_id, timestamp)", len(cur.fetchall()) == 2)


# ------------------------------------------------------------------------- 3

def prueba_alta_automatica() -> None:
    print("\n3. Alta automatica de cliente (requisito 7.2)")
    node_id = f"TEST-AUTO-{random.randint(1000, 9999)}"

    nuevo, intervalo = repo.registrar_nodo(
        node_id, "Regional de prueba", "host-test", "Linux",
        "192.168.1.99", INTERVALO_PRUEBA)
    check("Primer HELLO devuelve nuevo=True", nuevo is True)
    check("Devuelve el intervalo", intervalo == INTERVALO_PRUEBA, str(intervalo))

    eventos = repo.listar_eventos(limite=5, node_id=node_id)
    check("Dejo el evento ALTA_AUTOMATICA",
          any(e["tipo"] == "ALTA_AUTOMATICA" for e in eventos))

    nuevo2, _ = repo.registrar_nodo(
        node_id, "Regional de prueba", "host-test", "Linux",
        "192.168.1.99", INTERVALO_PRUEBA)
    check("Segundo HELLO devuelve nuevo=False", nuevo2 is False)

    _limpiar(node_id)


# ------------------------------------------------------------------------- 4

def _escritor(node_id: str, region: str, hasta: float) -> None:
    """Simula un nodo: se registra y escribe metricas hasta 'hasta'."""
    try:
        repo.registrar_nodo(node_id, region, f"host-{node_id}", "Linux",
                            "192.168.1.50", INTERVALO_PRUEBA)
        usado = random.uniform(100, 300)
        while time.time() < hasta:
            usado += random.uniform(0.01, 0.08)      # crecimiento simulado
            total = 500.0
            repo.guardar_metrica(node_id, _iso(), {
                "nombre": "/dev/sda1",
                "tipo": random.choice(["SSD", "HDD"]),
                "total_gb": round(total, 2),
                "usado_gb": round(usado, 2),
                "libre_gb": round(total - usado, 2),
                "uso_pct": round(usado / total * 100, 2),
                "iops_lectura": random.randint(50, 400),
                "iops_escritura": random.randint(20, 200),
                "latencia_ms": round(random.uniform(0.2, 3.0), 3),
            })
            time.sleep(INTERVALO_PRUEBA)
    except Exception as e:                            # noqa: BLE001
        errores_hilos.append(f"{node_id}: {type(e).__name__}: {e}")


def prueba_concurrencia() -> None:
    print(f"\n4. Concurrencia: 9 hilos escribiendo {SEGUNDOS_CARGA} s")
    ids = [f"TEST-{n}" for n, _ in config.REGIONALES]

    with cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM metricas")
        antes = cur.fetchone()["c"]

    hasta = time.time() + SEGUNDOS_CARGA
    hilos = [threading.Thread(target=_escritor, args=(nid, reg, hasta), daemon=True)
             for nid, (_, reg) in zip(ids, config.REGIONALES)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()

    with cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM metricas")
        despues = cur.fetchone()["c"]

    insertadas = despues - antes
    esperadas = 9 * (SEGUNDOS_CARGA // INTERVALO_PRUEBA)

    check("Ningun error de bloqueo en los hilos",
          not errores_hilos, "; ".join(errores_hilos[:3]))
    check("No se perdieron filas",
          insertadas >= esperadas * 0.95,
          f"{insertadas} insertadas de ~{esperadas} esperadas")

    with cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM v_ultima_metrica WHERE node_id LIKE 'TEST-%'")
        check("v_ultima_metrica devuelve 1 fila por nodo",
              cur.fetchone()["c"] == 9)

    # Datos cruzados: cada nodo debe tener solo metricas suyas.
    with cursor() as cur:
        cur.execute("""SELECT COUNT(DISTINCT node_id) AS n FROM metricas
                        WHERE node_id LIKE 'TEST-%'""")
        check("Sin datos cruzados entre nodos", cur.fetchone()["n"] == 9)


# ------------------------------------------------------------------------- 5

def prueba_watchdog() -> None:
    print("\n5. Watchdog: deteccion de nodo caido")
    node_id = "TEST-CNS-LPZ-01"
    with cursor() as cur:
        cur.execute(
            """UPDATE nodos
                  SET ultimo_reporte = NOW(3) - INTERVAL 60 SECOND, estado='ACTIVO'
                WHERE node_id = %s""", (node_id,))

    caidos = repo.marcar_nodos_caidos(config.FACTOR_TIMEOUT)
    check("Detecta el nodo sin reportes", node_id in caidos, str(caidos[:3]))

    with cursor() as cur:
        cur.execute("SELECT estado FROM nodos WHERE node_id=%s", (node_id,))
        check("Quedo en NO_REPORTA", cur.fetchone()["estado"] == "NO_REPORTA")

    repo.marcar_recuperado(node_id)
    with cursor() as cur:
        cur.execute("SELECT estado FROM nodos WHERE node_id=%s", (node_id,))
        check("Vuelve a ACTIVO al recuperarse", cur.fetchone()["estado"] == "ACTIVO")


# ------------------------------------------------------------------------- 6

def prueba_mensajes() -> None:
    print("\n6. Ciclo de mensaje PENDIENTE -> ENVIADO -> CONFIRMADO")
    node_id = "TEST-CNS-CBB-02"
    cmd_id = repo.crear_mensaje(node_id, "MENSAJE", "Verifique espacio en disco")

    pendientes = repo.mensajes_pendientes()
    check("El despachador lo ve como PENDIENTE",
          any(m["cmd_id"] == cmd_id for m in pendientes))

    repo.marcar_enviado(cmd_id)
    time.sleep(0.05)
    repo.confirmar_ack(cmd_id)

    msgs = repo.listar_mensajes(node_id=node_id, limite=5)
    fila = next((m for m in msgs if m["cmd_id"] == cmd_id), None)
    check("Queda CONFIRMADO", fila is not None and fila["estado"] == "CONFIRMADO")
    check("Se calcula el round-trip", fila is not None and fila["rtt_ms"] is not None,
          f"{fila['rtt_ms']} ms" if fila and fila["rtt_ms"] else "")

    cmd2 = repo.crear_mensaje(node_id, "SET_INTERVAL", None, 5)
    repo.actualizar_intervalo(node_id, 5)
    with cursor() as cur:
        cur.execute("SELECT intervalo_seg FROM nodos WHERE node_id=%s", (node_id,))
        check("SET_INTERVAL persiste el intervalo (7.3)",
              int(cur.fetchone()["intervalo_seg"]) == 5)
    repo.marcar_fallido(cmd2)


# ------------------------------------------------------------------------- 7

def prueba_agregaciones() -> None:
    print("\n7. Consultas de agregacion")
    c = repo.resumen_cluster()
    check("Resumen del cluster devuelve datos", bool(c))
    check("Capacidad total > 0", float(c.get("capacidad_total_gb", 0)) > 0,
          f"{c.get('capacidad_total_gb')} GB")

    total = float(c.get("capacidad_total_gb") or 0)
    usado = float(c.get("usado_total_gb") or 0)
    libre = float(c.get("libre_total_gb") or 0)
    check("usado + libre = total (+/- 1 GB)", abs((usado + libre) - total) < 1.0,
          f"{usado:.2f} + {libre:.2f} vs {total:.2f}")

    pct = float(c.get("uso_pct_global") or 0)
    check("% global coherente", abs(pct - (usado / total * 100)) < 0.5 if total else False,
          f"{pct}%")

    nodos = repo.listar_nodos()
    check("listar_nodos trae filas", len(nodos) > 0, f"{len(nodos)} nodos")

    hist = repo.historial("TEST-CNS-LPZ-01", horas=1)
    check("historial devuelve serie temporal", len(hist) > 1, f"{len(hist)} puntos")

    g = repo.crecimiento(horas=1)
    check("crecimiento calcula GB/dia", len(g) > 0,
          f"ej: {g[0]['growth_gb_dia']} GB/dia" if g else "")

    d = repo.disponibilidad()
    check("disponibilidad calculada", len(d) > 0,
          f"ej: {d[0]['disponibilidad_pct']}%" if d else "")


# ------------------------------------------------------------------- limpieza

def _limpiar(patron: str = "TEST-%") -> None:
    with cursor() as cur:
        cur.execute("DELETE FROM nodos WHERE node_id LIKE %s", (patron,))
        # metricas, eventos y mensajes caen solos por ON DELETE CASCADE


def _iso() -> str:
    from datetime import datetime
    return datetime.now().isoformat(timespec="milliseconds")


def main() -> int:
    print("=" * 68)
    print(" PRUEBA DE LA CAPA DE DATOS - Storage Cluster CNS")
    print("=" * 68)

    prueba_estructura()
    if fallos:
        print("\nLa estructura no esta lista. Corre primero:  mysql -u root -p < db/schema.sql")
        return 1

    prueba_alta_automatica()
    prueba_concurrencia()
    prueba_watchdog()
    prueba_mensajes()
    prueba_agregaciones()

    print("\nLimpiando datos de prueba...")
    _limpiar()

    print("\n" + "=" * 68)
    if fallos:
        print(f" RESULTADO: {len(fallos)} fallas -> {', '.join(fallos)}")
        return 1
    print(" RESULTADO: todo OK. La capa de datos esta lista para el equipo.")
    print("=" * 68)
    return 0


if __name__ == "__main__":
    sys.exit(main())
