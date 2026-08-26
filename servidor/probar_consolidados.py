"""
Prueba de metricas consolidadas — M2.5.  Edwin.

    python -m servidor.probar_consolidados

Comprueba (con servidor + clientes reales alimentando metricas):

  1. Totales de v_cluster cuadran con suma manual de nodos ACTIVOS
  2. % global = usado_total / capacidad_total (se recalcula con cada METRIC)
  3. Growth rate sale del historico (primer vs ultimo usado_gb), no de un dato suelto
  4. Indicadores N/A documentados (overcommit, fragmentacion, quorum, replication)

Usa prefijo CLU- y borra al terminar.
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from comun import config                                          # noqa: E402
from db import repositorio as repo                                # noqa: E402
from db.conexion import cerrar_conexion_del_hilo, cursor          # noqa: E402
from servidor.consolidados import (                               # noqa: E402
    INDICADORES_NO_APLICA,
    suma_manual_desde_nodos,
    verificar_coherencia_cluster,
    verificar_growth_desde_historico,
)

PREFIJO = "CLU-"
PUERTO = int(os.getenv("PUERTO_PRUEBA_CLU", "5194"))
INTERVALO = 2
NODOS = [(f"{PREFIJO}{nid}", reg) for nid, reg in config.REGIONALES[:9]]

fallos: list[str] = []
_procesos: list[subprocess.Popen] = []


def check(nombre: str, ok: bool, detalle: str = "") -> bool:
    print(f"  [{'OK  ' if ok else 'FALLA'}] {nombre}"
          + (f"  -> {detalle}" if detalle else ""))
    if not ok:
        fallos.append(nombre)
    return ok


def esperar(condicion, segundos: float = 40.0, paso: float = 0.5) -> bool:
    limite = time.time() + segundos
    while time.time() < limite:
        try:
            if condicion():
                return True
        except Exception:                                         # noqa: BLE001
            pass
        time.sleep(paso)
    return False


def _entorno(**extra: str) -> dict[str, str]:
    e = dict(os.environ)
    e.update({
        "SOCKET_HOST": "127.0.0.1",
        "SOCKET_PORT": str(PUERTO),
        "INTERVALO_DEFECTO_SEG": str(INTERVALO),
        "PYTHONUNBUFFERED": "1",
    })
    e.update(extra)
    return e


def arrancar(cmd: list[str]) -> subprocess.Popen:
    p = subprocess.Popen(cmd, cwd=RAIZ, env=_entorno(),
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _procesos.append(p)
    return p


def matar(p: subprocess.Popen | None) -> None:
    if p is None or p.poll() is not None:
        return
    p.send_signal(signal.SIGINT)
    try:
        p.wait(timeout=8)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=5)


def limpiar_bd() -> None:
    with cursor() as cur:
        cur.execute("DELETE FROM nodos WHERE node_id LIKE %s", (PREFIJO + "%",))


def metricas_de(node_id: str) -> int:
    with cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM metricas WHERE node_id=%s", (node_id,))
        return int(cur.fetchone()["c"])


def main() -> int:
    print("=" * 70)
    print(" PRUEBA CONSOLIDADOS — M2.5")
    print(f" puerto {PUERTO}  ·  hasta {len(NODOS)} nodos  ·  intervalo {INTERVALO}s")
    print("=" * 70)

    config.asegurar_directorios()
    limpiar_bd()

    try:
        print("\n1. Alimentar metricas (servidor -> guardar_metrica -> historial)")
        srv = arrancar([sys.executable, "-m", "servidor.main"])
        time.sleep(2.5)
        check("Servidor activo", srv.poll() is None)

        for nid, reg in NODOS:
            arrancar([sys.executable, "-m", "cliente.main",
                      "--node-id", nid, "--region", reg,
                      "--host", "127.0.0.1", "--puerto", str(PUERTO),
                      "--intervalo", str(INTERVALO)])
            time.sleep(0.12)

        ids = [n for n, _ in NODOS]
        principal = ids[0]
        check("Nodos con metricas en historial",
              esperar(lambda: all(metricas_de(i) >= 3 for i in ids[:3]), 45),
              f"ej. {principal}: {metricas_de(principal)} filas")

        print("\n2. Totales cuadran con suma manual (v_cluster)")
        ok, det = verificar_coherencia_cluster()
        manual = suma_manual_desde_nodos()
        vista = repo.resumen_cluster()
        check("v_cluster == suma manual de nodos ACTIVOS", ok, det)
        check("Capacidad total > 0",
              float(vista.get("capacidad_total_gb") or 0) > 0,
              f"{vista.get('capacidad_total_gb')} GB")
        check("Nodos activos / totales",
              int(vista.get("nodos_activos") or 0) >= len(ids),
              f"{vista.get('nodos_activos')}/{vista.get('nodos_totales')}")

        print("\n3. % global coherente y se recalcula con nuevas metricas")
        pct1 = float(vista.get("uso_pct_global") or 0)
        usado1 = float(vista.get("usado_total_gb") or 0)
        cap1 = float(vista.get("capacidad_total_gb") or 0)
        esperado1 = round(usado1 / cap1 * 100, 2) if cap1 else 0
        check("% global = usado/capacidad x 100",
              abs(pct1 - esperado1) < 0.5, f"{pct1}% vs {esperado1}%")

        time.sleep(INTERVALO * 2 + 1)
        ok2, det2 = verificar_coherencia_cluster()
        vista2 = repo.resumen_cluster()
        check("Consolidados siguen coherentes tras mas METRIC", ok2, det2)
        # usado puede variar poco en disco real; lo importante es que la formula
        # sigue cuadrando con los nodos actuales.
        manual2 = suma_manual_desde_nodos()
        pct2 = float(vista2.get("uso_pct_global") or 0)
        u2, c2 = float(manual2["usado_total_gb"]), float(manual2["capacidad_total_gb"])
        check("% global recalculado desde nodos actuales",
              abs(pct2 - round(u2 / c2 * 100, 2)) < 0.5 if c2 else True,
              f"{pct2}%")

        print("\n4. Growth rate desde historico (no dato suelto)")
        ok_g, det_g = verificar_growth_desde_historico(principal, horas=1)
        check("Growth usa ventana temporal del historial", ok_g, det_g)
        g_list = repo.crecimiento(horas=1)
        check("crecimiento() devuelve filas por nodo",
              len(g_list) >= 3, f"{len(g_list)} nodos")

        print("\n5. Indicadores N/A del enunciado (con justificacion)")
        check("Overcommit documentado como N/A", "overcommit" in INDICADORES_NO_APLICA)
        check("Fragmentacion documentada como N/A", "fragmentacion" in INDICADORES_NO_APLICA)
        check("Quorum documentado como N/A", "quorum" in INDICADORES_NO_APLICA)
        check("Replication health documentado como N/A",
              "replication_health" in INDICADORES_NO_APLICA)
        for k, v in INDICADORES_NO_APLICA.items():
            check(f"Justificacion {k}", v.startswith("N/A"), v[:60] + "…")

        print("\n--- Reporte completo ---")
        from servidor.consolidados import reporte_cluster
        reporte_cluster(horas_growth=1)

    finally:
        print("\nLimpiando...")
        for p in list(_procesos):
            matar(p)
        try:
            limpiar_bd()
        except Exception as e:                                    # noqa: BLE001
            print(f"  AVISO: {e}")
        cerrar_conexion_del_hilo()

    print("\n" + "=" * 70)
    if fallos:
        print(f" RESULTADO: {len(fallos)} fallas")
        for f in fallos:
            print(f"   - {f}")
        return 1
    print(" RESULTADO: consolidados M2.5 OK.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
