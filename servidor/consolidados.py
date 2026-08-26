"""
Metricas consolidadas del cluster — M2.5.  Edwin.

El servidor de sockets ALIMENTA los consolidados: cada METRIC que recibe
atender_cliente() guarda una fila en metricas via repo.guardar_metrica().
MySQL agrega en v_cluster; la API expone GET /api/cluster y GET /api/growth.

Este modulo NO recalcula en Python lo que ya hace MySQL: valida coherencia
y documenta indicadores N/A del enunciado.

    python -m servidor.consolidados          # reporte en terminal (demo)
    python -m servidor.probar_consolidados   # prueba automatica M2.5
"""
from __future__ import annotations

from typing import Any

from db import repositorio as repo

# Indicadores que el enunciado nombra pero no aplican a este cluster monolitico.
INDICADORES_NO_APLICA: dict[str, str] = {
    "overcommit": (
        "N/A — cada regional tiene su disco fisico; no hay capa de "
        "sobreasignacion de almacenamiento compartido."
    ),
    "fragmentacion": (
        "N/A — no hay volumen unificado ni sistema de archivos distribuido; "
        "cada nodo reporta su propio disco."
    ),
    "quorum": (
        "N/A — no hay decisiones por mayoria; el nodo central solo agrega "
        "metricas, no coordina replicas."
    ),
    "replication_health": (
        "N/A — los nodos no replican datos entre si; cada uno es independiente."
    ),
}


def suma_manual_desde_nodos(nodos: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """
    Replica la logica de v_cluster en Python para comprobar que cuadra.

    Solo suma nodos ACTIVOS con metrica (total_gb presente), igual que la vista.
    """
    if nodos is None:
        nodos = repo.listar_nodos()
    activos = [
        n for n in nodos
        if n.get("estado") == "ACTIVO" and n.get("total_gb") is not None
    ]
    cap = sum(float(n["total_gb"]) for n in activos)
    usado = sum(float(n["usado_gb"] or 0) for n in activos)
    libre = sum(float(n["libre_gb"] or 0) for n in activos)
    pct = round(usado / cap * 100, 2) if cap else None
    return {
        "nodos_totales": len(nodos),
        "nodos_activos": sum(1 for n in nodos if n.get("estado") == "ACTIVO"),
        "capacidad_total_gb": round(cap, 2),
        "usado_total_gb": round(usado, 2),
        "libre_total_gb": round(libre, 2),
        "uso_pct_global": pct,
    }


def verificar_coherencia_cluster(tolerancia: float = 0.02) -> tuple[bool, str]:
    """
    Compara v_cluster (MySQL) con la suma manual de listar_nodos.
    Devuelve (ok, detalle).
    """
    vista = repo.resumen_cluster()
    manual = suma_manual_desde_nodos()

    def _cerca(a: float | None, b: float | None) -> bool:
        if a is None and b is None:
            return True
        if a is None or b is None:
            return False
        return abs(float(a) - float(b)) <= tolerancia

    for c in ("nodos_totales", "nodos_activos"):
        if int(vista.get(c) or 0) != int(manual.get(c) or 0):
            return False, f"{c}: vista={vista.get(c)} manual={manual.get(c)}"

    for c in ("capacidad_total_gb", "usado_total_gb", "libre_total_gb"):
        if not _cerca(vista.get(c), manual.get(c)):
            return False, f"{c}: vista={vista.get(c)} manual={manual.get(c)}"

    usado = float(vista.get("usado_total_gb") or 0)
    cap = float(vista.get("capacidad_total_gb") or 0)
    pct_vista = float(vista.get("uso_pct_global") or 0)
    pct_esperado = round(usado / cap * 100, 2) if cap else 0
    if abs(pct_vista - pct_esperado) > 0.5:
        return False, f"uso_pct_global: {pct_vista}% vs esperado {pct_esperado}%"

    return True, "v_cluster cuadra con suma manual de nodos ACTIVOS"


def verificar_growth_desde_historico(node_id: str, horas: int = 1) -> tuple[bool, str]:
    """
    El growth rate debe salir del histórico (primer vs ultimo usado_gb),
    no del ultimo reporte aislado.
    """
    puntos = repo.historial(node_id, horas=horas, limite=500)
    if len(puntos) < 2:
        return False, f"{node_id}: menos de 2 puntos en historial"

    delta_manual = round(float(puntos[-1]["usado_gb"]) - float(puntos[0]["usado_gb"]), 2)
    filas = [g for g in repo.crecimiento(horas=horas) if g["node_id"] == node_id]
    if not filas:
        return False, f"{node_id}: crecimiento() no devolvio fila"

    g = filas[0]
    if g.get("horas_observadas", 0) <= 0:
        return False, f"{node_id}: horas_observadas=0 (no usa ventana temporal)"

    if abs(float(g["delta_gb"]) - delta_manual) > 0.15:
        return False, (
            f"{node_id}: delta_gb API={g['delta_gb']} vs historial={delta_manual}"
        )

    return True, (
        f"{node_id}: delta={g['delta_gb']} GB en {g['horas_observadas']} h "
        f"-> {g['growth_gb_dia']} GB/dia (desde historial, no ultimo dato)"
    )


def reporte_cluster(horas_growth: int = 24) -> None:
    """Imprime consolidados + growth + N/A para demo/defensa."""
    c = repo.resumen_cluster()
    print("--- Consolidados (v_cluster / GET /api/cluster) ---")
    print(f"  Nodos activos / totales : {c.get('nodos_activos')} / {c.get('nodos_totales')}")
    print(f"  Capacidad total         : {c.get('capacidad_total_gb')} GB")
    print(f"  Usado total             : {c.get('usado_total_gb')} GB")
    print(f"  Libre total             : {c.get('libre_total_gb')} GB")
    print(f"  Utilizacion global      : {c.get('uso_pct_global')}%")
    print(f"  Latencia ponderada      : {c.get('latencia_ponderada_ms')} ms")

    ok, det = verificar_coherencia_cluster()
    print(f"  Coherencia suma manual  : {'OK' if ok else 'FALLA'} ({det})")

    print(f"\n--- Growth rate (historico, GET /api/growth?horas={horas_growth}) ---")
    for g in repo.crecimiento(horas=horas_growth)[:9]:
        print(f"  {g['node_id']}: {g['growth_gb_dia']} GB/dia "
              f"(delta {g['delta_gb']} GB en {g['horas_observadas']} h)")

    print("\n--- Indicadores no aplicables (enunciado) ---")
    for nombre, justificacion in INDICADORES_NO_APLICA.items():
        print(f"  {nombre}: {justificacion}")


def main() -> None:
    reporte_cluster()


if __name__ == "__main__":
    main()
