"""
Prueba M2.6 — concurrencia con los nueve nodos.  Edwin.

    python -m servidor.probar_m26              # 10 minutos (defensa)
    python -m servidor.probar_m26 --rapido   # ~1 min (desarrollo)

Comprueba:
  - 9 nodos reportando en paralelo sin mezclar metricas entre nodos
  - Soak de N minutos sin perder reportes (carga sostenida)
  - Matar DOS clientes -> cronometrar NO_REPORTA (anotado en logs/)
  - Levantarlos de nuevo -> recuperacion automatica verificada

El archivo logs/m26_mediciones.txt queda listo para decir en voz alta en la defensa.

Usa prefijo CONC9- y borra al terminar.
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from comun import config                                          # noqa: E402
from db import repositorio as repo                                # noqa: E402
from db.conexion import cerrar_conexion_del_hilo, cursor          # noqa: E402

PREFIJO = "CONC9-"
INTERVALO = int(os.getenv("M26_INTERVALO", "2"))
PUERTO = int(os.getenv("PUERTO_PRUEBA_M26", "5193"))
FACTOR = int(os.getenv("FACTOR_TIMEOUT", str(config.FACTOR_TIMEOUT)))
PERIODO_WD = int(os.getenv("PERIODO_WATCHDOG_SEG", "1"))

ARCHIVO_MEDICIONES = RAIZ / "logs" / "m26_mediciones.txt"

fallos: list[str] = []
_procesos: list[subprocess.Popen] = []
_lineas_medicion: list[str] = []


def check(nombre: str, ok: bool, detalle: str = "") -> bool:
    print(f"  [{'OK  ' if ok else 'FALLA'}] {nombre}"
          + (f"  -> {detalle}" if detalle else ""))
    if not ok:
        fallos.append(nombre)
    return ok


def anotar(texto: str) -> None:
    print(f"      [medicion] {texto}")
    _lineas_medicion.append(texto)


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
        "FACTOR_TIMEOUT": str(FACTOR),
        "PERIODO_WATCHDOG_SEG": str(PERIODO_WD),
        "PERIODO_DESPACHADOR_SEG": "1",
        "PYTHONUNBUFFERED": "1",
    })
    e.update(extra)
    return e


def arrancar(cmd: list[str], **env: str) -> subprocess.Popen:
    p = subprocess.Popen(cmd, cwd=RAIZ, env=_entorno(**env),
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _procesos.append(p)
    return p


def matar_limpio(p: subprocess.Popen | None) -> None:
    if p is None or p.poll() is not None:
        return
    p.send_signal(signal.SIGINT)
    try:
        p.wait(timeout=8)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=5)


def matar_abrupto(p: subprocess.Popen) -> None:
    if p.poll() is None:
        p.kill()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


def proc_de(node_id: str) -> subprocess.Popen:
    return next(
        p for p in _procesos
        if p.poll() is None and node_id in " ".join(p.args))  # type: ignore[arg-type]
    )


def ids_prueba() -> list[str]:
    return [f"{PREFIJO}{nid}" for nid, _ in config.REGIONALES]


def estado_de(node_id: str) -> str | None:
    with cursor() as cur:
        cur.execute("SELECT estado FROM nodos WHERE node_id=%s", (node_id,))
        f = cur.fetchone()
    return f["estado"] if f else None


def metricas_de(node_id: str) -> int:
    with cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM metricas WHERE node_id=%s", (node_id,))
        return int(cur.fetchone()["c"])


def region_de(node_id: str) -> str | None:
    with cursor() as cur:
        cur.execute("SELECT region FROM nodos WHERE node_id=%s", (node_id,))
        f = cur.fetchone()
    return f["region"] if f else None


def hay_evento(node_id: str, tipo: str) -> bool:
    return any(e["tipo"] == tipo
               for e in repo.listar_eventos(limite=50, node_id=node_id))


def limpiar_bd() -> None:
    with cursor() as cur:
        cur.execute("DELETE FROM nodos WHERE node_id LIKE %s", (PREFIJO + "%",))


def verificar_sin_cruce(ids: list[str]) -> tuple[bool, str]:
    """Cada node_id debe conservar SU region; metricas solo bajo ese id."""
    for nid, (_, region) in zip(ids, config.REGIONALES):
        if region_de(nid) != region:
            return False, f"{nid} region={region_de(nid)!r} esperada {region!r}"
    with cursor() as cur:
        cur.execute(
            "SELECT COUNT(DISTINCT node_id) AS n FROM metricas "
            "WHERE node_id LIKE %s",
            (PREFIJO + "%",),
        )
        distintos = int(cur.fetchone()["n"])
    if distintos != len(ids):
        return False, f"solo {distintos}/{len(ids)} node_id tienen metricas"
    return True, f"{len(ids)} nodos aislados, 0 cruces de region"


def umbral_max_deteccion() -> float:
    return FACTOR * INTERVALO + PERIODO_WD + 2


def medir_caida(node_id: str) -> float | None:
    t0 = time.time()
    if not esperar(lambda: estado_de(node_id) == "NO_REPORTA", 45):
        return None
    return time.time() - t0


def medir_recuperacion(node_id: str) -> float | None:
    t0 = time.time()
    if not esperar(lambda: estado_de(node_id) == "ACTIVO", 45):
        return None
    return time.time() - t0


def guardar_mediciones(minutos: float, victimas: list[tuple[str, float | None]],
                       recuperaciones: list[tuple[str, float | None]]) -> None:
    config.asegurar_directorios()
    umbral = umbral_max_deteccion()
    lineas = [
        f"M2.6 Mediciones — {datetime.now().isoformat(timespec='seconds')}",
        f"Puerto {PUERTO}  ·  intervalo {INTERVALO}s  ·  factor {FACTOR}x  ·  "
        f"watchdog {PERIODO_WD}s",
        f"Umbral teorico max deteccion: ~{umbral:.0f} s "
        f"({FACTOR}x{INTERVALO}s + {PERIODO_WD}s ciclo + margen)",
        f"Soak concurrente: {minutos:.0f} min con 9 nodos",
        "",
    ]
    lineas.extend(_lineas_medicion)
    lineas.append("")
    lineas.append("--- Deteccion de caida (matar proceso, sin aviso) ---")
    for nid, seg in victimas:
        if seg is None:
            lineas.append(f"  {nid}: FALLO (no llego a NO_REPORTA)")
        else:
            lineas.append(
                f"  {nid}: NO_REPORTA en {seg:.1f} s "
                f"(teorico <= {umbral:.0f} s) — decir en defensa: "
                f"\"detectamos la caida en {seg:.0f} segundos\""
            )
    lineas.append("")
    lineas.append("--- Recuperacion automatica ---")
    for nid, seg in recuperaciones:
        if seg is None:
            lineas.append(f"  {nid}: FALLO (no volvio a ACTIVO)")
        else:
            lineas.append(f"  {nid}: ACTIVO en {seg:.1f} s tras relanzar cliente")
    lineas.append("")
    ARCHIVO_MEDICIONES.write_text("\n".join(lineas) + "\n", encoding="utf-8")
    print(f"\n  Mediciones guardadas en: {ARCHIVO_MEDICIONES}")


def main() -> int:
    p = argparse.ArgumentParser(description="Prueba M2.6 — 9 nodos concurrentes")
    p.add_argument("--minutos", type=float,
                   default=float(os.getenv("M26_MINUTOS", "10")),
                   help="Duracion del soak (default 10)")
    p.add_argument("--rapido", action="store_true",
                   help="Soak de 1 minuto (desarrollo)")
    args = p.parse_args()
    minutos = 1.0 if args.rapido else args.minutos
    umbral = umbral_max_deteccion()
    ids = ids_prueba()

    print("=" * 70)
    print(" PRUEBA M2.6 — Concurrencia con 9 nodos")
    print(f" puerto {PUERTO}  ·  soak {minutos:.0f} min  ·  intervalo {INTERVALO}s")
    print(f" umbral deteccion ~{umbral:.0f} s max")
    print("=" * 70)

    config.asegurar_directorios()
    limpiar_bd()

    victima_ids = [ids[2], ids[6]]
    mediciones_caida: list[tuple[str, float | None]] = []
    mediciones_recup: list[tuple[str, float | None]] = []

    try:
        print("\n1. Levantar servidor + 9 nodos casi a la vez")
        srv = arrancar([sys.executable, "-m", "servidor.main"])
        time.sleep(2.5)
        check("Servidor activo", srv.poll() is None)

        for nid, (_, reg) in zip(ids, config.REGIONALES):
            arrancar([sys.executable, "-m", "cliente.main",
                      "--node-id", nid, "--region", reg,
                      "--host", "127.0.0.1", "--puerto", str(PUERTO),
                      "--intervalo", str(INTERVALO)])
            time.sleep(0.05)

        check("9 nodos registrados",
              esperar(lambda: sum(1 for i in ids if estado_de(i)) == 9, 40))

        print(f"\n2. Soak {minutos:.0f} min — 9 nodos reportando (cero cruces)")
        min_esperadas = int(minutos * 60 / INTERVALO * 0.45)
        anotar(f"Minimo esperado ~{min_esperadas} metricas/nodo en {minutos:.0f} min")

        inicio = time.time()
        fin = inicio + minutos * 60
        muestra = 0
        while time.time() < fin:
            restante = fin - time.time()
            if restante <= 0:
                break
            time.sleep(min(30, restante))
            if srv.poll() is not None:
                check("Servidor sigue vivo durante soak", False, "proceso murio")
                break
            ok, det = verificar_sin_cruce(ids)
            if not ok:
                check(f"Sin mezcla de datos (muestra {muestra})", False, det)
            muestra += 1
            activos = sum(1 for i in ids if estado_de(i) == "ACTIVO")
            counts = [metricas_de(i) for i in ids]
            print(f"      … {int(time.time() - inicio)}s  activos={activos}/9  "
                  f"metricas min/med/max="
                  f"{min(counts)}/{sum(counts)//len(counts)}/{max(counts)}")

        ok, det = verificar_sin_cruce(ids)
        check("Cero datos cruzados tras el soak", ok, det)
        check("Todos con metricas acumuladas",
              all(metricas_de(i) >= min_esperadas for i in ids),
              f"min={min(metricas_de(i) for i in ids)} esperado>={min_esperadas}")
        anotar(f"Metricas finales: {[metricas_de(i) for i in ids]}")

        print("\n3. Matar DOS nodos y cronometrar NO REPORTA")
        for vid in victima_ids:
            antes = metricas_de(vid)
            matar_abrupto(proc_de(vid))
            t_caida = time.time()
            seg = medir_caida(vid)
            check(f"Watchdog detecta {vid}",
                  seg is not None, f"{seg:.1f} s" if seg else "timeout")
            if seg is not None:
                check(f"Deteccion {vid} dentro del umbral (~{umbral:.0f}s)",
                      seg <= umbral + 3, f"{seg:.1f}s")
                anotar(f"{vid}: NO_REPORTA en {seg:.1f}s (corte {t_caida:.0f})")
            check(f"Evento NO_REPORTA {vid}", hay_evento(vid, "NO_REPORTA"))
            mediciones_caida.append((vid, seg))
            # metricas del muerto no deben crecer
            time.sleep(INTERVALO + 1)
            check(f"{vid} dejo de reportar", metricas_de(vid) == antes)

        otros = [i for i in ids if i not in victima_ids]
        check("Los 7 restantes siguen ACTIVO",
              sum(1 for i in otros if estado_de(i) == "ACTIVO") >= 7)

        print("\n4. Recuperacion automatica (relanzar los dos)")
        for vid, (_, reg) in zip(victima_ids,
                                 [config.REGIONALES[2], config.REGIONALES[6]]):
            nid = vid
            arrancar([sys.executable, "-m", "cliente.main",
                      "--node-id", nid, "--region", reg,
                      "--host", "127.0.0.1", "--puerto", str(PUERTO),
                      "--intervalo", str(INTERVALO)])
            seg = medir_recuperacion(nid)
            check(f"{nid} vuelve a ACTIVO solo",
                  seg is not None, f"{seg:.1f}s" if seg else "timeout")
            check(f"Evento RECUPERADO {nid}",
                  esperar(lambda: hay_evento(nid, "RECUPERADO"), 20))
            if seg is not None:
                anotar(f"{nid}: RECUPERADO en {seg:.1f}s")
            mediciones_recup.append((nid, seg))

        check("Servidor intacto al final", srv.poll() is None)

    finally:
        print("\nLimpiando...")
        for proc in list(_procesos):
            matar_limpio(proc)
        try:
            limpiar_bd()
        except Exception as e:                                    # noqa: BLE001
            print(f"  AVISO: {e}")
        guardar_mediciones(minutos, mediciones_caida, mediciones_recup)
        cerrar_conexion_del_hilo()

    print("\n" + "=" * 70)
    if fallos:
        print(f" RESULTADO: {len(fallos)} fallas")
        for f in fallos:
            print(f"   - {f}")
        return 1
    print(" RESULTADO: M2.6 OK — 9 nodos, soak, 2 caidas medidas, recuperacion.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
