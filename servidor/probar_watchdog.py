"""
Prueba del watchdog — M2.3 / estado No Reporta.  Edwin.

    python -m servidor.probar_watchdog

Comprueba con procesos reales (servidor + cliente):

  1. Matar un cliente lo pone en NO_REPORTA dentro del umbral esperado
  2. Levantarlo de nuevo lo devuelve a ACTIVO sin tocar nada
  3. Ambos cambios quedan en eventos con timestamp
  4. failover_events sube en v_nodos_estado

Umbral teorico maximo:
    FACTOR_TIMEOUT x intervalo_seg + PERIODO_WATCHDOG_SEG + margen

Usa prefijo WD- y borra al terminar.
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

PREFIJO = "WD-"
NODE_ID = f"{PREFIJO}LPZ-01"
REGION = "La Paz (prueba watchdog)"
PUERTO = int(os.getenv("PUERTO_PRUEBA_WD", "5196"))
INTERVALO = 2
FACTOR = 3
PERIODO_WD = 1

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
        "FACTOR_TIMEOUT": str(FACTOR),
        "PERIODO_WATCHDOG_SEG": str(PERIODO_WD),
        "PERIODO_DESPACHADOR_SEG": "1",
        "PYTHONUNBUFFERED": "1",
    })
    e.update(extra)
    return e


def arrancar(cmd: list[str]) -> subprocess.Popen:
    p = subprocess.Popen(cmd, cwd=RAIZ, env=_entorno(),
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _procesos.append(p)
    return p


def matar_abrupto(p: subprocess.Popen) -> None:
    """Simula cable desconectado: el nodo no avisa que se murio."""
    if p.poll() is None:
        p.kill()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


def matar_limpio(p: subprocess.Popen | None) -> None:
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


def estado_de(node_id: str) -> str | None:
    with cursor() as cur:
        cur.execute("SELECT estado FROM nodos WHERE node_id=%s", (node_id,))
        f = cur.fetchone()
    return f["estado"] if f else None


def eventos_de(node_id: str) -> list[dict]:
    return repo.listar_eventos(limite=50, node_id=node_id)


def failover_de(node_id: str) -> int:
    for n in repo.listar_nodos():
        if n["node_id"] == node_id:
            return int(n.get("failover_events") or 0)
    return 0


def metricas_de(node_id: str) -> int:
    with cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM metricas WHERE node_id=%s", (node_id,))
        return int(cur.fetchone()["c"])


def main() -> int:
    umbral_max = FACTOR * INTERVALO + PERIODO_WD + 3
    print("=" * 70)
    print(" PRUEBA WATCHDOG — M2.3 / estado No Reporta")
    print(f" puerto {PUERTO}  ·  intervalo {INTERVALO}s  ·  factor {FACTOR}x")
    print(f" watchdog cada {PERIODO_WD}s  ·  umbral max esperado ~{umbral_max}s")
    print("=" * 70)

    config.asegurar_directorios()
    limpiar_bd()

    servidor = None
    cliente = None
    try:
        print("\n1. Servidor + cliente reportando")
        servidor = arrancar([sys.executable, "-m", "servidor.main"])
        time.sleep(2.5)
        check("Servidor activo", servidor.poll() is None)

        cliente = arrancar([sys.executable, "-m", "cliente.main",
                            "--node-id", NODE_ID, "--region", REGION,
                            "--host", "127.0.0.1", "--puerto", str(PUERTO),
                            "--intervalo", str(INTERVALO)])
        check("Cliente registrado y ACTIVO",
              esperar(lambda: estado_de(NODE_ID) == "ACTIVO", 25),
              estado_de(NODE_ID) or "")
        check("Ya reporta metricas",
              esperar(lambda: metricas_de(NODE_ID) >= 2, 25),
              f"{metricas_de(NODE_ID)} filas")

        print("\n2. Corte abrupto -> NO REPORTA (sin aviso del cliente)")
        t_antes_caida = time.time()
        matar_abrupto(cliente)
        cliente = None

        t0 = time.time()
        ok_caida = esperar(lambda: estado_de(NODE_ID) == "NO_REPORTA", 35)
        detectado = time.time() - t0
        check("Pasa a NO_REPORTA sin que el cliente avise", ok_caida,
              f"detectado en {detectado:.1f}s")
        check(f"Dentro del umbral (~{umbral_max}s)",
              detectado <= umbral_max,
              f"{detectado:.1f}s <= {umbral_max}s")

        ev_caida = [e for e in eventos_de(NODE_ID) if e["tipo"] == "NO_REPORTA"]
        check("Evento NO_REPORTA en tabla eventos",
              len(ev_caida) >= 1, f"{len(ev_caida)} evento(s)")
        if ev_caida:
            ts = ev_caida[-1]["timestamp"]
            check("Evento NO_REPORTA tiene timestamp",
                  ts is not None, str(ts))
            check("Timestamp posterior al corte",
                  ts.timestamp() >= t_antes_caida - 1 if hasattr(ts, "timestamp")
                  else True, str(ts))

        failovers_tras_caida = failover_de(NODE_ID)
        check("failover_events >= 1 tras la caida",
              failovers_tras_caida >= 1, str(failovers_tras_caida))

        print("\n3. Volver a levantar -> ACTIVO solo (watchdog RECUPERADO)")
        cliente = arrancar([sys.executable, "-m", "cliente.main",
                            "--node-id", NODE_ID, "--region", REGION,
                            "--host", "127.0.0.1", "--puerto", str(PUERTO),
                            "--intervalo", str(INTERVALO)])
        ok_rec = esperar(lambda: estado_de(NODE_ID) == "ACTIVO", 35)
        check("Vuelve a ACTIVO sin intervencion manual", ok_rec,
              estado_de(NODE_ID) or "")

        ev_rec = [e for e in eventos_de(NODE_ID) if e["tipo"] == "RECUPERADO"]
        check("Evento RECUPERADO en tabla eventos",
              len(ev_rec) >= 1, f"{len(ev_rec)} evento(s)")
        if ev_rec:
            check("Evento RECUPERADO tiene timestamp",
                  ev_rec[-1]["timestamp"] is not None,
                  str(ev_rec[-1]["timestamp"]))

        check("Sigue reportando metricas tras recuperarse",
              esperar(lambda: metricas_de(NODE_ID) >= 4, 25),
              f"{metricas_de(NODE_ID)} filas totales")

        print("\n--- Demo en vivo (matar cliente y observar dashboard) ---")
        print(f"  Intervalo {INTERVALO}s  ·  umbral ~{FACTOR}x = "
              f"{FACTOR * INTERVALO}s  ·  pill roja NO REPORTA en ~{umbral_max}s")
        print("  GET /api/events?node_id=" + NODE_ID)

    finally:
        print("\nLimpiando...")
        matar_limpio(cliente)
        for p in list(_procesos):
            matar_limpio(p)
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
    print(" RESULTADO: watchdog M2.3 OK — NO_REPORTA, RECUPERADO, eventos con hora.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
