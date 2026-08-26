"""
Prueba del alta automatica en caliente — M2.2 / requisito 7.2.  Edwin.

    python -m servidor.probar_alta_automatica

Simula lo que el docente pide en vivo: el servidor YA esta corriendo con otros
nodos, llega un cliente con un node_id que NUNCA existio, y debe:

  - Insertarse solo en la tabla nodos (estado ACTIVO)
  - Dejar el evento ALTA_AUTOMATICA
  - Aparecer en GET /api/nodes (lo que ve el dashboard)
  - Responder HELLO_OK con nuevo=true al cliente

No reinicia el servidor ni toca archivos de configuracion.

Usa prefijo ALTA- y borra al terminar.
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

PREFIJO = "ALTA-"
PUERTO = int(os.getenv("PUERTO_PRUEBA_ALTA", "5197"))
INTERVALO = 3
NODO_NUEVO = f"{PREFIJO}DEMO-01"
REGION = "Regional Demo En Vivo"
# Dos nodos “de fondo” para demostrar que el alta es EN CALIENTE, no al arrancar.
NODOS_FONDO = [(f"{PREFIJO}FONDO-A", "Fondo A"), (f"{PREFIJO}FONDO-B", "Fondo B")]

fallos: list[str] = []
_procesos: list[subprocess.Popen] = []


def check(nombre: str, ok: bool, detalle: str = "") -> bool:
    print(f"  [{'OK  ' if ok else 'FALLA'}] {nombre}"
          + (f"  -> {detalle}" if detalle else ""))
    if not ok:
        fallos.append(nombre)
    return ok


def esperar(condicion, segundos: float = 25.0, paso: float = 0.5) -> bool:
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
        "PERIODO_WATCHDOG_SEG": "2",
        "PERIODO_DESPACHADOR_SEG": "1",
        "PYTHONUNBUFFERED": "1",
    })
    e.update(extra)
    return e


def arrancar(cmd: list[str], **env_extra: str) -> subprocess.Popen:
    p = subprocess.Popen(cmd, cwd=RAIZ, env=_entorno(**env_extra),
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


def fila_nodo(node_id: str) -> dict | None:
    with cursor() as cur:
        cur.execute(
            "SELECT node_id, region, estado, primer_registro FROM nodos "
            "WHERE node_id=%s",
            (node_id,),
        )
        return cur.fetchone()


def hay_alta_automatica(node_id: str) -> bool:
    return any(
        e["tipo"] == "ALTA_AUTOMATICA"
        for e in repo.listar_eventos(limite=20, node_id=node_id)
    )


def visible_en_api(node_id: str) -> bool:
    return any(n["node_id"] == node_id for n in repo.listar_nodos())


def main() -> int:
    print("=" * 70)
    print(" PRUEBA ALTA AUTOMATICA EN CALIENTE — M2.2 / requisito 7.2")
    print(f" puerto {PUERTO}  ·  nodo nuevo: {NODO_NUEVO}")
    print("=" * 70)

    config.asegurar_directorios()
    limpiar_bd()

    servidor = None
    try:
        print("\n1. Servidor arriba con nodos de fondo (sistema ya corriendo)")
        servidor = arrancar([sys.executable, "-m", "servidor.main"])
        time.sleep(2.5)
        check("Servidor activo", servidor.poll() is None)

        for nid, reg in NODOS_FONDO:
            arrancar([sys.executable, "-m", "cliente.main",
                      "--node-id", nid, "--region", reg,
                      "--host", "127.0.0.1", "--puerto", str(PUERTO),
                      "--intervalo", str(INTERVALO)])
            time.sleep(0.5)

        check("Nodos de fondo registrados",
              esperar(lambda: all(fila_nodo(n) is not None for n, _ in NODOS_FONDO)),
              f"{len(NODOS_FONDO)} nodos")

        check("El nodo nuevo NO existe antes del HELLO",
              fila_nodo(NODO_NUEVO) is None)

        print("\n2. Conectar nodo nuevo EN CALIENTE (sin reiniciar servidor)")
        proc_nuevo = arrancar([sys.executable, "-m", "cliente.main",
                               "--node-id", NODO_NUEVO, "--region", REGION,
                               "--host", "127.0.0.1", "--puerto", str(PUERTO),
                               "--intervalo", str(INTERVALO)])

        check("Fila nueva en tabla nodos",
              esperar(lambda: fila_nodo(NODO_NUEVO) is not None),
              NODO_NUEVO)

        fila = fila_nodo(NODO_NUEVO) or {}
        check("Estado ACTIVO desde el alta",
              fila.get("estado") == "ACTIVO", fila.get("estado") or "")
        check("Region guardada correctamente",
              fila.get("region") == REGION, fila.get("region") or "")
        check("primer_registro documenta cuando se dio de alta",
              fila.get("primer_registro") is not None,
              str(fila.get("primer_registro")))

        check("Evento ALTA_AUTOMATICA en bitacora",
              esperar(lambda: hay_alta_automatica(NODO_NUEVO)),
              "tabla eventos")

        check("Visible en API / dashboard (listar_nodos)",
              esperar(lambda: visible_en_api(NODO_NUEVO)),
              "GET /api/nodes")

        check("El cliente sigue conectado tras el alta",
              proc_nuevo.poll() is None, f"pid {proc_nuevo.pid}")

        print("\n3. El nodo nuevo ya reporta metricas")
        check("Metricas guardadas para el nodo nuevo",
              esperar(lambda: repo.listar_nodos() and any(
                  n["node_id"] == NODO_NUEVO and n.get("total_gb") is not None
                  for n in repo.listar_nodos()), 30),
              "ultima metrica en v_nodos_estado")

        print("\n--- Comando para repetir en la demo en vivo ---")
        print(f"  python -m cliente.main --node-id {NODO_NUEVO} "
              f'--region "{REGION}" --host <IP_SERVIDOR>')

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
    print(" RESULTADO: alta automatica en caliente OK (7.2).")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
