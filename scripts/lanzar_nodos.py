"""
Levanta los 9 nodos regionales — tarea 1.6.  Responsable: Martin.

    python scripts/lanzar_nodos.py --host 192.168.1.100
    python scripts/lanzar_nodos.py --host 192.168.1.100 --cantidad 3

Ctrl+C baja los nueve.

OJO: esto NO reemplaza a los dos clientes reales en maquinas distintas, que el
enunciado exige. Sirve para que el consolidado del dashboard se vea completo.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from comun import config  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1", help="IP del servidor central")
    p.add_argument("--puerto", type=int, default=config.SOCKET_PORT)
    p.add_argument("--intervalo", type=int, default=config.INTERVALO_DEFECTO_SEG)
    p.add_argument("--cantidad", type=int, default=9)
    a = p.parse_args()

    procesos: list[subprocess.Popen] = []
    print(f"Levantando {a.cantidad} nodos contra {a.host}:{a.puerto}\n")

    for node_id, region in config.REGIONALES[: a.cantidad]:
        proc = subprocess.Popen(
            [sys.executable, "-m", "cliente.main",
             "--node-id", node_id, "--region", region,
             "--host", a.host, "--puerto", str(a.puerto),
             "--intervalo", str(a.intervalo)],
            cwd=RAIZ,
        )
        procesos.append(proc)
        print(f"  {node_id:<14} {region:<14} pid {proc.pid}")
        time.sleep(0.4)          # escalonar el arranque

    print("\nCtrl+C para bajar todos.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDeteniendo nodos...")
        for proc in procesos:
            proc.terminate()
        for proc in procesos:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        print("Listo.")


if __name__ == "__main__":
    main()
