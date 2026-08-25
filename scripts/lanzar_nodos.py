"""
Levanta los 9 nodos regionales — tarea 1.6.  Responsable: Martin.

    python scripts/lanzar_nodos.py --host 192.168.1.100
    python scripts/lanzar_nodos.py --host 192.168.1.100 --cantidad 3

Ctrl+C baja los nueve. Tambien responde a SIGTERM, que es lo que le manda
start_demo.sh: sin eso, matar este proceso dejaria a los nueve clientes vivos
reintentando contra un servidor que ya no existe.

OJO: esto NO reemplaza a los dos clientes reales en maquinas distintas, que el
enunciado exige. Sirve para que el consolidado del dashboard se vea completo.
"""
from __future__ import annotations

import argparse
import signal
import subprocess
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from comun import config  # noqa: E402

_procesos: list[subprocess.Popen] = []
_bajando = False


def bajar(*_args) -> None:
    global _bajando
    if _bajando:
        return
    _bajando = True
    print("\nDeteniendo nodos...")
    for proc in _procesos:
        if proc.poll() is None:
            proc.terminate()
    for proc in _procesos:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    print("Listo.")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1", help="IP del servidor central")
    p.add_argument("--puerto", type=int, default=config.SOCKET_PORT)
    p.add_argument("--intervalo", type=int, default=config.INTERVALO_DEFECTO_SEG)
    p.add_argument("--cantidad", type=int, default=len(config.REGIONALES))
    a = p.parse_args()

    if not (1 <= a.cantidad <= len(config.REGIONALES)):
        p.error(f"--cantidad tiene que estar entre 1 y {len(config.REGIONALES)}; "
                f"solo hay {len(config.REGIONALES)} regionales definidas en "
                f"comun/config.py")

    signal.signal(signal.SIGTERM, bajar)
    signal.signal(signal.SIGINT, bajar)

    print(f"Levantando {a.cantidad} nodos contra {a.host}:{a.puerto}\n")
    for node_id, region in config.REGIONALES[: a.cantidad]:
        proc = subprocess.Popen(
            [sys.executable, "-m", "cliente.main",
             "--node-id", node_id, "--region", region,
             "--host", a.host, "--puerto", str(a.puerto),
             "--intervalo", str(a.intervalo)],
            cwd=RAIZ,
        )
        _procesos.append(proc)
        print(f"  {node_id:<14} {region:<14} pid {proc.pid}")
        time.sleep(0.4)          # escalonar el arranque

    print("\nCtrl+C para bajar todos.\n")
    try:
        while not _bajando:
            time.sleep(1)
            # Avisar si alguno se cayo, en vez de seguir diciendo que todo va bien.
            for proc, (node_id, _) in zip(_procesos, config.REGIONALES):
                if proc.poll() is not None and proc.returncode is not None:
                    print(f"  AVISO: {node_id} termino con codigo {proc.returncode}")
                    _procesos.remove(proc)
            if not _procesos:
                print("No queda ningun nodo en pie.")
                return
    except KeyboardInterrupt:
        pass
    finally:
        bajar()


if __name__ == "__main__":
    main()
