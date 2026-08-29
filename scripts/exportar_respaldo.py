"""
Exporta evidencia del cluster para el Plan B (tarea 5.4).  Responsable: Alexander.

    python scripts/exportar_respaldo.py

Corre esto DURANTE la demo real (con el sistema arriba y los 9 nodos
reportando), no después. Junta en una sola carpeta con fecha y hora:

    respaldo_demo/<fecha_hora>/
        nodos.json        estado y ultima metrica de cada nodo
        cluster.json       totales y KPIs consolidados
        eventos.json        bitacora (altas, caidas, recuperaciones)
        mensajes.json       mensajeria con sus ACK
        logs/                copia de logs/cliente_<id>.log tal como estaban

Esta carpeta es la evidencia de que el sistema corrio con datos reales, por
si el dia de la defensa falla el proyector, la red del aula o una maquina no
enciende: se muestra esto en vez de (o ademas de) el video de respaldo.
"""
from __future__ import annotations

import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from comun import config          # noqa: E402
from db import repositorio as repo  # noqa: E402
from db.conexion import probar_conexion  # noqa: E402


def _guardar_json(ruta: Path, datos) -> None:
    ruta.write_text(
        json.dumps(datos, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )


def main() -> None:
    if not probar_conexion():
        print("Sin conexion a MySQL. Corre esto con el sistema arriba.")
        sys.exit(1)

    marca = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    destino = RAIZ / "respaldo_demo" / marca
    destino.mkdir(parents=True, exist_ok=True)

    print(f"Exportando evidencia a {destino}")

    nodos = repo.listar_nodos()
    _guardar_json(destino / "nodos.json", nodos)
    print(f"  nodos.json       ({len(nodos)} nodos)")

    cluster = repo.resumen_cluster()
    _guardar_json(destino / "cluster.json", cluster)
    print("  cluster.json")

    eventos = repo.listar_eventos(limite=500)
    _guardar_json(destino / "eventos.json", eventos)
    print(f"  eventos.json     ({len(eventos)} eventos)")

    mensajes = repo.listar_mensajes(limite=500)
    _guardar_json(destino / "mensajes.json", mensajes)
    print(f"  mensajes.json    ({len(mensajes)} mensajes)")

    if config.DIR_LOGS.exists():
        destino_logs = destino / "logs"
        shutil.copytree(config.DIR_LOGS, destino_logs, dirs_exist_ok=True)
        cuenta = len(list(destino_logs.glob("*.log")))
        print(f"  logs/            ({cuenta} archivos .log)")

    print("\nListo. Guarda esta carpeta en pendrive y en la nube junto al video.")


if __name__ == "__main__":
    main()
