"""
Unir ESTA computadora al cluster como un servidor regional — v2.

    python scripts/unirse.py --host 192.168.1.100

Eso es todo. El script pregunta a que departamento y sede pertenece esta
maquina, arma un node_id que no choca con ninguno, y arranca el cliente.

Sin preguntas (para un guion o para la demo):

    python scripts/unirse.py --host 192.168.1.100 --region "La Paz" --sede "El Alto"
    python scripts/unirse.py --host 192.168.1.100 --region Beni --node-id CNS-BEN-08

POR QUE EXISTE ESTE SCRIPT
--------------------------
Para unirse al cluster no hace falta nada del otro lado: el servidor da de alta
al nodo solo cuando lo ve por primera vez (requisito 7.2). Nadie tiene que
editar una lista, ni reiniciar el servidor, ni tocar la base de datos.

Lo unico que hace falta es que la maquina nueva sepa DOS cosas: la IP del
servidor central y quien dice ser. Este script se encarga de la segunda, que es
la que se presta a errores — un node_id repetido hace que dos maquinas
escriban en la misma fila.

QUE ES CADA COSA
----------------
  departamento (region)  una de las nueve administraciones regionales de la
                         CNS. Es lo que se suma en el consolidado.
  sede                   la oficina concreta. Un departamento puede tener
                         varias: La Paz atiende desde la ciudad de La Paz y
                         desde El Alto, con un servidor en cada una.
  node_id                el nombre unico de ESTA maquina. Dos maquinas nunca
                         pueden compartirlo.

Cada computadora que corre esto es UN nodo: un servidor de archivos regional.
"""
from __future__ import annotations

import argparse
import platform
import socket
import subprocess
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from comun import config  # noqa: E402

# Las nueve administraciones regionales, con su abreviatura de tres letras.
DEPARTAMENTOS = [
    ("La Paz", "LPZ"),
    ("Cochabamba", "CBB"),
    ("Santa Cruz", "SCZ"),
    ("Oruro", "ORU"),
    ("Potosi", "PTS"),
    ("Chuquisaca", "CHU"),
    ("Tarija", "TJA"),
    ("Beni", "BEN"),
    ("Pando", "PAN"),
]
ABREVIATURA = dict(DEPARTAMENTOS)


def ip_local() -> str:
    """
    IP de esta maquina en la LAN.

    Se abre un socket UDP hacia una direccion externa y se le pregunta al
    sistema que interfaz habria usado. No se manda ni un byte: UDP no conecta.
    Es la unica forma portable de saber cual de las IP de la maquina es la que
    ve el resto de la red — gethostbyname(gethostname()) devuelve 127.0.0.1 en
    media Linux.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def elegir_departamento() -> str:
    print("\n  A que administracion regional pertenece esta computadora?\n")
    for i, (nombre, _) in enumerate(DEPARTAMENTOS, 1):
        print(f"    {i}. {nombre}")
    while True:
        try:
            eleccion = input("\n  Numero (1-9): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Cancelado.")
            raise SystemExit(1)
        if eleccion.isdigit() and 1 <= int(eleccion) <= len(DEPARTAMENTOS):
            return DEPARTAMENTOS[int(eleccion) - 1][0]
        print("  Elegi un numero del 1 al 9.")


def preguntar(texto: str, defecto: str) -> str:
    try:
        dado = input(f"  {texto} [{defecto}]: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\n  Cancelado.")
        raise SystemExit(1)
    return dado or defecto


def node_id_sugerido(region: str, sede: str) -> str:
    """
    Un node_id que no choque con los de la demo.

    Se arma con la abreviatura del departamento y las dos ultimas cifras de la
    IP de esta maquina. Dos computadoras distintas en la misma LAN tienen IP
    distinta, asi que no pueden generar el mismo. Si aun asi coincidiera con
    uno de la lista de la demo, se le agrega una letra.
    """
    abrev = ABREVIATURA.get(region, (region[:3] or "CNS").upper())
    if sede and sede.strip().lower() != region.strip().lower():
        # Sede propia (El Alto): se usan sus tres primeras letras, asi el id
        # dice de que oficina es sin tener que abrir la base.
        abrev = "".join(c for c in sede.upper() if c.isalpha())[:3] or abrev
    sufijo = ip_local().rsplit(".", 1)[-1].rjust(2, "0")[-2:]
    propuesto = f"CNS-{abrev}-{sufijo}"
    usados = {n for n, _ in config.REGIONALES}
    letra = ord("A")
    while propuesto in usados:
        propuesto = f"CNS-{abrev}-{sufijo}{chr(letra)}"
        letra += 1
    return propuesto


def main() -> int:
    p = argparse.ArgumentParser(
        description="Unir esta computadora al cluster como servidor regional")
    p.add_argument("--host", help="IP del servidor central (el nodo de monitoreo)")
    p.add_argument("--puerto", type=int, default=config.SOCKET_PORT)
    p.add_argument("--region", help="departamento; si falta, se pregunta")
    p.add_argument("--sede", help="oficina concreta (ej: El Alto)")
    p.add_argument("--node-id", help="nombre unico; si falta, se sugiere uno")
    p.add_argument("--intervalo", type=int, default=config.INTERVALO_DEFECTO_SEG)
    p.add_argument("--recursos", default=",".join(config.RECURSOS_DEFECTO))
    p.add_argument("--si", action="store_true",
                   help="no preguntar nada, aceptar todos los valores")
    a = p.parse_args()

    print("=" * 66)
    print(" UNIR ESTA COMPUTADORA AL STORAGE CLUSTER CNS")
    print("=" * 66)
    print(f"  Esta maquina : {platform.node()}  ({platform.system()} "
          f"{platform.release()})")
    print(f"  Su IP en la LAN: {ip_local()}")

    host = a.host
    if not host:
        if a.si:
            p.error("--host es obligatorio con --si")
        print("\n  La IP del servidor central es la de la computadora donde")
        print("  corre 'python -m servidor.main'.")
        host = preguntar("IP del servidor central", "127.0.0.1")

    region = a.region or (DEPARTAMENTOS[0][0] if a.si else elegir_departamento())

    sede = a.sede
    if not sede:
        if a.si:
            sede = region
        else:
            print("\n  La SEDE es la oficina concreta. Un departamento puede tener")
            print("  varias: La Paz atiende desde La Paz y desde El Alto, con un")
            print("  servidor en cada una. Si hay una sola, dejalo como esta.")
            sede = preguntar("Sede", region)

    node_id = a.node_id
    if not node_id:
        sugerido = node_id_sugerido(region, sede)
        node_id = sugerido if a.si else preguntar("\n  Nombre unico de este nodo",
                                                  sugerido)

    print("\n" + "-" * 66)
    print(f"  Nodo         : {node_id}")
    print(f"  Departamento : {region}")
    print(f"  Sede         : {sede}")
    print(f"  Servidor     : {host}:{a.puerto}")
    print(f"  Reporta cada : {a.intervalo} s")
    print(f"  Mide         : {a.recursos}")
    print("-" * 66)
    print("\n  Este nodo se da de alta SOLO la primera vez que se conecta")
    print("  (requisito 7.2): nadie tiene que registrarlo del otro lado.")
    print("  Ctrl+C para desconectarlo. Lo que mida mientras no haya red")
    print("  queda guardado y se envia al reconectar.\n")

    if not a.si:
        try:
            if input("  Arrancar? [S/n]: ").strip().lower() in ("n", "no"):
                print("  Cancelado.")
                return 1
        except (EOFError, KeyboardInterrupt):
            print("\n  Cancelado.")
            return 1

    orden = [sys.executable, "-m", "cliente.main",
             "--node-id", node_id, "--region", region, "--sede", sede,
             "--host", host, "--puerto", str(a.puerto),
             "--intervalo", str(a.intervalo), "--recursos", a.recursos]
    print()
    # Se reemplaza este proceso por el del cliente en vez de lanzarlo aparte:
    # asi Ctrl+C llega directo al cliente y no queda un proceso padre inutil.
    try:
        return subprocess.call(orden, cwd=str(RAIZ))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
