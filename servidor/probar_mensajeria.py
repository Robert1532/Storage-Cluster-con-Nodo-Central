"""
Prueba de mensajeria servidor -> cliente — M2.4 / requisito 7.1.  Edwin.

    python -m servidor.probar_mensajeria

Comprueba:
  - Unicast a un nodo (los 3 textos del enunciado)
  - Broadcast a todos los nodos conectados
  - Texto en archivo logs/cliente_<node_id>.log
  - ACK en mensajes con ack_en y emparejado por cmd_id

Usa prefijo MSG- y borra al terminar.
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
from servidor.mensajeria import (                                 # noqa: E402
    TEXTO_CONFIG,
    TEXTO_ESPACIO,
    TEXTO_REINICIE,
    encolar_a_nodo,
    encolar_broadcast,
)

PREFIJO = "MSG-"
PUERTO = int(os.getenv("PUERTO_PRUEBA_MSG", "5195"))
INTERVALO = 2
NODOS = [
    (f"{PREFIJO}A", "La Paz"),
    (f"{PREFIJO}B", "Cochabamba"),
    (f"{PREFIJO}C", "Santa Cruz"),
]

fallos: list[str] = []
_procesos: list[subprocess.Popen] = []


def check(nombre: str, ok: bool, detalle: str = "") -> bool:
    print(f"  [{'OK  ' if ok else 'FALLA'}] {nombre}"
          + (f"  -> {detalle}" if detalle else ""))
    if not ok:
        fallos.append(nombre)
    return ok


def esperar(condicion, segundos: float = 20.0, paso: float = 0.4) -> bool:
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


def log_de(node_id: str) -> str:
    ruta = config.DIR_LOGS / f"cliente_{node_id}.log"
    return ruta.read_text(encoding="utf-8") if ruta.exists() else ""


def esperar_ack(cmd_id: str) -> dict | None:
    def _listo() -> bool:
        m = repo.obtener_mensaje(cmd_id)
        return m is not None and m.get("estado") == "CONFIRMADO"

    if not esperar(_listo, 25):
        return None
    return repo.obtener_mensaje(cmd_id)


def main() -> int:
    print("=" * 70)
    print(" PRUEBA MENSAJERIA — M2.4 / requisito 7.1")
    print(f" puerto {PUERTO}  ·  nodos: {', '.join(n for n, _ in NODOS)}")
    print("=" * 70)

    config.asegurar_directorios()
    limpiar_bd()
    for nid, _ in NODOS:
        (config.DIR_LOGS / f"cliente_{nid}.log").unlink(missing_ok=True)

    try:
        print("\n1. Servidor + clientes conectados")
        srv = arrancar([sys.executable, "-m", "servidor.main"])
        time.sleep(2.5)
        check("Servidor activo", srv.poll() is None)
        for nid, reg in NODOS:
            arrancar([sys.executable, "-m", "cliente.main",
                      "--node-id", nid, "--region", reg,
                      "--host", "127.0.0.1", "--puerto", str(PUERTO),
                      "--intervalo", str(INTERVALO)])
            time.sleep(0.4)
        ids = [n for n, _ in NODOS]
        check("Nodos registrados",
              esperar(lambda: all(repo.existe_nodo(i) for i in ids), 25))

        principal = ids[0]
        print("\n2. Unicast — tres textos del enunciado a un nodo")
        textos = [TEXTO_REINICIE, TEXTO_ESPACIO, TEXTO_CONFIG]
        for texto in textos:
            cmd_id = encolar_a_nodo(principal, texto)
            fila = esperar_ack(cmd_id)
            check(f"ACK unicast: {texto!r}",
                  fila is not None and fila.get("ack_en") is not None,
                  f"cmd_id={cmd_id[:8]}… ack_en={fila.get('ack_en') if fila else None}")
            check(f"cmd_id empareja en BD ({texto[:20]}…)",
                  fila is not None and fila.get("cmd_id") == cmd_id)

        contenido = log_de(principal)
        for texto in textos:
            check(f"En .log del cliente: {texto!r}", texto in contenido)

        print("\n3. Broadcast — mismo texto a todos los nodos")
        cmd_ids = encolar_broadcast(TEXTO_ESPACIO)
        check(f"Broadcast encolo {len(ids)} mensajes", len(cmd_ids) == len(ids),
              f"{len(cmd_ids)} cmd_ids")
        confirmados = 0
        for cid in cmd_ids:
            fila = esperar_ack(cid)
            if fila and fila.get("ack_en"):
                confirmados += 1
        check("Todos los nodos confirmaron el broadcast",
              confirmados == len(ids), f"{confirmados}/{len(ids)} CONFIRMADO")
        for nid in ids:
            check(f".log de {nid} tiene el broadcast",
                  TEXTO_ESPACIO in log_de(nid))

        print("\n--- Comandos para demo en vivo ---")
        print("  Unicast (dashboard o terminal):")
        print(f"    python -m servidor.mensajeria --node-id {principal} "
              f'--texto "{TEXTO_REINICIE}"')
        print("  Broadcast a todos:")
        print(f'    python -m servidor.mensajeria --broadcast --texto "{TEXTO_ESPACIO}"')

    finally:
        print("\nLimpiando...")
        for p in list(_procesos):
            matar(p)
        try:
            limpiar_bd()
        except Exception as e:                                    # noqa: BLE001
            print(f"  AVISO: {e}")
        for nid, _ in NODOS:
            (config.DIR_LOGS / f"cliente_{nid}.log").unlink(missing_ok=True)
        cerrar_conexion_del_hilo()

    print("\n" + "=" * 70)
    if fallos:
        print(f" RESULTADO: {len(fallos)} fallas")
        for f in fallos:
            print(f"   - {f}")
        return 1
    print(" RESULTADO: mensajeria M2.4 OK — unicast, broadcast, log y ACK.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
