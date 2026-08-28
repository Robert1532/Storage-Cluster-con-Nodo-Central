"""
Encolar mensajes hacia clientes — M2.4 / requisito 7.1.  Edwin.

La API y este modulo NO hablan directo con los sockets: insertan filas
PENDIENTE en mensajes; el despachador de main.py las envia como CMD.

    python -m servidor.mensajeria --node-id CNS-LPZ-01 --texto "Reinicie servicio"
    python -m servidor.mensajeria --broadcast "Verifique espacio en disco"
    python -m servidor.mensajeria --node-id CNS-SCZ-03 --recursos disco,ram,cpu
    python -m servidor.mensajeria --node-id CNS-SCZ-03 --sync

Cada mensaje recibe un cmd_id unico (UUID). El ACK del cliente trae el mismo
cmd_id para emparejar la confirmacion con la fila en mensajes.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from comun import protocolo                                       # noqa: E402
from db import repositorio as repo                                # noqa: E402

# Textos minimos del enunciado (coinciden con dashboard/index.html).
TEXTO_REINICIE = "Reinicie servicio"
TEXTO_ESPACIO = "Verifique espacio en disco"
TEXTO_CONFIG = "Actualización de configuración"
TEXTOS_ENUNCIADO = (TEXTO_REINICIE, TEXTO_ESPACIO, TEXTO_CONFIG)


def encolar_a_nodo(node_id: str, texto: str) -> str:
    """
    Unicast: una fila PENDIENTE para un node_id.
    Devuelve cmd_id para seguir el ACK en mensajes / GET /api/messages.
    """
    return repo.crear_mensaje(node_id, protocolo.ACCION_MENSAJE, texto)


def encolar_broadcast(texto: str) -> list[str]:
    """
    Broadcast: una fila PENDIENTE por cada nodo registrado en la base.

    Cada uno lleva su propio cmd_id (no se reutiliza: dos ACK distintos).
    El despachador envia a los conectados y marca FALLIDO a los que no esten.
    """
    cmd_ids: list[str] = []
    for nodo in repo.listar_nodos():
        cmd_ids.append(encolar_a_nodo(nodo["node_id"], texto))
    return cmd_ids


def encolar_recursos(node_id: str, recursos: list[str]) -> str:
    """
    v2 — le dice a un nodo QUE debe medir, sin entrar a esa maquina.

    La lista viaja en el campo `texto` del CMD (separada por comas) porque el
    protocolo ya la transporta y no hacia falta un campo nuevo para una lista
    de cuatro palabras. Ademas se persiste en nodos.recursos_pedidos, para que
    el nodo la readopte sola cuando reconecte.
    """
    texto = ",".join(str(r).strip().lower() for r in recursos)
    repo.actualizar_recursos_pedidos(node_id, recursos)
    return repo.crear_mensaje(node_id, protocolo.ACCION_SET_RECURSOS, texto)


def encolar_sync(node_id: str) -> str:
    """v2 — pide a un nodo que mande ya lo que tenga guardado sin sincronizar."""
    return repo.crear_mensaje(node_id, protocolo.ACCION_SOLICITAR_SYNC, None)


def main() -> int:
    p = argparse.ArgumentParser(description="Encolar comandos a los nodos (M2.4)")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--node-id", help="Un solo nodo")
    g.add_argument("--broadcast", action="store_true", help="Todos los nodos")
    q = p.add_mutually_exclusive_group(required=True)
    q.add_argument("--texto", choices=list(TEXTOS_ENUNCIADO),
                   help="Texto del mensaje (uno de los tres del enunciado)")
    q.add_argument("--recursos",
                   help="v2: que debe medir el nodo (ej: disco,ram,cpu)")
    q.add_argument("--sync", action="store_true",
                   help="v2: pedirle que sincronice lo pendiente ahora")
    a = p.parse_args()

    if a.broadcast and not a.texto:
        print("ERROR: --recursos y --sync son por nodo, no broadcast",
              file=sys.stderr)
        return 1

    if a.broadcast:
        ids = encolar_broadcast(a.texto)
        print(f"Broadcast encolado a {len(ids)} nodos ({a.texto!r})")
        for cid in ids:
            print(f"  cmd_id={cid}")
    else:
        if not repo.existe_nodo(a.node_id):
            print(f"ERROR: no existe el nodo '{a.node_id}'", file=sys.stderr)
            return 1
        if a.recursos:
            cid = encolar_recursos(a.node_id, a.recursos.split(","))
            print(f"SET_RECURSOS para {a.node_id}: cmd_id={cid}")
        elif a.sync:
            cid = encolar_sync(a.node_id)
            print(f"SOLICITAR_SYNC para {a.node_id}: cmd_id={cid}")
        else:
            cid = encolar_a_nodo(a.node_id, a.texto)
            print(f"Encolado para {a.node_id}: cmd_id={cid}")
    print("El despachador envia en <= 1 s; el ACK queda en mensajes.ack_en")
    return 0


if __name__ == "__main__":
    sys.exit(main())
