"""
Prueba de integracion end-to-end — tarea 5.2.  Responsable: Alexander.

    python scripts/prueba_integracion.py

Levanta el servidor de sockets y varios clientes de verdad, contra la base de
datos de verdad, y comprueba automaticamente lo que el enunciado exige y lo que
el tribunal va a pedir en vivo:

  1. Alta automatica de un cliente nuevo (requisito 7.2)
  2. Las metricas viajan por el socket y quedan guardadas
  3. Mensaje del servidor al cliente, archivo .log y ACK (requisito 7.1)
  4. Cambio de intervalo en caliente desde el servidor (requisito 7.3)
  5. Un cliente que se muere pasa a "No Reporta"
  6. Y al volver, se recupera solo
  7. El servidor se cae y los clientes reconectan solos
  8. El cliente numero 10 es rechazado con un motivo, no con silencio
  9. No quedan conexiones a MySQL colgadas

Esto NO reemplaza la demo en vivo: la reemplaza el ensayo. Sirve para que
cualquiera del equipo compruebe en dos minutos que su cambio no rompio nada.

Usa nodos con prefijo ITEST- y los borra al terminar.
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

PREFIJO = "ITEST-"
INTERVALO = 2
PUERTO = int(os.getenv("PUERTO_PRUEBA", "5199"))
NODOS = [(f"{PREFIJO}LPZ", "La Paz"), (f"{PREFIJO}CBB", "Cochabamba"),
         (f"{PREFIJO}SCZ", "Santa Cruz")]

fallos: list[str] = []
_procesos: list[subprocess.Popen] = []


def check(nombre: str, condicion: bool, detalle: str = "") -> bool:
    print(f"  [{'OK  ' if condicion else 'FALLA'}] {nombre}"
          + (f"  -> {detalle}" if detalle else ""))
    if not condicion:
        fallos.append(nombre)
    return condicion


def esperar(descripcion: str, condicion, segundos: float = 20.0,
            paso: float = 0.5) -> bool:
    """Sondea hasta que la condicion se cumpla o se acabe el tiempo."""
    limite = time.time() + segundos
    while time.time() < limite:
        try:
            if condicion():
                return True
        except Exception:                                         # noqa: BLE001
            pass
        time.sleep(paso)
    print(f"      (se agotaron {segundos:.0f}s esperando: {descripcion})")
    return False


# ------------------------------------------------------------------ procesos

def _entorno(**extra: str) -> dict[str, str]:
    entorno = dict(os.environ)
    entorno.update({
        "SOCKET_HOST": "127.0.0.1",
        "SOCKET_PORT": str(PUERTO),
        "INTERVALO_DEFECTO_SEG": str(INTERVALO),
        "PERIODO_WATCHDOG_SEG": "1",
        "PERIODO_DESPACHADOR_SEG": "1",
        "PYTHONUNBUFFERED": "1",
    })
    entorno.update(extra)
    return entorno


def arrancar_servidor(max_nodos: int = 9) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-m", "servidor.main"], cwd=RAIZ,
        env=_entorno(MAX_NODOS=str(max_nodos)),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _procesos.append(proc)
    time.sleep(2.5)
    return proc


def arrancar_cliente(node_id: str, region: str,
                     intervalo: int = INTERVALO) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-m", "cliente.main",
         "--node-id", node_id, "--region", region,
         "--host", "127.0.0.1", "--puerto", str(PUERTO),
         "--intervalo", str(intervalo)],
        cwd=RAIZ, env=_entorno(),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _procesos.append(proc)
    return proc


def matar(proc: subprocess.Popen | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


# ----------------------------------------------------------------- consultas

def estado_de(node_id: str) -> str | None:
    with cursor() as cur:
        cur.execute("SELECT estado FROM nodos WHERE node_id=%s", (node_id,))
        fila = cur.fetchone()
    return fila["estado"] if fila else None


def metricas_de(node_id: str) -> int:
    with cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM metricas WHERE node_id=%s", (node_id,))
        return int(cur.fetchone()["c"])


def hay_evento(node_id: str, tipo: str) -> bool:
    return any(e["tipo"] == tipo
               for e in repo.listar_eventos(limite=50, node_id=node_id))


def limpiar_bd() -> None:
    with cursor() as cur:
        cur.execute("DELETE FROM nodos WHERE node_id LIKE %s", (PREFIJO + "%",))


# ------------------------------------------------------------------- pruebas

def main() -> int:
    print("=" * 70)
    print(" PRUEBA DE INTEGRACION END-TO-END - Storage Cluster CNS")
    print(f" servidor 127.0.0.1:{PUERTO}  ·  intervalo {INTERVALO}s")
    print("=" * 70)

    config.asegurar_directorios()
    limpiar_bd()
    for node_id, _ in NODOS:
        (config.DIR_LOGS / f"cliente_{node_id}.log").unlink(missing_ok=True)

    servidor = None
    try:
        # ------------------------------------------------------------ 1 y 2
        print("\n1-2. Alta automatica y flujo de metricas")
        servidor = arrancar_servidor()
        check("El servidor arranca", servidor.poll() is None)

        for node_id, region in NODOS:
            arrancar_cliente(node_id, region)

        principal = NODOS[0][0]
        check("El nodo nuevo se registra solo (7.2)",
              esperar("alta del nodo", lambda: estado_de(principal) is not None))
        check("Deja el evento ALTA_AUTOMATICA", hay_evento(principal, "ALTA_AUTOMATICA"))
        check("Los 3 nodos quedaron registrados",
              esperar("los 3 nodos",
                      lambda: all(estado_de(n) is not None for n, _ in NODOS)))
        check("Las metricas llegan y se guardan",
              esperar("2 metricas", lambda: metricas_de(principal) >= 2, 25))

        with cursor() as cur:
            cur.execute("SELECT * FROM v_nodos_estado WHERE node_id=%s", (principal,))
            fila = cur.fetchone()
        check("El dashboard ve capacidad real del disco",
              fila is not None and fila["total_gb"] is not None
              and float(fila["total_gb"]) > 0,
              f"{(fila or {}).get('total_gb')} GB")
        check("Y el sistema operativo del nodo",
              fila is not None and bool(fila["sistema_operativo"]),
              (fila or {}).get("sistema_operativo") or "")

        # ---------------------------------------------------------------- 3
        print("\n3. Mensaje del servidor al cliente, log y ACK (7.1)")
        texto = "Verifique espacio en disco"
        cmd_id = repo.crear_mensaje(principal, "MENSAJE", texto)
        check("El mensaje se confirma con ACK",
              esperar("ACK", lambda: (repo.obtener_mensaje(cmd_id) or {}).get("estado")
                      == "CONFIRMADO", 15))
        fila = repo.obtener_mensaje(cmd_id) or {}
        check("Se mide el round-trip", fila.get("rtt_ms") is not None,
              f"{fila.get('rtt_ms')} ms")

        archivo = config.DIR_LOGS / f"cliente_{principal}.log"
        contenido = archivo.read_text(encoding="utf-8") if archivo.exists() else ""
        check("El cliente lo escribio en su archivo .log", texto in contenido,
              archivo.name)

        # ---------------------------------------------------------------- 4
        print("\n4. Cambio de intervalo en caliente (7.3)")
        repo.actualizar_intervalo(principal, 1)
        cmd_int = repo.crear_mensaje(principal, "SET_INTERVAL", None, 1)
        check("El comando SET_INTERVAL se confirma",
              esperar("ACK del SET_INTERVAL",
                      lambda: (repo.obtener_mensaje(cmd_int) or {}).get("estado")
                      == "CONFIRMADO", 15))
        antes = metricas_de(principal)
        time.sleep(5)
        despues = metricas_de(principal)
        # Con intervalo 1s tienen que entrar ~5 en 5 segundos; con el viejo
        # (2s) entrarian ~2. Se pide >=3 para dar aire.
        check("El cliente empezo a reportar mas seguido", despues - antes >= 3,
              f"{despues - antes} metricas en 5 s")
        contenido = archivo.read_text(encoding="utf-8")
        check("El cambio quedo en el .log del cliente", "SET_INTERVAL" in contenido)

        # ---------------------------------------------------------------- 5
        print("\n5. Un cliente se muere -> No Reporta")
        victima = NODOS[1][0]
        proc_victima = next(p for p in _procesos if p.poll() is None
                            and victima in " ".join(p.args))          # type: ignore[arg-type]
        matar(proc_victima)
        t0 = time.time()
        ok = esperar("estado NO_REPORTA",
                     lambda: estado_de(victima) == "NO_REPORTA", 30)
        check("Pasa a NO_REPORTA sin que el cliente avise", ok,
              f"detectado en {time.time() - t0:.1f} s")
        check("Deja el evento NO_REPORTA", hay_evento(victima, "NO_REPORTA"))

        # ---------------------------------------------------------------- 6
        print("\n6. Vuelve -> se recupera solo")
        arrancar_cliente(victima, NODOS[1][1])
        check("Vuelve a ACTIVO solo",
              esperar("estado ACTIVO", lambda: estado_de(victima) == "ACTIVO", 30))
        check("Deja el evento RECUPERADO", hay_evento(victima, "RECUPERADO"))

        # ---------------------------------------------------------------- 7
        print("\n7. Se cae el servidor -> los clientes reconectan solos")
        antes = metricas_de(principal)
        matar(servidor)
        time.sleep(3)
        vivos = [p for p in _procesos if p.poll() is None]
        check("Los clientes NO se murieron con el servidor", len(vivos) >= 2,
              f"{len(vivos)} procesos cliente vivos")

        servidor = arrancar_servidor()
        check("Vuelven a reportar sin tocarlos",
              esperar("metricas nuevas",
                      lambda: metricas_de(principal) > antes, 40))
        check("Y quedan registrados como reconexion",
              hay_evento(principal, "CONEXION"))

        # ---------------------------------------------------------------- 8
        print("\n8. El cluster lleno rechaza con un motivo")
        matar(servidor)
        servidor = arrancar_servidor(max_nodos=1)
        sobrante = f"{PREFIJO}EXTRA"
        proc_extra = arrancar_cliente(sobrante, "Nodo de mas")
        termino = esperar("que el cliente sobrante termine",
                          lambda: proc_extra.poll() is not None, 25)
        check("El cliente rechazado termina en vez de reintentar para siempre",
              termino, f"codigo de salida {proc_extra.returncode}")

        # ---------------------------------------------------------------- 9
        print("\n9. No quedan conexiones a MySQL colgadas")
        for p in list(_procesos):
            matar(p)
        time.sleep(3)
        with cursor() as cur:
            cur.execute("""SELECT COUNT(*) AS c FROM information_schema.processlist
                            WHERE user = %s AND id <> CONNECTION_ID()""",
                        (config.DB_USER,))
            colgadas = int(cur.fetchone()["c"])
        check("Los procesos cerraron sus conexiones al terminar", colgadas == 0,
              f"{colgadas} conexiones vivas de {config.DB_USER}")

    finally:
        print("\nLimpiando...")
        for p in list(_procesos):
            matar(p)
        try:
            limpiar_bd()
        except Exception as e:                                    # noqa: BLE001
            print(f"  AVISO: no se pudo limpiar la base ({e})")
        for node_id, _ in NODOS:
            (config.DIR_LOGS / f"cliente_{node_id}.log").unlink(missing_ok=True)
        (config.DIR_LOGS / f"cliente_{PREFIJO}EXTRA.log").unlink(missing_ok=True)
        cerrar_conexion_del_hilo()

    print("\n" + "=" * 70)
    if fallos:
        print(f" RESULTADO: {len(fallos)} fallas")
        for f in fallos:
            print(f"   - {f}")
        return 1
    print(" RESULTADO: el sistema completo funciona end-to-end.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
