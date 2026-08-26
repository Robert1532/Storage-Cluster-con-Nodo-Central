"""
Prueba de concurrencia del servidor central — smoke test rapido.  Edwin.

    python -m servidor.probar_concurrencia     # ~2 min, varias comprobaciones
    python -m servidor.probar_m26                # M2.6 completo: 10 min + 2 caidas
    python -m servidor.probar_m26 --rapido       # M2.6 abreviado (~1 min soak)

Para la defensa use probar_m26 (10 min, cronometra dos caidas, guarda
logs/m26_mediciones.txt con el numero exacto a decir en voz alta).
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

PREFIJO = "SRVTEST-"
INTERVALO = 2
PUERTO = int(os.getenv("PUERTO_PRUEBA_SERVIDOR", "5198"))
FACTOR = 3
PERIODO_WD = 1

fallos: list[str] = []
_procesos: list[subprocess.Popen] = []


def check(nombre: str, condicion: bool, detalle: str = "") -> bool:
    print(f"  [{'OK  ' if condicion else 'FALLA'}] {nombre}"
          + (f"  -> {detalle}" if detalle else ""))
    if not condicion:
        fallos.append(nombre)
    return condicion


def esperar(descripcion: str, condicion, segundos: float = 30.0,
            paso: float = 0.5) -> bool:
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


def _entorno(**extra: str) -> dict[str, str]:
    entorno = dict(os.environ)
    entorno.update({
        "SOCKET_HOST": "127.0.0.1",
        "SOCKET_PORT": str(PUERTO),
        "INTERVALO_DEFECTO_SEG": str(INTERVALO),
        "FACTOR_TIMEOUT": str(FACTOR),
        "PERIODO_WATCHDOG_SEG": str(PERIODO_WD),
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


def arrancar_cliente(node_id: str, region: str) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-m", "cliente.main",
         "--node-id", node_id, "--region", region,
         "--host", "127.0.0.1", "--puerto", str(PUERTO),
         "--intervalo", str(INTERVALO)],
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


def matar_abrupto(proc: subprocess.Popen) -> None:
    """Simula cable desenchufado: kill sin SIGINT, sin cierre ordenado."""
    if proc.poll() is None:
        proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


def ids_prueba() -> list[str]:
    return [f"{PREFIJO}{nid}" for nid, _ in config.REGIONALES]


def estado_de(node_id: str) -> str | None:
    with cursor() as cur:
        cur.execute("SELECT estado FROM nodos WHERE node_id=%s", (node_id,))
        fila = cur.fetchone()
    return fila["estado"] if fila else None


def metricas_de(node_id: str) -> int:
    with cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM metricas WHERE node_id=%s", (node_id,))
        return int(cur.fetchone()["c"])


def region_de(node_id: str) -> str | None:
    with cursor() as cur:
        cur.execute("SELECT region FROM nodos WHERE node_id=%s", (node_id,))
        fila = cur.fetchone()
    return fila["region"] if fila else None


def hay_evento(node_id: str, tipo: str) -> bool:
    return any(e["tipo"] == tipo
               for e in repo.listar_eventos(limite=100, node_id=node_id))


def limpiar_bd() -> None:
    with cursor() as cur:
        cur.execute("DELETE FROM nodos WHERE node_id LIKE %s", (PREFIJO + "%",))


def main() -> int:
    print("=" * 70)
    print(" PRUEBA DE CONCURRENCIA — Servidor Central (Edwin)")
    print(f" servidor 127.0.0.1:{PUERTO}  ·  intervalo {INTERVALO}s  ·  "
          f"watchdog cada {PERIODO_WD}s")
    print("=" * 70)

    config.asegurar_directorios()
    limpiar_bd()

    servidor = None
    try:
        # ------------------------------------------------------------ 1 y 2
        print("\n1. Nueve nodos en paralelo (tarea 2.1)")
        servidor = arrancar_servidor()
        check("El servidor arranca", servidor.poll() is None)

        for node_id, region in config.REGIONALES:
            nid = f"{PREFIJO}{node_id}"
            arrancar_cliente(nid, region)
            time.sleep(0.15)          # escalonar un poco el arranque

        ids = ids_prueba()
        check("Los 9 nodos quedaron registrados",
              esperar("registro de los 9",
                      lambda: sum(1 for i in ids if estado_de(i) is not None) == 9,
                      35))

        principal = ids[0]
        check("Cada nodo envia metricas",
              esperar("metricas en los 9",
                      lambda: all(metricas_de(i) >= 2 for i in ids), 40),
              f"ej. {principal}: {metricas_de(principal)} filas")

        print("\n2. Sin mezcla de datos entre nodos")
        mezclados = 0
        for nid, (orig_id, region) in zip(ids, config.REGIONALES):
            guardada = region_de(nid)
            if guardada != region:
                mezclados += 1
        check("Cada node_id conserva su region (no hay cruce de datos)",
              mezclados == 0, f"{mezclados} nodos con region incorrecta")
        check("Las metricas van al node_id correcto",
              all(metricas_de(i) >= 2 for i in ids),
              "filas aisladas por node_id en tabla metricas")

        print("\n3. Conexion y desconexion registradas")
        check("Los 9 dejan evento ALTA_AUTOMATICA o CONEXION",
              all(hay_evento(i, "ALTA_AUTOMATICA") or hay_evento(i, "CONEXION")
                  for i in ids))

        # -------------------------------------------------------- corte abrupto
        print("\n4. Corte abrupto no tumba el servidor")
        abrupto_id = ids[2]
        proc_abrupto = next(
            p for p in _procesos
            if p.poll() is None and abrupto_id in " ".join(p.args))  # type: ignore[arg-type]
        antes_srv = metricas_de(principal)
        matar_abrupto(proc_abrupto)
        time.sleep(2)
        check("El proceso servidor sigue vivo tras el corte",
              servidor.poll() is None)
        check("Los demas nodos siguen reportando",
              esperar("metricas de los otros",
                      lambda: metricas_de(principal) > antes_srv, 20),
              f"{metricas_de(principal) - antes_srv} metricas nuevas")
        check("Queda evento DESCONEXION del nodo cortado",
              esperar("DESCONEXION",
                      lambda: hay_evento(abrupto_id, "DESCONEXION"), 15))

        # -------------------------------------------------------- 2.5
        print("\n5. Consolidados del cluster (tarea 2.5 — v_cluster)")
        resumen = repo.resumen_cluster()
        check("v_cluster ve los 9 nodos",
              int(resumen.get("nodos_totales", 0)) >= 9,
              f"totales={resumen.get('nodos_totales')}")
        check("Hay nodos activos reportando",
              int(resumen.get("nodos_activos", 0)) >= 9,
              f"activos={resumen.get('nodos_activos')}")
        cap = float(resumen.get("capacidad_total_gb") or 0)
        check("Capacidad total consolidada > 0", cap > 0, f"{cap:.1f} GB")
        pct = resumen.get("uso_pct_global")
        check("Porcentaje global calculado", pct is not None,
              f"{float(pct or 0):.1f}%")

        # -------------------------------------------------------- 2.4
        print("\n6. Mensajeria bajo carga (tarea 2.4)")
        texto = "Prueba de concurrencia del despachador"
        cmd_id = repo.crear_mensaje(principal, "MENSAJE", texto)
        check("El mensaje se confirma con ACK bajo carga",
              esperar("ACK concurrente",
                      lambda: (repo.obtener_mensaje(cmd_id) or {}).get("estado")
                      == "CONFIRMADO", 20))

        # -------------------------------------------------------- 2.6
        print("\n7. Deteccion de caida y umbral (tarea 2.6)")
        victima = ids[4]
        proc_victima = next(
            p for p in _procesos
            if p.poll() is None and victima in " ".join(p.args))  # type: ignore[arg-type]
        matar(proc_victima)

        umbral_teorico = FACTOR * INTERVALO + PERIODO_WD + 2
        t0 = time.time()
        ok = esperar("NO_REPORTA",
                     lambda: estado_de(victima) == "NO_REPORTA", 35)
        detectado = time.time() - t0
        check("Watchdog marca NO_REPORTA sin aviso del cliente", ok,
              f"detectado en {detectado:.1f} s")
        check(f"Deteccion dentro del umbral (~{umbral_teorico}s)",
              detectado <= umbral_teorico + 3,
              f"{detectado:.1f}s vs max ~{umbral_teorico + 3}s")

        # Los otros 8 siguen activos
        otros = [i for i in ids if i != victima]
        activos = sum(1 for i in otros if estado_de(i) == "ACTIVO")
        check("Los otros nodos siguen ACTIVO", activos >= 8,
              f"{activos}/8")

        # -------------------------------------------------------- MAX_NODOS
        print("\n8. Cluster lleno rechaza al decimo (requisito 9 nodos max)")
        matar(servidor)
        servidor = arrancar_servidor(max_nodos=9)
        # Ya hay 8 clientes vivos; agregamos uno mas = 9, luego el sobrante
        extra_ok = f"{PREFIJO}EXTRA-OK"
        extra_mal = f"{PREFIJO}EXTRA-MAL"
        arrancar_cliente(extra_ok, "Regional extra")
        time.sleep(3)
        proc_mal = arrancar_cliente(extra_mal, "Nodo de mas")
        termino = esperar("rechazo del sobrante",
                          lambda: proc_mal.poll() is not None, 25)
        check("El decimo cliente termina (rechazado, no reintenta forever)",
              termino, f"codigo {proc_mal.returncode}")

    finally:
        print("\nLimpiando...")
        for p in list(_procesos):
            matar(p)
        try:
            limpiar_bd()
        except Exception as e:                                    # noqa: BLE001
            print(f"  AVISO: no se pudo limpiar la base ({e})")
        cerrar_conexion_del_hilo()

    print("\n" + "=" * 70)
    if fallos:
        print(f" RESULTADO: {len(fallos)} fallas")
        for f in fallos:
            print(f"   - {f}")
        return 1
    print(" RESULTADO: servidor central OK — concurrencia, consolidados y watchdog.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
