"""
Prueba de la capa de datos — cierra los "listo cuando" de las tareas 3.1 a 3.3.
Responsable: Robert.  Se corre ANTES de que el servidor de sockets exista.

    python -m db.probar_bd

Que verifica:
  1. Conexion y version de MySQL
  2. Que las 4 tablas, las 3 vistas y los indices existan
  3. Alta automatica: un nodo nuevo se inserta solo y deja su evento
  4. CONCURRENCIA REAL: 9 hilos escribiendo metricas al mismo tiempo
  5. Watchdog: caida y recuperacion, con sus eventos
  6. Ciclo de mensaje: PENDIENTE -> ENVIADO -> CONFIRMADO
  7. Saneado: datos basura del cliente no rompen el INSERT
  8. Las consultas de agregacion devuelven numeros coherentes

Todo lo que crea usa el prefijo TEST- y se borra al final, PASE LO QUE PASE
(hay un try/finally en main). Importa: la base de Aiven es compartida por los
cinco, y dejar basura ahi contamina el dashboard de los demas.
"""
from __future__ import annotations

import random
import sys
import threading
import time

from comun import config
from db import repositorio as repo
from db.conexion import (cerrar_conexion_del_hilo, cursor, medir_latencia,
                         probar_conexion)

PREFIJO = "TEST-"
SEGUNDOS_CARGA = 20          # subir a 600 para la prueba de 10 minutos
INTERVALO_PRUEBA = 1         # los nodos de prueba reportan cada 1 s

fallos: list[str] = []
errores_hilos: list[str] = []


def check(nombre: str, condicion: bool, detalle: str = "") -> None:
    marca = "OK  " if condicion else "FALLA"
    print(f"  [{marca}] {nombre}" + (f"  -> {detalle}" if detalle else ""))
    if not condicion:
        fallos.append(nombre)


def _alta(node_id: str, region: str = "Regional de prueba",
          intervalo: int = INTERVALO_PRUEBA) -> tuple[bool, int]:
    return repo.registrar_nodo(node_id, region, f"host-{node_id}", "Linux",
                               "192.168.1.50", intervalo)


def _metrica(usado: float = 200.0, total: float = 500.0, **extra) -> dict:
    disco = {
        "nombre": "/dev/sda1",
        "tipo": random.choice(["SSD", "HDD"]),
        "total_gb": round(total, 2),
        "usado_gb": round(usado, 2),
        "libre_gb": round(total - usado, 2),
        "uso_pct": round(usado / total * 100, 2),
        "iops_lectura": random.randint(50, 400),
        "iops_escritura": random.randint(20, 200),
        "latencia_ms": round(random.uniform(0.2, 3.0), 3),
    }
    disco.update(extra)
    return disco


# --------------------------------------------------------------------- 1 y 2

def prueba_estructura() -> None:
    print("\n1-2. Conexion y estructura")
    check("Conexion a MySQL", probar_conexion())

    with cursor() as cur:
        cur.execute("SELECT VERSION() AS v")
        version = cur.fetchone()["v"]
        cur.execute("SHOW TABLES")
        nombres = {list(f.values())[0] for f in cur.fetchall()}

    partes = version.split("-")[0].split(".")
    numeros = tuple(int(x) for x in (partes + ["0", "0"])[:3])
    check("MySQL 8.0.14 o superior (las vistas lo necesitan)",
          numeros >= (8, 0, 14), version)

    for t in ("nodos", "metricas", "eventos", "mensajes"):
        check(f"Tabla {t}", t in nombres)
    for v in ("v_ultima_metrica", "v_nodos_estado", "v_cluster"):
        check(f"Vista {v}", v in nombres)

    with cursor() as cur:
        cur.execute("SHOW INDEX FROM metricas WHERE Key_name = 'ix_nodo_tiempo'")
        columnas = [f["Column_name"] for f in cur.fetchall()]
    check("Indice (node_id, timestamp, id) en metricas",
          columnas == ["node_id", "timestamp", "id"], ", ".join(columnas))


# ------------------------------------------------------------------------- 3

def prueba_alta_automatica() -> None:
    print("\n3. Alta automatica de cliente (requisito 7.2)")
    node_id = f"{PREFIJO}AUTO-{random.randint(1000, 9999)}"

    nuevo, intervalo = _alta(node_id, intervalo=7)
    check("Primer HELLO devuelve nuevo=True", nuevo is True)

    # No se compara contra el argumento (eso pasaria aunque el INSERT guardara
    # otra cosa): se lee de la base.
    with cursor() as cur:
        cur.execute("SELECT intervalo_seg FROM nodos WHERE node_id=%s", (node_id,))
        fila = cur.fetchone()
    guardado = int(fila["intervalo_seg"]) if fila else -1
    check("El intervalo quedo guardado en la base", guardado == 7, str(guardado))
    check("Y es el que devuelve registrar_nodo", intervalo == 7, str(intervalo))

    eventos = repo.listar_eventos(limite=5, node_id=node_id)
    check("Dejo el evento ALTA_AUTOMATICA",
          any(e["tipo"] == "ALTA_AUTOMATICA" for e in eventos))

    # Segundo HELLO con OTRO intervalo: tiene que devolver el de la BASE, no el
    # que manda el cliente. Esa es la mitad "desde el servidor" del 7.3.
    nuevo2, intervalo2 = _alta(node_id, intervalo=99)
    check("Segundo HELLO devuelve nuevo=False", nuevo2 is False)
    check("Devuelve el intervalo de la BASE, no el del cliente",
          intervalo2 == 7, f"devolvio {intervalo2}, el cliente pidio 99")

    # Carrera: dos hilos dando de alta el MISMO nodo a la vez. Con un
    # SELECT-luego-INSERT esto revienta con "Duplicate entry".
    node_race = f"{PREFIJO}RACE-{random.randint(1000, 9999)}"
    barrera = threading.Barrier(4)
    errores: list[str] = []

    def competir():
        try:
            barrera.wait(timeout=10)
            _alta(node_race)
        except Exception as e:                        # noqa: BLE001
            errores.append(f"{type(e).__name__}: {e}")
        finally:
            cerrar_conexion_del_hilo()

    hilos = [threading.Thread(target=competir) for _ in range(4)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()
    check("Cuatro altas simultaneas del mismo nodo, sin error",
          not errores, "; ".join(errores[:2]))


# ------------------------------------------------------------------------- 4

def _escritor(node_id: str, region: str, hasta: float) -> None:
    """Simula un nodo: se registra y escribe metricas hasta 'hasta'."""
    try:
        _alta(node_id, region)
        usado = random.uniform(100, 300)
        while time.time() < hasta:
            usado += random.uniform(0.01, 0.08)      # crecimiento simulado
            repo.guardar_metrica(node_id, _iso(), _metrica(usado))
            time.sleep(INTERVALO_PRUEBA)
    except Exception as e:                            # noqa: BLE001
        errores_hilos.append(f"{node_id}: {type(e).__name__}: {e}")
    finally:
        # Un hilo que termina cierra su conexion, o queda colgada del lado del
        # servidor. Lo mismo hace el servidor cuando se desconecta un cliente.
        cerrar_conexion_del_hilo()


def ids_prueba() -> list[str]:
    return [f"{PREFIJO}{n}" for n, _ in config.REGIONALES]


def prueba_concurrencia() -> None:
    print(f"\n4. Concurrencia: 9 hilos escribiendo {SEGUNDOS_CARGA} s")

    # Cuanto tarda un viaje a la base define TODO lo que sigue. Contra MySQL
    # local son milisegundos; contra Aiven son cientos, y en ese caso nueve
    # hilos NO pueden escribir 9 filas por segundo aunque el codigo sea
    # perfecto. Por eso lo que se espera se calcula, no se asume.
    lat = medir_latencia()
    donde = "local" if lat < 20 else "en la nube"
    print(f"      Latencia a la base: {lat:.1f} ms por operacion ({donde})")
    if lat >= 20:
        print("      Contra la nube esta prueba mide la RED, no el codigo.")
        print("      La prueba de carga larga (10 min) hacela contra MySQL local.")

    ids = ids_prueba()
    marcadores = ",".join(["%s"] * len(ids))

    with cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS c FROM metricas WHERE node_id IN ({marcadores})",
                    tuple(ids))
        antes = cur.fetchone()["c"]

    hasta = time.time() + SEGUNDOS_CARGA
    hilos = [threading.Thread(target=_escritor, args=(nid, reg, hasta), daemon=True)
             for nid, (_, reg) in zip(ids, config.REGIONALES)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()

    check("Ningun error en los hilos",
          not errores_hilos, "; ".join(errores_hilos[:3]))

    # Lo que de verdad importa no es el numero bruto de filas (eso lo decide la
    # red), sino que los NUEVE hilos hayan podido escribir en paralelo. Se
    # cuentan solo los ids de esta prueba: contar toda la tabla haria pasar el
    # check aunque los hilos no hubieran escrito nada.
    with cursor() as cur:
        cur.execute(f"""SELECT node_id, COUNT(*) AS n FROM metricas
                         WHERE node_id IN ({marcadores}) GROUP BY node_id""",
                    tuple(ids))
        por_nodo = {f["node_id"]: int(f["n"]) for f in cur.fetchall()}
        cur.execute(f"SELECT COUNT(*) AS c FROM metricas WHERE node_id IN ({marcadores})",
                    tuple(ids))
        insertadas = cur.fetchone()["c"] - antes

    escribieron = len(por_nodo)
    check("Los 9 hilos escribieron en paralelo", escribieron == 9,
          f"{escribieron}/9 nodos con metricas")
    check("Sin datos cruzados entre nodos", set(por_nodo) == set(ids))

    with cursor() as cur:
        cur.execute(f"""SELECT COUNT(*) AS c FROM v_ultima_metrica
                         WHERE node_id IN ({marcadores})""", tuple(ids))
        check("v_ultima_metrica devuelve 1 fila por nodo",
              cur.fetchone()["c"] == escribieron)

    # guardar_metrica son 2 consultas; el ciclo de cada hilo es el intervalo
    # mas esos dos viajes.
    ciclo = INTERVALO_PRUEBA + (2 * lat / 1000)
    esperadas = max(1, int(9 * SEGUNDOS_CARGA / ciclo))
    check("Rendimiento acorde a la latencia medida",
          insertadas >= esperadas * 0.6,
          f"{insertadas} filas en {SEGUNDOS_CARGA}s = "
          f"{insertadas / SEGUNDOS_CARGA:.1f}/s (esperado ~{esperadas})")
    print("      De referencia: en produccion son 9 nodos cada 10 s = 0.9 filas/s")


# ------------------------------------------------------------------------- 5

def prueba_watchdog() -> None:
    print("\n5. Watchdog: caida y recuperacion")
    node_id = f"{PREFIJO}WD-{random.randint(1000, 9999)}"
    _alta(node_id)
    repo.guardar_metrica(node_id, _iso(), _metrica())

    # Envejecer el ultimo reporte para simular que dejo de reportar.
    with cursor() as cur:
        cur.execute("""UPDATE nodos
                          SET ultimo_reporte = NOW(3) - INTERVAL 60 SECOND,
                              estado = 'ACTIVO'
                        WHERE node_id = %s""", (node_id,))

    caidos = repo.marcar_nodos_caidos(config.FACTOR_TIMEOUT)
    check("Detecta el nodo sin reportes", node_id in caidos)

    with cursor() as cur:
        cur.execute("SELECT estado FROM nodos WHERE node_id=%s", (node_id,))
        fila = cur.fetchone()
    check("Quedo en NO_REPORTA", fila is not None and fila["estado"] == "NO_REPORTA")

    eventos = repo.listar_eventos(limite=10, node_id=node_id)
    check("Dejo el evento NO_REPORTA",
          any(e["tipo"] == "NO_REPORTA" for e in eventos))

    # Correr el watchdog otra vez no debe duplicar el evento: el nodo ya no
    # esta ACTIVO, asi que no vuelve a entrar.
    repetidos = repo.marcar_nodos_caidos(config.FACTOR_TIMEOUT)
    check("No lo marca dos veces", node_id not in repetidos)

    # Vuelve a reportar -> el watchdog lo recupera.
    repo.guardar_metrica(node_id, _iso(), _metrica())
    recuperados = repo.marcar_nodos_recuperados(config.FACTOR_TIMEOUT)
    check("Detecta la recuperacion", node_id in recuperados)

    with cursor() as cur:
        cur.execute("SELECT estado FROM nodos WHERE node_id=%s", (node_id,))
        fila = cur.fetchone()
    check("Volvio a ACTIVO", fila is not None and fila["estado"] == "ACTIVO")

    eventos = repo.listar_eventos(limite=10, node_id=node_id)
    check("Dejo el evento RECUPERADO",
          any(e["tipo"] == "RECUPERADO" for e in eventos))


# ------------------------------------------------------------------------- 6

def prueba_mensajes() -> None:
    print("\n6. Ciclo de mensaje PENDIENTE -> ENVIADO -> CONFIRMADO")
    node_id = f"{PREFIJO}MSG-{random.randint(1000, 9999)}"
    _alta(node_id)

    cmd_id = repo.crear_mensaje(node_id, "MENSAJE", "Verifique espacio en disco")

    # Se busca por cmd_id, no dentro de la pagina de 50 pendientes: si hay 50
    # mensajes viejos sin despachar, el nuestro seria el ultimo de la cola y el
    # check fallaria con el codigo perfectamente correcto.
    fila = repo.obtener_mensaje(cmd_id)
    check("Nace en estado PENDIENTE", fila is not None and fila["estado"] == "PENDIENTE")

    repo.marcar_enviado(cmd_id)
    time.sleep(0.05)
    repo.confirmar_ack(cmd_id)

    fila = repo.obtener_mensaje(cmd_id)
    check("Queda CONFIRMADO", fila is not None and fila["estado"] == "CONFIRMADO")
    check("Se calcula el round-trip", fila is not None and fila["rtt_ms"] is not None,
          f"{fila['rtt_ms']} ms" if fila and fila["rtt_ms"] is not None else "")

    # Un fallo guarda su motivo: un FALLIDO sin explicacion no sirve de nada.
    cmd_falla = repo.crear_mensaje(node_id, "MENSAJE", "Se va a marcar fallido")
    repo.marcar_fallido(cmd_falla, "El nodo no esta conectado")
    fila = repo.obtener_mensaje(cmd_falla)
    check("El FALLIDO guarda el motivo",
          fila is not None and fila["estado"] == "FALLIDO"
          and fila["detalle"] == "El nodo no esta conectado",
          (fila or {}).get("detalle") or "sin detalle")

    # SET_INTERVAL persiste el valor (requisito 7.3).
    repo.crear_mensaje(node_id, "SET_INTERVAL", None, 5)
    repo.actualizar_intervalo(node_id, 5)
    with cursor() as cur:
        cur.execute("SELECT intervalo_seg FROM nodos WHERE node_id=%s", (node_id,))
        fila = cur.fetchone()
    check("SET_INTERVAL persiste el intervalo (7.3)",
          fila is not None and int(fila["intervalo_seg"]) == 5)

    # Un intervalo absurdo se acota en vez de reventar el CHECK del esquema.
    repo.actualizar_intervalo(node_id, 0)
    with cursor() as cur:
        cur.execute("SELECT intervalo_seg FROM nodos WHERE node_id=%s", (node_id,))
        fila = cur.fetchone()
    check("Un intervalo de 0 se acota, no rompe",
          fila is not None and int(fila["intervalo_seg"]) >= 1,
          str((fila or {}).get("intervalo_seg")))


# ------------------------------------------------------------------------- 7

def prueba_saneado() -> None:
    print("\n7. Datos basura del cliente: se sanean, no rompen")
    node_id = f"{PREFIJO}SUCIO-{random.randint(1000, 9999)}"
    _alta(node_id, region="R" * 300)              # region mas larga que la columna

    casos = [
        ("tipo fuera del ENUM", {"tipo": "NVMe"}),
        ("tipo nulo", {"tipo": None}),
        ("IOPS negativos", {"iops_lectura": -5, "iops_escritura": -9}),
        ("latencia enorme", {"latencia_ms": 123456.789}),
        ("capacidad enorme", {"total_gb": 9e15}),
        ("uso_pct fuera de rango", {"uso_pct": 12345.6}),
        ("nombre de disco larguisimo", {"nombre": "/dev/" + "x" * 500}),
        ("valores de texto donde van numeros", {"usado_gb": "no soy un numero"}),
        ("timestamp invalido", {}),
    ]
    for etiqueta, extra in casos:
        ts = "no es una fecha" if etiqueta == "timestamp invalido" else _iso()
        try:
            repo.guardar_metrica(node_id, ts, _metrica(**extra))
            check(f"Sobrevive a: {etiqueta}", True)
        except Exception as e:                    # noqa: BLE001
            check(f"Sobrevive a: {etiqueta}", False, f"{type(e).__name__}: {e}")

    with cursor() as cur:
        cur.execute("""SELECT disco_tipo, iops_lectura, latencia_ms, uso_pct
                         FROM metricas WHERE node_id=%s AND disco_tipo='DESCONOCIDO'
                        LIMIT 1""", (node_id,))
        fila = cur.fetchone()
    check("El tipo invalido quedo como DESCONOCIDO", fila is not None)


# ------------------------------------------------------------------------- 8

def prueba_agregaciones() -> None:
    print("\n8. Consultas de agregacion")
    ids = ids_prueba()
    referencia = ids[0]

    c = repo.resumen_cluster()
    check("Resumen del cluster devuelve datos", bool(c))
    check("Capacidad total > 0", float(c.get("capacidad_total_gb", 0)) > 0,
          f"{c.get('capacidad_total_gb')} GB")

    total = float(c.get("capacidad_total_gb") or 0)
    usado = float(c.get("usado_total_gb") or 0)
    libre = float(c.get("libre_total_gb") or 0)
    # usado + libre <= total, no igualdad: en Linux real, psutil deja fuera el
    # ~5% reservado para root y los tres numeros no cuadran exacto.
    check("usado + libre <= total", usado + libre <= total + 1.0,
          f"{usado:.2f} + {libre:.2f} vs {total:.2f}")

    pct = float(c.get("uso_pct_global") or 0)
    esperado = (usado / total * 100) if total else 0
    check("% global coherente", abs(pct - esperado) < 0.5, f"{pct}%")

    nodos = repo.listar_nodos()
    check("listar_nodos trae filas", len(nodos) > 0, f"{len(nodos)} nodos")

    hist = repo.historial(referencia, horas=1)
    check("historial devuelve serie temporal", len(hist) > 1, f"{len(hist)} puntos")
    if len(hist) > 1:
        check("historial viene ordenado de viejo a nuevo",
              hist[0]["timestamp"] <= hist[-1]["timestamp"])
        # El recorte tiene que quedarse con lo MAS NUEVO, no con lo mas viejo.
        with cursor() as cur:
            cur.execute("""SELECT MAX(timestamp) AS m FROM metricas
                            WHERE node_id=%s AND timestamp >= NOW() - INTERVAL 1 HOUR""",
                        (referencia,))
            ultimo_real = cur.fetchone()["m"]
        check("historial incluye el punto mas reciente",
              hist[-1]["timestamp"] == ultimo_real)

        limitado = repo.historial(referencia, horas=1, limite=10)
        check("historial con limite tambien termina en el mas reciente",
              len(limitado) <= 10 and limitado[-1]["timestamp"] == ultimo_real,
              f"{len(limitado)} puntos")

    g = repo.crecimiento(horas=1)
    check("crecimiento calcula GB/dia", len(g) > 0,
          f"ej: {g[0]['growth_gb_dia']} GB/dia" if g else "")

    d = repo.disponibilidad(horas=1)
    check("disponibilidad calculada", len(d) > 0,
          f"ej: {d[0]['disponibilidad_pct']}%" if d else "")

    ev = repo.listar_eventos(limite=20)
    check("listar_eventos trae la bitacora", len(ev) > 0, f"{len(ev)} eventos")


# ------------------------------------------------------------------- limpieza

def _limpiar() -> None:
    """Borra todo lo que empiece con TEST-. Las metricas, eventos y mensajes
    caen solos por ON DELETE CASCADE."""
    try:
        with cursor() as cur:
            cur.execute("DELETE FROM nodos WHERE node_id LIKE %s", (PREFIJO + "%",))
            return cur.rowcount
    except Exception as e:                                        # noqa: BLE001
        print(f"  AVISO: no se pudo limpiar ({e}). Borra a mano los nodos TEST-.")
        return 0


def _iso() -> str:
    from comun import protocolo
    return protocolo.ahora_iso()


def main() -> int:
    print("=" * 70)
    print(" PRUEBA DE LA CAPA DE DATOS - Storage Cluster CNS")
    print("=" * 70)

    prueba_estructura()
    if fallos:
        print("\nLa estructura no esta lista. Corre primero:")
        print("  mysql ... < db/schema.sql")
        return 1

    try:
        _limpiar()               # por si quedo basura de una corrida abortada
        prueba_alta_automatica()
        prueba_concurrencia()
        prueba_watchdog()
        prueba_mensajes()
        prueba_saneado()
        prueba_agregaciones()
    finally:
        # Pase lo que pase: la base de Aiven es compartida.
        print("\nLimpiando datos de prueba...")
        borrados = _limpiar()
        print(f"  {borrados} nodos TEST- borrados")
        cerrar_conexion_del_hilo()

    print("\n" + "=" * 70)
    if fallos:
        print(f" RESULTADO: {len(fallos)} fallas")
        for f in fallos:
            print(f"   - {f}")
        return 1
    print(" RESULTADO: todo OK. La capa de datos esta lista para el equipo.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
