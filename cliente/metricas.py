"""
Sensores del nodo — tareas 1.1, 1.2 y 1.8 (v2).  Responsable: Martin.

    python -m cliente.metricas                 # ver todo lo que sabe medir
    python -m cliente.metricas --solo disco,ram

QUE CAMBIO EN LA VERSION 2
--------------------------
Antes este archivo medía UNA cosa: el primer disco. Ahora es un REGISTRO DE
COLECTORES. Cada colector es una funcion que devuelve una lista de recursos, y
se registra con un decorador:

    @colector("ram")
    def _ram():
        return [protocolo.recurso("RAM", "fisica", {...})]

Para que el sistema mida algo nuevo — la temperatura, la cola de E/S, los
sensores de una UPS — se escribe una funcion de diez lineas aqui y se agrega su
nombre a RECURSOS en el .env. NO hay que tocar el protocolo, ni el servidor, ni
la base de datos, ni el dashboard: el recurso viaja como {tipo, nombre,
metricas} y se guarda en la tabla `recursos` con su JSON.

Eso es lo que quiere decir "flexible": el costo de agregar una metrica es una
funcion, no una migracion.

EL PRIMER DISCO SIGUE SIENDO ESPECIAL
------------------------------------
El enunciado obliga a reportar solo el primer disco, y de el salen v_cluster y
el KPI de utilizacion global. Por eso leer_disco() sigue existiendo igual que
antes y su resultado viaja aparte, en el bloque `disco` del mensaje. Los discos
ADICIONALES (el pendrive que enchufan en la laptop de Santa Cruz) van por el
camino nuevo, como recursos de tipo DISCO.

NINGUN COLECTOR LANZA EXCEPCIONES: si algo no se puede medir, devuelve el valor
neutro o una lista vacia y sigue. Un sensor que revienta tumba al cliente
entero, y el enunciado pide justamente lo contrario.
"""
from __future__ import annotations

import logging
import platform
import subprocess
import threading
import time
from pathlib import Path
from typing import Callable

import psutil

from comun import protocolo

log = logging.getLogger("metricas")

GB = 1024 ** 3
SISTEMA = platform.system()

# Estado para calcular tasas por diferencia entre dos lecturas. Va protegido:
# si alguna vez se levantan dos NodoCliente en el MISMO proceso, sin candado se
# pisarian los deltas y los IOPS saldrian basura. (Hoy lanzar_nodos.py usa
# procesos separados, asi que no pasa; el candado es para que siga sin pasar.)
_ultima_lectura: dict[str, float] | None = None
_ultima_red: dict[str, float] | None = None
_candado_io = threading.Lock()

# El tipo de disco no cambia mientras el proceso viva. En Windows averiguarlo
# lanza un PowerShell que tarda entre medio segundo y varios segundos: hacerlo
# en cada metrica alargaria el ciclo lo suficiente como para que el watchdog
# marque NO_REPORTA un nodo sano.
_tipo_cache: str | None = None

# Foto de las unidades vistas la ultima vez: {punto_montaje: total_gb}. Sirve
# para detectar en el propio nodo que enchufaron o sacaron un pendrive, y
# dejarlo anotado en su bitacora local aunque en ese momento no haya red.
_discos_vistos: dict[str, float] = {}

# ============================================================ REGISTRO
COLECTORES: dict[str, Callable[[], list[dict]]] = {}


def colector(nombre: str):
    """
    Registra una funcion como colector de recursos.

    Firma esperada: () -> list[dict]  (cada dict, un protocolo.recurso()).
    El nombre es el que se pone en RECURSOS del .env o en el CMD SET_RECURSOS.
    """
    def envoltura(fn: Callable[[], list[dict]]) -> Callable[[], list[dict]]:
        COLECTORES[nombre] = fn
        return fn
    return envoltura


def _seguro(nombre: str, fn: Callable[[], list[dict]]) -> list[dict]:
    """
    Ejecuta un colector sin dejar que su fallo se propague.

    Un colector roto tiene que costar UNA metrica ausente, no la sesion del
    nodo. Este es el mismo criterio que ya seguia leer_disco() en la version 1,
    aplicado ahora a todos.
    """
    try:
        salida = fn()
        return salida if isinstance(salida, list) else []
    except Exception as e:                                        # noqa: BLE001
        log.warning("El colector '%s' fallo: %s", nombre, e)
        return []


# ============================================================ DISCO (compat)

def _es_removible(particion) -> bool:
    """
    Distingue un pendrive o un disco USB de una unidad fija.

    Windows: psutil pone 'removable' o 'fixed' en `opts`.
    Linux  : /sys/block/<dev>/removable vale 1 en los extraibles.

    Importa porque el enunciado pide capacidad del cluster: sumar un pendrive
    de 32 GB como si fuera almacenamiento de un datacenter falsea el consolidado.
    Se reporta, se ve en el dashboard, pero etiquetado como lo que es.
    """
    opciones = (getattr(particion, "opts", "") or "").lower()
    if "removable" in opciones:
        return True
    if "cdrom" in opciones:
        return True
    if SISTEMA == "Linux":
        dispositivo = _dispositivo_de(particion.mountpoint)
        if dispositivo:
            try:
                return (Path("/sys/block") / dispositivo / "removable"
                        ).read_text().strip() == "1"
            except OSError:
                return False
    return False


def particiones_utiles() -> list:
    """
    Las unidades que de verdad son almacenamiento.

    Se filtran los pseudo-sistemas de archivos de Linux (tmpfs, squashfs, los
    loop de los snaps) y las unidades vacias de Windows (lector de tarjetas,
    DVD), que devuelven total=0 o revientan disk_usage().
    """
    try:
        particiones = psutil.disk_partitions(all=False)
    except Exception:                                             # noqa: BLE001
        return []
    utiles = []
    for p in particiones:
        if p.fstype in ("", "squashfs", "tmpfs", "devtmpfs", "overlay", "autofs"):
            continue
        try:
            uso = psutil.disk_usage(p.mountpoint)
        except (PermissionError, OSError):
            continue
        if uso.total <= 0:            # unidad vacia: no sirve como "disco"
            continue
        utiles.append((p, uso))
    return utiles


def primer_disco() -> str:
    """
    El enunciado obliga a reportar SOLO el primer disco detectado.
    'Primero' = la primera particion fisica con punto de montaje utilizable.

    Se prefiere una unidad FIJA: si alguien deja un pendrive enchufado y el
    sistema lo lista primero, el nodo reportaria 32 GB como capacidad de un
    datacenter. Solo si no hay ninguna fija se cae al primero que haya.
    """
    utiles = particiones_utiles()
    for p, _ in utiles:
        if not _es_removible(p):
            return p.mountpoint
    if utiles:
        return utiles[0][0].mountpoint
    return "C:\\" if SISTEMA == "Windows" else "/"


def _dispositivo_de(punto_montaje: str) -> str | None:
    """
    Dispositivo de bloque que respalda un punto de montaje, en Linux.
    Ejemplo: '/' -> 'sda'.  Devuelve None si no se puede resolver.
    """
    if SISTEMA != "Linux":
        return None
    try:
        for p in psutil.disk_partitions(all=False):
            if p.mountpoint == punto_montaje and p.device.startswith("/dev/"):
                nombre = p.device.rsplit("/", 1)[-1]        # /dev/sda1 -> sda1
                # Subir de la particion al disco: sda1 -> sda, nvme0n1p2 -> nvme0n1
                for bloque in sorted(Path("/sys/block").iterdir()):
                    if nombre == bloque.name or nombre.startswith(bloque.name):
                        return bloque.name
                return nombre
    except Exception:                                             # noqa: BLE001
        pass
    return None


def tipo_disco(punto_montaje: str | None = None) -> str:
    """
    SSD, HDD o USB. psutil no lo da: hay que preguntarle al sistema.
      Linux   -> /sys/block/<dev>/queue/rotational   (0 = SSD, 1 = HDD)
      Windows -> PowerShell, Get-PhysicalDisk, campo MediaType (texto)

    El resultado se cachea: el disco principal no cambia mientras el proceso
    viva. (Los discos ADICIONALES no usan esta cache: ver _tipo_de_particion.)
    """
    global _tipo_cache
    if _tipo_cache is not None:
        return _tipo_cache

    resultado = protocolo.TIPO_DESCONOCIDO
    try:
        if SISTEMA == "Linux":
            # Se busca el dispositivo que respalda ESTE punto de montaje, no el
            # primero por orden alfabetico: con un nvme y un sda, el alfabetico
            # devuelve el que no es.
            objetivo = _dispositivo_de(punto_montaje) if punto_montaje else None
            candidatos = ([Path("/sys/block") / objetivo] if objetivo
                          else sorted(Path("/sys/block").iterdir()))
            for bloque in candidatos:
                if bloque.name.startswith(("loop", "ram", "sr", "dm-")):
                    continue
                try:
                    rot = (bloque / "queue" / "rotational").read_text().strip()
                except OSError:
                    continue          # este no se deja leer: probamos el siguiente
                resultado = protocolo.TIPO_HDD if rot == "1" else protocolo.TIPO_SSD
                break

        elif SISTEMA == "Windows":
            salida = subprocess.run(
                ["powershell", "-NoProfile", "-NonInteractive", "-Command",
                 "(Get-PhysicalDisk | Select-Object -First 1).MediaType"],
                capture_output=True, text=True, timeout=15,
            ).stdout.strip().upper()
            if "SSD" in salida:
                resultado = protocolo.TIPO_SSD
            elif "HDD" in salida:
                resultado = protocolo.TIPO_HDD
    except Exception as e:                                        # noqa: BLE001
        log.debug("No se pudo determinar el tipo de disco: %s", e)

    _tipo_cache = resultado
    return resultado


def _tipo_de_particion(particion) -> str:
    """Tipo de UNA unidad cualquiera, sin cache: un pendrive aparece y
    desaparece, y cachear su tipo dejaria 'USB' pegado en la unidad fija."""
    if _es_removible(particion):
        return protocolo.TIPO_USB
    return tipo_disco(particion.mountpoint)


def _contadores_io(dispositivo: str | None):
    """
    Contadores de E/S del disco. Se pide por dispositivo cuando se puede: sin
    perdisk, psutil suma TODOS los dispositivos de bloque del sistema (en
    Ubuntu con snaps eso incluye los loop0..loopN) y los IOPS no
    corresponderian al disco que estamos reportando.
    """
    try:
        if dispositivo:
            porcada = psutil.disk_io_counters(perdisk=True)
            if porcada and dispositivo in porcada:
                return porcada[dispositivo]
        return psutil.disk_io_counters()
    except Exception:                                             # noqa: BLE001
        # En algunos contenedores y VMs no hay /proc/diskstats y psutil lanza
        # RuntimeError. No es motivo para dejar de reportar capacidad.
        return None


def _iops_y_latencia(dispositivo: str | None) -> tuple[int, int, float]:
    """
    IOPS = operaciones / segundo, calculado por DIFERENCIA entre dos lecturas.
    La primera llamada no tiene con que comparar y devuelve ceros: es normal.
    """
    global _ultima_lectura

    c = _contadores_io(dispositivo)
    if c is None:
        return 0, 0, 0.0

    # time.monotonic y no time.time: si alguien cambia la hora del sistema a
    # mitad de la medicion, time.time() puede retroceder y dt sale negativo,
    # o saltar y los IOPS salen a cero. El reloj monotonico no hace ninguna de
    # las dos cosas. Es el mismo motivo por el que el servidor fecha las
    # metricas con mono_ns (ver comun/protocolo.py).
    ahora = time.monotonic()
    actual = {
        "t": ahora,
        "lec": getattr(c, "read_count", 0),
        "esc": getattr(c, "write_count", 0),
        # macOS no expone read_time/write_time: sin getattr esto seria un
        # AttributeError que mata al cliente.
        "t_lec": getattr(c, "read_time", 0),
        "t_esc": getattr(c, "write_time", 0),
    }

    with _candado_io:
        anterior = _ultima_lectura
        _ultima_lectura = actual

    if anterior is None:
        return 0, 0, 0.0

    dt = actual["t"] - anterior["t"]
    if dt <= 0:
        return 0, 0, 0.0

    d_lec = actual["lec"] - anterior["lec"]
    d_esc = actual["esc"] - anterior["esc"]
    d_tiempo = (actual["t_lec"] - anterior["t_lec"]) + \
               (actual["t_esc"] - anterior["t_esc"])

    if d_lec < 0 or d_esc < 0 or d_tiempo < 0:
        # Los contadores se reiniciaron (reboot, disco reconectado, wrap del
        # entero). Un delta negativo daria IOPS negativos, que el esquema
        # rechaza. Se descarta esta muestra y se arranca de nuevo.
        return 0, 0, 0.0

    ops = d_lec + d_esc
    latencia = (d_tiempo / ops) if ops > 0 else 0.0   # ms por operacion
    # round y no int: un nodo tranquilo con 0.6 ops/s reportaria siempre 0.
    return round(d_lec / dt), round(d_esc / dt), round(latencia, 3)


def leer_disco() -> dict:
    """
    Devuelve el dict `disco` del PRIMER disco, listo para protocolo.metric().
    Nunca lanza: ante un fallo devuelve ceros y lo registra en el log.
    """
    vacio = {
        "nombre": None, "tipo": protocolo.TIPO_DESCONOCIDO,
        "total_gb": 0.0, "usado_gb": 0.0, "libre_gb": 0.0, "uso_pct": 0.0,
        "iops_lectura": 0, "iops_escritura": 0, "latencia_ms": 0.0,
    }
    try:
        punto = primer_disco()
        uso = psutil.disk_usage(punto)
    except Exception as e:                                        # noqa: BLE001
        log.warning("No se pudo leer el disco: %s", e)
        return vacio

    dispositivo = _dispositivo_de(punto)
    lec, esc, lat = _iops_y_latencia(dispositivo)

    total = uso.total or 0
    return {
        "nombre": punto,
        "tipo": tipo_disco(punto),
        "total_gb": round(total / GB, 2),
        "usado_gb": round(uso.used / GB, 2),
        "libre_gb": round(uso.free / GB, 2),
        # La guarda no sobra: una unidad vacia da total=0 y esto seria una
        # division por cero que mata al cliente.
        "uso_pct": round(uso.used / total * 100, 2) if total else 0.0,
        "iops_lectura": lec,
        "iops_escritura": esc,
        "latencia_ms": lat,
    }
    # Nota para el informe: en Linux, usado + libre NO suma el total, porque el
    # 5% del sistema de archivos queda reservado para root. No es un error de
    # la medicion.


# ============================================================== COLECTORES

@colector("discos")
def _colector_discos() -> list[dict]:
    """
    TODAS las unidades, no solo la primera.

    Esto es lo que resuelve el caso del pendrive: la laptop de Santa Cruz
    reporta su disco interno como siempre, y ademas el USB de 32 GB como un
    recurso DISCO aparte, etiquetado como extraible. El dashboard puede sumar
    la capacidad real del nodo sin ensuciar el KPI del enunciado, que por
    definicion mira solo el primer disco.
    """
    principal = primer_disco()
    salida: list[dict] = []
    for p, uso in particiones_utiles():
        total = uso.total or 0
        salida.append(protocolo.recurso(
            protocolo.REC_DISCO, p.mountpoint,
            {
                "total_gb": round(total / GB, 2),
                "usado_gb": round(uso.used / GB, 2),
                "libre_gb": round(uso.free / GB, 2),
                "uso_pct": round(uso.used / total * 100, 2) if total else 0.0,
            },
            {
                "tipo": _tipo_de_particion(p),
                "fs": p.fstype,
                "dispositivo": p.device,
                "removible": "si" if _es_removible(p) else "no",
                "principal": "si" if p.mountpoint == principal else "no",
            },
        ))
    return salida


@colector("ram")
def _colector_ram() -> list[dict]:
    """
    Memoria fisica y de intercambio.

    Se reporta con las MISMAS claves que un disco (total_gb, usado_gb,
    libre_gb, uso_pct) a proposito: asi el dashboard dibuja una barra de RAM
    con el mismo componente que usa para el disco, y las columnas generadas de
    la tabla `recursos` sirven para las dos cosas sin ningun caso especial.
    """
    salida: list[dict] = []
    v = psutil.virtual_memory()
    salida.append(protocolo.recurso(
        protocolo.REC_RAM, "fisica",
        {
            "total_gb": round(v.total / GB, 2),
            "usado_gb": round((v.total - v.available) / GB, 2),
            "libre_gb": round(v.available / GB, 2),
            # v.percent de psutil ya descuenta cache y buffers en Linux: es el
            # numero que muestra `free -h` como "used", no total-free.
            "uso_pct": round(v.percent, 2),
        },
        {"unidad": "GB"},
    ))
    try:
        s = psutil.swap_memory()
        if s.total > 0:
            salida.append(protocolo.recurso(
                protocolo.REC_RAM, "swap",
                {
                    "total_gb": round(s.total / GB, 2),
                    "usado_gb": round(s.used / GB, 2),
                    "libre_gb": round(s.free / GB, 2),
                    "uso_pct": round(s.percent, 2),
                },
                {"unidad": "GB"},
            ))
    except Exception:                                             # noqa: BLE001
        pass
    return salida


@colector("cpu")
def _colector_cpu() -> list[dict]:
    """
    Uso de CPU, nucleos y frecuencia.

    cpu_percent(interval=None) devuelve el porcentaje DESDE LA LLAMADA
    ANTERIOR. Por eso el cliente hace una lectura de siembra al arrancar cada
    sesion: sin ella, la primera medicion sale 0.0 y parece un nodo dormido.
    Con interval=1 seria bloqueante y agregaria un segundo a cada ciclo.
    """
    metricas = {"uso_pct": round(psutil.cpu_percent(interval=None), 2)}
    etiquetas = {}
    try:
        metricas["nucleos"] = psutil.cpu_count(logical=True) or 0
        metricas["nucleos_fisicos"] = psutil.cpu_count(logical=False) or 0
    except Exception:                                             # noqa: BLE001
        pass
    try:
        f = psutil.cpu_freq()
        if f:
            metricas["frecuencia_mhz"] = round(f.current, 1)
    except Exception:                                             # noqa: BLE001
        # cpu_freq no existe en algunas VMs y en varios ARM: no es un error.
        pass
    try:
        # getloadavg existe en Windows desde Python 3.3 (psutil lo emula), pero
        # los primeros 5 minutos devuelve valores sin sentido. Se manda igual:
        # el que lo lea sabe que es una media movil.
        c1, c5, c15 = psutil.getloadavg()
        metricas["carga_1min"] = round(c1, 2)
        metricas["carga_5min"] = round(c5, 2)
        metricas["carga_15min"] = round(c15, 2)
    except Exception:                                             # noqa: BLE001
        pass
    try:
        etiquetas["modelo"] = platform.processor() or platform.machine()
    except Exception:                                             # noqa: BLE001
        pass
    return [protocolo.recurso(protocolo.REC_CPU, "total", metricas, etiquetas)]


@colector("red")
def _colector_red() -> list[dict]:
    """
    Trafico de red agregado, en KB/s, por diferencia entre dos lecturas.

    Para un cluster que replica historiales clinicos, saber que un nodo esta
    saturando su enlace explica una latencia de disco que de otro modo no se
    entiende. Como los IOPS: la primera lectura devuelve ceros.
    """
    global _ultima_red
    try:
        c = psutil.net_io_counters()
    except Exception:                                             # noqa: BLE001
        return []
    if c is None:
        return []

    ahora = time.monotonic()
    actual = {"t": ahora, "rx": c.bytes_recv, "tx": c.bytes_sent,
              "err": c.errin + c.errout, "drop": c.dropin + c.dropout}
    with _candado_io:
        anterior = _ultima_red
        _ultima_red = actual

    if anterior is None:
        return []
    dt = actual["t"] - anterior["t"]
    if dt <= 0:
        return []
    d_rx = actual["rx"] - anterior["rx"]
    d_tx = actual["tx"] - anterior["tx"]
    if d_rx < 0 or d_tx < 0:          # contadores reiniciados
        return []
    return [protocolo.recurso(
        protocolo.REC_RED, "total",
        {
            "rx_kbps": round(d_rx / dt / 1024, 2),
            "tx_kbps": round(d_tx / dt / 1024, 2),
            "errores": max(0, actual["err"] - anterior["err"]),
            "descartes": max(0, actual["drop"] - anterior["drop"]),
        },
    )]


# ==================================================== CAMBIOS EN LAS UNIDADES

def detectar_cambios_discos() -> list[tuple[str, str]]:
    """
    Compara las unidades de ahora con las de la lectura anterior y devuelve
    [(tipo_de_evento, detalle)].

    Es lo que permite que el nodo anote "enchufaron un pendrive de 28.8 GB en
    E:\\" en su bitacora local AUNQUE en ese momento no haya red. El servidor
    hace su propia deteccion cuando recibe los datos (repositorio.discos_
    conocidos): los dos lados llegan a la misma conclusion por su cuenta, que
    es lo que se quiere en un sistema distribuido.

    El umbral de 0.5 GB no es arbitrario: redimensionar una particion o montar
    un snapshot mueve el total unos MB, y no queremos un evento por eso.
    """
    global _discos_vistos
    ahora: dict[str, float] = {}
    removibles: dict[str, bool] = {}
    for p, uso in particiones_utiles():
        ahora[p.mountpoint] = round((uso.total or 0) / GB, 2)
        removibles[p.mountpoint] = _es_removible(p)

    if not _discos_vistos:
        _discos_vistos = ahora
        return []

    cambios: list[tuple[str, str]] = []
    for punto, total in ahora.items():
        if punto not in _discos_vistos:
            que = "pendrive/unidad extraible" if removibles.get(punto) else "disco"
            cambios.append(("DISCO_AGREGADO",
                            f"Se agrego {que} en {punto} ({total} GB)"))
        elif abs(total - _discos_vistos[punto]) > 0.5:
            cambios.append(("CAPACIDAD_CAMBIADA",
                            f"{punto}: {_discos_vistos[punto]} GB -> {total} GB"))
    for punto in _discos_vistos:
        if punto not in ahora:
            cambios.append(("DISCO_REMOVIDO", f"Se quito la unidad {punto}"))

    _discos_vistos = ahora
    return cambios


# ================================================================= FACHADA

def sembrar() -> None:
    """
    Primera lectura de todos los contadores acumulativos.

    Los IOPS, el uso de CPU y el trafico de red se calculan por DIFERENCIA: sin
    una lectura previa, la primera medicion de cada sesion saldria en cero (o,
    peor, con un pico enorme calculado sobre unos milisegundos). El cliente
    llama a esto al conectar, antes del primer envio.
    """
    leer_disco()
    try:
        psutil.cpu_percent(interval=None)
    except Exception:                                             # noqa: BLE001
        pass
    _colector_red()
    detectar_cambios_discos()


def capacidades() -> list[str]:
    """Que sabe medir este cliente. Viaja en el HELLO."""
    return ["disco"] + sorted(COLECTORES.keys())


def leer_recursos(activos: list[str] | None = None) -> list[dict]:
    """
    Ejecuta los colectores pedidos y devuelve la lista de recursos.

    `activos` sale de la configuracion del nodo o del CMD SET_RECURSOS que
    manda el servidor. Un nombre desconocido se ignora con un aviso: que el
    operador escriba mal el nombre de un recurso no puede dejar al nodo sin
    reportar los que si existen.
    """
    if activos is None:
        activos = list(COLECTORES.keys())
    salida: list[dict] = []
    for nombre in activos:
        nombre = str(nombre).strip().lower()
        if nombre == "disco":
            continue                  # va aparte, en el bloque `disco`
        fn = COLECTORES.get(nombre)
        if fn is None:
            log.debug("Recurso desconocido, se ignora: %r", nombre)
            continue
        salida.extend(_seguro(nombre, fn))
    return salida


def leer_todo(activos: list[str] | None = None) -> tuple[dict, list[dict]]:
    """Una medicion completa: (disco_principal, recursos)."""
    return leer_disco(), leer_recursos(activos)


if __name__ == "__main__":
    import argparse
    import json

    p = argparse.ArgumentParser(description="Ver lo que mide este nodo")
    p.add_argument("--solo", help="lista separada por comas (ej: disco,ram)")
    a = p.parse_args()

    logging.basicConfig(level=logging.INFO)
    activos = ([x.strip() for x in a.solo.split(",")] if a.solo else None)

    sembrar()
    time.sleep(1)                     # deja que los deltas tengan sentido
    disco, recursos = leer_todo(activos)

    print("Capacidades de este nodo:", ", ".join(capacidades()))
    print("\n--- Primer disco (bloque `disco` del protocolo) ---")
    print(json.dumps(disco, indent=2, ensure_ascii=False))
    print(f"\n--- Recursos ({len(recursos)}) ---")
    print(json.dumps(recursos, indent=2, ensure_ascii=False))
