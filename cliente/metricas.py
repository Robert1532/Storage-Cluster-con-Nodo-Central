"""
Sensor de disco — tareas 1.1 y 1.2.  Responsable: Martin.

ESQUELETO: la estructura y las partes dificiles (tipo SSD/HDD, IOPS por delta,
diferencias Windows/Linux) estan resueltas. Lo marcado con # TODO Martin falta.

Devuelve el dict `disco` EXACTAMENTE con las claves del protocolo. Si cambian
un nombre aqui, se rompe el servidor y la base. Avisen antes.

leer_disco() NO lanza excepciones: si algo no se puede medir, devuelve el valor
neutro y sigue. Un sensor que revienta tumba al cliente entero, y el enunciado
pide justamente lo contrario.
"""
from __future__ import annotations

import logging
import platform
import subprocess
import threading
import time
from pathlib import Path

import psutil

from comun import protocolo

log = logging.getLogger("metricas")

GB = 1024 ** 3
SISTEMA = platform.system()

# Estado para calcular IOPS por diferencia entre dos lecturas. Va protegido:
# si alguna vez se levantan dos NodoCliente en el MISMO proceso, sin candado se
# pisarian los deltas y los IOPS saldrian basura. (Hoy lanzar_nodos.py usa
# procesos separados, asi que no pasa; el candado es para que siga sin pasar.)
_ultima_lectura: dict[str, float] | None = None
_candado_io = threading.Lock()

# El tipo de disco no cambia mientras el proceso viva. En Windows averiguarlo
# lanza un PowerShell que tarda entre medio segundo y varios segundos: hacerlo
# en cada metrica alargaria el ciclo lo suficiente como para que el watchdog
# marque NO_REPORTA un nodo sano.
_tipo_cache: str | None = None


def primer_disco() -> str:
    """
    El enunciado obliga a reportar SOLO el primer disco detectado.
    'Primero' = la primera particion fisica con punto de montaje utilizable.
    Se filtran los pseudo-sistemas de archivos de Linux (tmpfs, squashfs) y las
    unidades vacias de Windows (lector de tarjetas, DVD), que devuelven total=0
    o revientan disk_usage().
    """
    try:
        particiones = psutil.disk_partitions(all=False)
    except Exception:                                             # noqa: BLE001
        particiones = []

    for p in particiones:
        if p.fstype in ("", "squashfs", "tmpfs", "devtmpfs", "overlay"):
            continue
        try:
            uso = psutil.disk_usage(p.mountpoint)
        except (PermissionError, OSError):
            continue
        if uso.total <= 0:            # unidad vacia: no sirve como "disco"
            continue
        return p.mountpoint

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
    SSD o HDD. psutil no lo da: hay que preguntarle al sistema.
      Linux   -> /sys/block/<dev>/queue/rotational   (0 = SSD, 1 = HDD)
      Windows -> PowerShell, Get-PhysicalDisk, campo MediaType (texto)

    El resultado se cachea: no cambia mientras el proceso viva.
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

    ahora = time.time()
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
    Devuelve el dict `disco` listo para protocolo.metric().
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
    # TODO Martin: correr esto en Windows y en Linux y comparar con lo que
    #              muestra el explorador de archivos / df -h. Guardar las dos
    #              capturas (tarea 1.2, vale 10% de la nota).
    #
    #              Nota para el informe: en Linux, usado + libre NO suma el
    #              total, porque el 5% del sistema de archivos queda reservado
    #              para root. No es un error de la medicion.


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    leer_disco()          # primera lectura: siembra el delta de IOPS
    time.sleep(1)
    print(json.dumps(leer_disco(), indent=2, ensure_ascii=False))
