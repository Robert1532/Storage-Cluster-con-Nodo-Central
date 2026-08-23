"""
Sensor de disco — tareas 1.1 y 1.2.  Responsable: Martin.

ESQUELETO: la estructura y las partes dificiles (tipo SSD/HDD, IOPS por delta)
estan resueltas. Lo marcado con # TODO Martin falta.

Devuelve el dict `disco` EXACTAMENTE con las claves del protocolo. Si cambian
un nombre aqui, se rompe el servidor y la base. Avisen antes.
"""
from __future__ import annotations

import platform
import time

import psutil

GB = 1024 ** 3

# Estado para calcular IOPS por diferencia entre dos lecturas.
_ultima_lectura: dict[str, float] | None = None


def primer_disco() -> str:
    """
    El enunciado obliga a reportar SOLO el primer disco detectado.
    'Primero' = la primera particion fisica con punto de montaje valido.
    Se filtran los pseudo-sistemas de archivos de Linux (tmpfs, squashfs) y
    las unidades de CD vacias de Windows, que revientan disk_usage().
    """
    for p in psutil.disk_partitions(all=False):
        if p.fstype in ("", "squashfs", "tmpfs", "devtmpfs"):
            continue
        try:
            psutil.disk_usage(p.mountpoint)
        except (PermissionError, OSError):
            continue
        return p.mountpoint
    return "C:\\" if platform.system() == "Windows" else "/"


def tipo_disco() -> str:
    """
    SSD o HDD. psutil no lo da: hay que preguntarle al sistema.
      Linux   -> /sys/block/<dev>/queue/rotational   (0 = SSD, 1 = HDD)
      Windows -> WMI, clase MSFT_PhysicalDisk, campo MediaType (3=HDD, 4=SSD)
    """
    sistema = platform.system()
    try:
        if sistema == "Linux":
            from pathlib import Path
            for bloque in sorted(Path("/sys/block").iterdir()):
                if bloque.name.startswith(("loop", "ram", "sr")):
                    continue
                rot = (bloque / "queue" / "rotational").read_text().strip()
                return "HDD" if rot == "1" else "SSD"
        elif sistema == "Windows":
            import subprocess
            salida = subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 "(Get-PhysicalDisk | Select-Object -First 1).MediaType"],
                capture_output=True, text=True, timeout=8,
            ).stdout.strip().upper()
            if "SSD" in salida:
                return "SSD"
            if "HDD" in salida:
                return "HDD"
    except Exception:                                             # noqa: BLE001
        pass
    return "DESCONOCIDO"


def _iops_y_latencia() -> tuple[int, int, float]:
    """
    IOPS = operaciones / segundo, calculado por DIFERENCIA entre dos lecturas.
    La primera llamada no tiene con que comparar y devuelve ceros: es normal.
    """
    global _ultima_lectura
    c = psutil.disk_io_counters()
    ahora = time.time()
    if c is None:
        return 0, 0, 0.0

    actual = {
        "t": ahora,
        "lec": c.read_count, "esc": c.write_count,
        "t_lec": c.read_time, "t_esc": c.write_time,
    }
    if _ultima_lectura is None:
        _ultima_lectura = actual
        return 0, 0, 0.0

    dt = actual["t"] - _ultima_lectura["t"]
    if dt <= 0:
        return 0, 0, 0.0

    d_lec = actual["lec"] - _ultima_lectura["lec"]
    d_esc = actual["esc"] - _ultima_lectura["esc"]
    d_tiempo = (actual["t_lec"] - _ultima_lectura["t_lec"]) + \
               (actual["t_esc"] - _ultima_lectura["t_esc"])
    ops = d_lec + d_esc
    _ultima_lectura = actual

    latencia = (d_tiempo / ops) if ops > 0 else 0.0   # ms por operacion
    return int(d_lec / dt), int(d_esc / dt), round(latencia, 3)


def leer_disco() -> dict:
    """Devuelve el dict `disco` listo para protocolo.metric()."""
    punto = primer_disco()
    uso = psutil.disk_usage(punto)
    lec, esc, lat = _iops_y_latencia()

    return {
        "nombre": punto,
        "tipo": tipo_disco(),
        "total_gb": round(uso.total / GB, 2),
        "usado_gb": round(uso.used / GB, 2),
        "libre_gb": round(uso.free / GB, 2),
        "uso_pct": round(uso.used / uso.total * 100, 2),
        "iops_lectura": lec,
        "iops_escritura": esc,
        "latencia_ms": lat,
    }
    # TODO Martin: correr esto en Windows y en Linux y comparar con lo que muestra
    #          el explorador de archivos / df -h. Guardar las dos capturas
    #          (tarea 1.2, vale 10% de la nota).


if __name__ == "__main__":
    import json
    leer_disco()          # primera lectura: siembra el delta de IOPS
    time.sleep(1)
    print(json.dumps(leer_disco(), indent=2, ensure_ascii=False))
