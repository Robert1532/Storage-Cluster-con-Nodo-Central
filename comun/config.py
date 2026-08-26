"""
Configuracion compartida por cliente, servidor y API.

NADIE debe leer os.environ directamente en su modulo: todo pasa por aqui.
Asi, si manana cambia el nombre de una variable, se cambia en un solo lugar.

Responsable: Robert (Datos y Coordinacion). Los demas solo importan de aqui.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ / ".env")


def _entero(nombre: str, defecto: int, minimo: int = 1, maximo: int = 10 ** 9) -> int:
    """
    Lee un entero del .env sin reventar. Una variable vacia o con un valor
    invalido cae al valor por defecto en vez de tumbar el import: si config.py
    lanza una excepcion, ningun modulo del proyecto puede siquiera loguear el
    motivo.
    """
    crudo = (os.getenv(nombre) or "").strip()
    if not crudo:
        return defecto
    try:
        valor = int(crudo)
    except ValueError:
        print(f"[config] {nombre}='{crudo}' no es un numero; uso {defecto}")
        return defecto
    if not (minimo <= valor <= maximo):
        print(f"[config] {nombre}={valor} fuera de [{minimo}, {maximo}]; uso {defecto}")
        return defecto
    return valor


def _texto(nombre: str, defecto: str = "") -> str:
    return (os.getenv(nombre) or defecto).strip()


# --- MySQL ---
DB_HOST = _texto("DB_HOST", "127.0.0.1")
DB_PORT = _entero("DB_PORT", 3306, 1, 65535)
DB_USER = _texto("DB_USER", "cns_app")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")      # sin strip: puede tener espacios
DB_NAME = _texto("DB_NAME", "cns_cluster")

# Conexiones simultaneas que puede abrir ESTE proceso. Con una conexion por
# hilo, es el techo de hilos que tocan la base a la vez.
DB_MAX_CONEXIONES = _entero("DB_POOL_SIZE", 16, 1, 100)
DB_POOL_SIZE = DB_MAX_CONEXIONES                 # alias, por compatibilidad

# Ruta al certificado CA. Vacio = MySQL local sin TLS.
# Con valor = MySQL gestionado (Aiven exige TLS y da su propio ca.pem).
_ssl_ca = _texto("DB_SSL_CA")
DB_SSL_CA: str | None = None
if _ssl_ca:
    _ruta_ca = Path(_ssl_ca)
    if not _ruta_ca.is_absolute():
        _ruta_ca = RAIZ / _ruta_ca
    if not _ruta_ca.exists():
        # Sin esto el fallo aparece como un error de TLS incomprensible.
        raise FileNotFoundError(
            f"DB_SSL_CA apunta a '{_ssl_ca}' pero ese archivo no existe.\n"
            f"Buscado en: {_ruta_ca}\n"
            f"Descarga el ca.pem del panel de Aiven y guardalo en db/ca.pem, "
            f"o deja DB_SSL_CA vacio si usas MySQL local sin TLS.")
    DB_SSL_CA = str(_ruta_ca)

# --- Sockets ---
SOCKET_HOST = _texto("SOCKET_HOST", "0.0.0.0")
SOCKET_PORT = _entero("SOCKET_PORT", 5050, 1, 65535)
MAX_NODOS = _entero("MAX_NODOS", 9, 1, 100)

# --- Reglas de monitoreo ---
INTERVALO_MIN_SEG = 1
INTERVALO_MAX_SEG = 3600
INTERVALO_DEFECTO_SEG = _entero("INTERVALO_DEFECTO_SEG", 10,
                                INTERVALO_MIN_SEG, INTERVALO_MAX_SEG)
FACTOR_TIMEOUT = _entero("FACTOR_TIMEOUT", 3, 2, 100)
PERIODO_WATCHDOG_SEG = _entero("PERIODO_WATCHDOG_SEG", 2, 1, 600)
PERIODO_DESPACHADOR_SEG = _entero("PERIODO_DESPACHADOR_SEG", 1, 1, 600)


def acotar_intervalo(segundos: object) -> int:
    """
    Deja el intervalo dentro de un rango sano. Un intervalo de 0 haria que el
    watchdog marque NO_REPORTA un segundo despues de cada reporte, para siempre.
    """
    try:
        valor = int(segundos)                                      # type: ignore[arg-type]
    except (TypeError, ValueError):
        return INTERVALO_DEFECTO_SEG
    return max(INTERVALO_MIN_SEG, min(INTERVALO_MAX_SEG, valor))


# --- API ---
API_HOST = _texto("API_HOST", "0.0.0.0")
API_PORT = _entero("API_PORT", 8000, 1, 65535)

# Hilos que FastAPI usa para atender pedidos. Cada hilo abre su propia conexion
# a MySQL, asi que este numero es tambien el techo de conexiones del proceso de
# la API. El valor por defecto de Starlette es 40, demasiado para el plan
# gratuito de Aiven cuando ademas corre el servidor de sockets.
API_HILOS = _entero("API_HILOS", 6, 1, 40)

# --- Rutas ---
DIR_LOGS = RAIZ / "logs"
DIR_DASHBOARD = RAIZ / "dashboard"


def asegurar_directorios() -> None:
    """
    Crea los directorios que hagan falta. NO se hace al importar: si el proceso
    no tiene permiso de escritura, importar config no puede fallar.
    """
    DIR_LOGS.mkdir(parents=True, exist_ok=True)


# --- Las 9 regionales de la CNS ---
# Nombres unicos y acordados. No inventar otros: el dashboard, la BD y la
# presentacion tienen que decir exactamente lo mismo.
REGIONALES = [
    ("CNS-LPZ-01", "La Paz"),
    ("CNS-CBB-02", "Cochabamba"),
    ("CNS-SCZ-03", "Santa Cruz"),
    ("CNS-ORU-04", "Oruro"),
    ("CNS-PTS-05", "Potosi"),
    ("CNS-CHU-06", "Chuquisaca"),
    ("CNS-TJA-07", "Tarija"),
    ("CNS-BEN-08", "Beni"),
    ("CNS-PAN-09", "Pando"),
]
