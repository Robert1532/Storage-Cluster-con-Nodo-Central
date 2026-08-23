"""
Configuracion compartida por cliente, servidor y API.

NADIE debe leer os.environ directamente en su modulo: todo pasa por aqui.
Asi, si manana cambia el nombre de una variable, se cambia en un solo lugar.

Responsable: Robert (Datos y Coordinacion). Los demas solo importan de aqui.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ / ".env")

# --- MySQL ---
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "cns_app")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "cns_cluster")
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", 16))

# Ruta al certificado CA. Vacio = MySQL local sin TLS.
# Con valor = MySQL gestionado (Aiven exige TLS y da su propio ca.pem).
_ssl_ca = os.getenv("DB_SSL_CA", "").strip()
DB_SSL_CA = str((RAIZ / _ssl_ca).resolve()) if _ssl_ca else None

# --- Sockets ---
SOCKET_HOST = os.getenv("SOCKET_HOST", "0.0.0.0")
SOCKET_PORT = int(os.getenv("SOCKET_PORT", 5050))
MAX_NODOS = int(os.getenv("MAX_NODOS", 9))

# --- Reglas de monitoreo ---
INTERVALO_DEFECTO_SEG = int(os.getenv("INTERVALO_DEFECTO_SEG", 10))
FACTOR_TIMEOUT = int(os.getenv("FACTOR_TIMEOUT", 3))
PERIODO_WATCHDOG_SEG = int(os.getenv("PERIODO_WATCHDOG_SEG", 2))
PERIODO_DESPACHADOR_SEG = int(os.getenv("PERIODO_DESPACHADOR_SEG", 1))

# --- API ---
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", 8000))

# --- Rutas ---
DIR_LOGS = RAIZ / "logs"
DIR_LOGS.mkdir(exist_ok=True)

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
