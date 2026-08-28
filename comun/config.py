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
# El enunciado pide "soporte exacto para 9 clientes". Se respeta: el servidor
# rechaza al que sobra con un ERROR explicito. Pero el limite es un PARAMETRO,
# no un numero escrito en el codigo, porque el cluster real crece: La Paz tiene
# dos servidores, y en la demo se agrega una computadora mas en caliente para
# mostrar el requisito 7.2. Para la defensa "9 exactos" basta con MAX_NODOS=9
# en el .env y el rechazo del decimo sale igual.
MAX_NODOS = _entero("MAX_NODOS", 12, 1, 100)

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


# --- Recursos que se reportan (v2) ---
# El cliente sabe medir varias cosas; esta lista decide cuales manda. Se puede
# cambiar por nodo desde el dashboard (CMD SET_RECURSOS) sin tocar el .env de
# esa maquina. "disco" siempre va: es el requisito del enunciado.
RECURSOS_DISPONIBLES = ("disco", "discos", "ram", "cpu", "red")
_crudo_recursos = _texto("RECURSOS", "disco,discos,ram,cpu")
RECURSOS_DEFECTO = [r for r in
                    (x.strip().lower() for x in _crudo_recursos.split(","))
                    if r in RECURSOS_DISPONIBLES] or ["disco"]

# --- Base local del cliente y sincronizacion (v2) ---
# El cliente guarda TODA muestra en su propia base SQLite, haya red o no. Al
# reconectar manda lo pendiente en lotes. Estos numeros acotan cuanto puede
# crecer ese archivo si el nodo pasa dias sin servidor.
BUFFER_MAX_MUESTRAS = _entero("BUFFER_MAX_MUESTRAS", 20000, 100, 5_000_000)
BUFFER_RETENCION_HORAS = _entero("BUFFER_RETENCION_HORAS", 72, 1, 8760)
SYNC_TAM_LOTE = _entero("SYNC_TAM_LOTE", 100, 1, 500)
# Pausa entre lotes: sin ella, un nodo con 20.000 muestras atrasadas satura al
# servidor y a MySQL justo cuando los otros ocho estan reportando normal.
SYNC_PAUSA_SEG = _entero("SYNC_PAUSA_MS", 150, 0, 10000) / 1000.0

# --- Reloj (v2) ---
# La hora de una metrica la pone SIEMPRE el servidor (ver comun/protocolo.py).
# Este umbral solo decide a partir de que desvio se deja constancia en la
# bitacora de que ese nodo tiene el reloj mal.
UMBRAL_RELOJ_SEG = _entero("UMBRAL_RELOJ_SEG", 60, 1, 86400)

# --- Deteccion de fallos intermitentes (v2) ---
# El servidor manda un PING de aplicacion cada tantos segundos. Un cable
# cortado o un wifi caido no producen FIN: sin este latido, la conexion queda
# "medio abierta" y el hilo espera bloqueado hasta el timeout del socket.
PERIODO_PING_SEG = _entero("PERIODO_PING_SEG", 15, 0, 3600)
# Cuantas caidas en la ventana hacen que un nodo se marque INTERMITENTE. Un
# nodo que va y viene es un problema distinto a uno que se cayo y ya.
INTERMITENCIA_CAIDAS = _entero("INTERMITENCIA_CAIDAS", 3, 2, 100)
INTERMITENCIA_VENTANA_MIN = _entero("INTERMITENCIA_VENTANA_MIN", 10, 1, 1440)

# --- WebSocket del dashboard (v2) ---
# Cada cuanto la API mira la base y difunde el estado a los navegadores
# conectados. El dashboard ya no hace F5 ni polling: recibe el empujon.
PERIODO_WS_SEG = _entero("PERIODO_WS_MS", 1000, 200, 60000) / 1000.0
WS_MAX_CLIENTES = _entero("WS_MAX_CLIENTES", 50, 1, 1000)

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
# Base local de cada cliente: datos/cliente_<node_id>.db
DIR_DATOS = RAIZ / "datos"


def asegurar_directorios() -> None:
    """
    Crea los directorios que hagan falta. NO se hace al importar: si el proceso
    no tiene permiso de escritura, importar config no puede fallar.
    """
    DIR_LOGS.mkdir(parents=True, exist_ok=True)
    DIR_DATOS.mkdir(parents=True, exist_ok=True)


# --- Las 9 regionales de la CNS ---
# Nombres unicos y acordados. No inventar otros: el dashboard, la BD y la
# presentacion tienen que decir exactamente lo mismo.
# La REGION YA NO ES LA IDENTIDAD DEL NODO: el node_id lo es. La Paz tiene dos
# servidores (01 y 10) y el dashboard los agrupa bajo la misma regional con su
# subtotal. Agregar una computadora nueva es agregar una linea aqui — o ni eso:
# un cliente con un node_id que no este en esta lista se da de alta solo
# (requisito 7.2). Esta lista solo la usa scripts/lanzar_nodos.py para la demo.
REGIONALES = [
    ("CNS-LPZ-01", "La Paz"),
    ("CNS-ELA-10", "La Paz"),          # segunda sede del departamento: El Alto
    ("CNS-CBB-02", "Cochabamba"),
    ("CNS-SCZ-03", "Santa Cruz"),
    ("CNS-ORU-04", "Oruro"),
    ("CNS-PTS-05", "Potosi"),
    ("CNS-CHU-06", "Chuquisaca"),
    ("CNS-TJA-07", "Tarija"),
    ("CNS-BEN-08", "Beni"),
    ("CNS-PAN-09", "Pando"),
]

# DEPARTAMENTO vs SEDE
# --------------------
# El enunciado habla de NUEVE administraciones regionales: eso es el
# DEPARTAMENTO, y es lo que va en `region`. La SEDE es la oficina concreta
# donde esta fisicamente ese servidor.
#
# La diferencia importa porque un departamento puede tener mas de una oficina:
# el departamento de La Paz atiende desde la ciudad de La Paz Y desde El Alto,
# y cada una tiene su propio servidor de archivos. Son dos maquinas, dos
# node_id, dos filas en la base — pero UNA sola regional en el consolidado.
#
# Por eso el dashboard agrupa por `region` y muestra la `sede` dentro: la
# pregunta "cuanto almacenamiento tiene La Paz" se responde sumando sus dos
# sedes, no eligiendo una.
#
# Esta lista es solo para la demo (scripts/lanzar_nodos.py). Un nodo manda su
# propia sede en el HELLO, asi que una maquina nueva puede unirse con la sede
# que quiera sin tocar este archivo.
SEDES = {
    "CNS-LPZ-01": "La Paz",
    "CNS-ELA-10": "El Alto",
    "CNS-CBB-02": "Cochabamba",
    "CNS-SCZ-03": "Santa Cruz de la Sierra",
    "CNS-ORU-04": "Oruro",
    "CNS-PTS-05": "Potosi",
    "CNS-CHU-06": "Sucre",
    "CNS-TJA-07": "Tarija",
    "CNS-BEN-08": "Trinidad",
    "CNS-PAN-09": "Cobija",
}


def sede_de(node_id: str, region: str = "") -> str:
    """
    Sede de un nodo conocido. Si no esta en la lista (una computadora que se
    suma en caliente), se usa el nombre del departamento: es mejor que dejarlo
    vacio y que el dashboard muestre un hueco.

    REGIONALES sigue siendo una lista de PARES y no de tercias a proposito: hay
    seis archivos de prueba que hacen `for nid, region in config.REGIONALES`, y
    romperlos por un campo opcional a dias de la defensa no vale la pena.
    """
    return SEDES.get(node_id) or region or "Sin sede"
