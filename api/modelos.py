"""
Modelos Pydantic — contrato de la API.  Responsable: Robert.

FastAPI usa esto para validar la entrada, serializar la salida y generar sola
la documentacion de /docs. Alex (dashboard) lee estos nombres de campo: si
cambia uno, se avisa igual que con el protocolo de sockets.

POR QUE float Y NO Decimal
--------------------------
Las columnas son DECIMAL en MySQL, y el driver las devuelve como Decimal de
Python. Si el modelo declara Decimal, Pydantic serializa a JSON como STRING
("1000.00") para no perder precision. Los endpoints sin response_model, en
cambio, devuelven numero. Esa mezcla es una trampa: en JavaScript,
"1000.00" + "500.00" da "1000.00500.00" en vez de 1500.

Aqui se declara float en todos lados, asi el dashboard recibe siempre numeros.
La precision exacta se conserva donde importa de verdad, que es en la base:
las sumas y el % global los calcula MySQL sobre DECIMAL, no el navegador.
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class NodoOut(BaseModel):
    node_id: str
    # region = departamento (las nueve regionales). sede = la oficina concreta:
    # el departamento de La Paz tiene sede La Paz y sede El Alto.
    region: str
    sede: str | None = None
    hostname: str | None = None
    sistema_operativo: str | None = None
    ip: str | None = None
    estado: str                                  # ACTIVO | NO_REPORTA
    intervalo_seg: int
    primer_registro: datetime | None = None
    ultimo_reporte: datetime | None = None
    disco_nombre: str | None = None
    disco_tipo: str | None = None
    total_gb: float | None = None
    usado_gb: float | None = None
    libre_gb: float | None = None
    uso_pct: float | None = None
    iops_lectura: int | None = None
    iops_escritura: int | None = None
    latencia_ms: float | None = None
    segundos_sin_reportar: int | None = None
    failover_events: int = 0

    # ------------------------------------------------------------- v2 ------
    agente_version: str | None = None
    # Que sabe medir el nodo y que le pidio el servidor que mande. Viajan como
    # texto separado por comas, igual que en la columna: el dashboard solo los
    # muestra, y una lista de cuatro palabras no justifica una tabla aparte.
    capacidades: str | None = None
    recursos_pedidos: str | None = None

    # "Se desconecto de la red el ...". Sin esto el dashboard solo podia decir
    # que un nodo no reporta, nunca desde cuando ni por que.
    ultima_desconexion: datetime | None = None
    motivo_desconexion: str | None = None
    ultima_reconexion: datetime | None = None

    # Un nodo que se cae y vuelve no es lo mismo que uno caido.
    intermitente: bool = False
    caidas_recientes: int = 0

    # Estado de la sincronizacion: cuantas muestras trae guardadas sin entregar
    # y hasta que numero de muestra confirmo el servidor.
    pendientes_sync: int = 0
    ultima_seq: int = 0

    # Cuanto miente el reloj del nodo. La metrica se guarda con la hora del
    # servidor igual; esto es para que el operador lo sepa.
    desvio_reloj_seg: float | None = None

    # VIVO o SYNC: si el ultimo dato llego en tiempo real o se recupero del
    # buffer del cliente despues de una caida. No son lo mismo y el dashboard
    # los distingue.
    origen_ultima_metrica: str | None = None

    recursos_activos: int = 0
    # Capacidad de los discos ADICIONALES (el pendrive de Santa Cruz). No entra
    # en total_gb porque el enunciado define esa columna como el primer disco.
    extra_disco_gb: float = 0.0


class ClusterOut(BaseModel):
    nodos_totales: int
    nodos_activos: int
    capacidad_total_gb: float
    usado_total_gb: float
    libre_total_gb: float
    uso_pct_global: float | None = None
    latencia_ponderada_ms: float | None = None
    # v2
    nodos_intermitentes: int = 0
    regionales: int = 0
    capacidad_con_extras_gb: float = 0.0
    extras_gb: float = 0.0


class PuntoHistorial(BaseModel):
    timestamp: datetime
    usado_gb: float
    libre_gb: float
    uso_pct: float
    iops_lectura: int
    iops_escritura: int
    latencia_ms: float


class HistorialOut(BaseModel):
    node_id: str
    horas: int
    puntos: list[PuntoHistorial]


class CrecimientoOut(BaseModel):
    node_id: str
    delta_gb: float
    horas_observadas: float
    growth_gb_dia: float


class DisponibilidadOut(BaseModel):
    node_id: str
    reportes: int
    esperados: int
    disponibilidad_pct: float


class EventoOut(BaseModel):
    node_id: str
    timestamp: datetime
    tipo: str
    detalle: str | None = None


class SaludOut(BaseModel):
    estado: str
    base_datos: str
    conexiones_abiertas: int


class MensajeIn(BaseModel):
    node_id: str = Field(..., min_length=1, max_length=32, examples=["CNS-LPZ-01"])
    texto: str = Field(..., min_length=1, max_length=255,
                       examples=["Verifique espacio en disco"])


class SetIntervalIn(BaseModel):
    intervalo_seg: int = Field(..., ge=1, le=3600, examples=[5])


class MensajeOut(BaseModel):
    cmd_id: str
    node_id: str
    accion: str
    texto: str | None = None
    valor: int | None = None
    estado: str
    detalle: str | None = None
    creado_en: datetime
    enviado_en: datetime | None = None
    ack_en: datetime | None = None
    rtt_ms: float | None = None


class RespuestaComando(BaseModel):
    cmd_id: str
    estado: str
    detalle: str


# ------------------------------------------------------------------- v2 -----

class RecursoOut(BaseModel):
    """
    Un recurso medido: un disco, la RAM, la CPU, una interfaz de red.

    `metricas` es un diccionario ABIERTO a proposito. Es lo que hace que
    agregar una medida nueva no obligue a tocar este archivo, ni el dashboard,
    ni la base: el cliente manda una clave mas y aparece sola.
    """
    node_id: str
    tipo: str                                    # DISCO | RAM | CPU | RED | CUSTOM
    nombre: str
    timestamp: datetime
    metricas: dict[str, float] = Field(default_factory=dict)
    etiquetas: dict[str, str] = Field(default_factory=dict)
    origen: str = "VIVO"
    total_gb: float | None = None
    usado_gb: float | None = None
    uso_pct: float | None = None
    # Cuanto hace que se midio. Un recurso que dejo de reportarse (el pendrive
    # que sacaron) sigue teniendo su ultima medicion guardada: esto es lo que
    # permite al dashboard mostrarlo como historico y no como estado actual.
    segundos_desde: int | None = None


class PuntoRecurso(BaseModel):
    timestamp: datetime
    total_gb: float | None = None
    usado_gb: float | None = None
    uso_pct: float | None = None
    origen: str = "VIVO"


class HistorialRecursoOut(BaseModel):
    node_id: str
    tipo: str
    nombre: str
    horas: int
    puntos: list[PuntoRecurso]


class RegionalOut(BaseModel):
    """
    Consolidado por REGIONAL, no por maquina.

    Existe porque La Paz tiene dos servidores: el enunciado habla de nueve
    administraciones regionales, no de nueve computadoras.
    """
    region: str
    sedes: str | None = None
    nodos: int
    nodos_activos: int
    capacidad_total_gb: float
    usado_total_gb: float
    libre_total_gb: float
    uso_pct: float | None = None
    ultimo_reporte: datetime | None = None


class SetRecursosIn(BaseModel):
    """Que debe medir un nodo. Se valida contra la lista de colectores que
    existen de verdad: un nombre inventado se rechaza con 422, en vez de
    dejar al nodo callado sin que nadie entienda por que."""
    recursos: list[str] = Field(..., min_length=1, max_length=16,
                                examples=[["disco", "ram", "cpu"]])


class PuntoCluster(BaseModel):
    t: datetime
    usado_gb: float
    total_gb: float
    uso_pct: float
    nodos: int


class HistorialClusterOut(BaseModel):
    """
    Utilizacion global en el tiempo: UNA serie, no una por nodo.

    Diez lineas en un mismo grafico no se leen. El total va aqui; cada nodo
    tiene su mini-linea dentro de su tarjeta.
    """
    horas: int
    puntos: list[PuntoCluster]


class TramoUso(BaseModel):
    desde: int
    hasta: int
    nodos: int
    node_ids: list[str] = Field(default_factory=list)
