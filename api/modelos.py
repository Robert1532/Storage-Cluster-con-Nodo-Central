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
    region: str
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


class ClusterOut(BaseModel):
    nodos_totales: int
    nodos_activos: int
    capacidad_total_gb: float
    usado_total_gb: float
    libre_total_gb: float
    uso_pct_global: float | None = None
    latencia_ponderada_ms: float | None = None


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
