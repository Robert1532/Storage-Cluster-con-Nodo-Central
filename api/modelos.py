"""
Modelos Pydantic — contrato de la API.  Responsable: Robert.

FastAPI usa esto para validar la entrada, serializar la salida y generar sola
la documentacion de /docs. Alex (dashboard) lee estos nombres de campo: si cambia
uno, se avisa igual que con el protocolo de sockets.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

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
    total_gb: Decimal | None = None
    usado_gb: Decimal | None = None
    libre_gb: Decimal | None = None
    uso_pct: Decimal | None = None
    iops_lectura: int | None = None
    iops_escritura: int | None = None
    latencia_ms: Decimal | None = None
    segundos_sin_reportar: int | None = None
    failover_events: int = 0


class ClusterOut(BaseModel):
    nodos_totales: int
    nodos_activos: int
    capacidad_total_gb: Decimal
    usado_total_gb: Decimal
    libre_total_gb: Decimal
    uso_pct_global: Decimal | None = None
    latencia_ponderada_ms: Decimal | None = None


class EventoOut(BaseModel):
    node_id: str
    timestamp: datetime
    tipo: str
    detalle: str | None = None


class MensajeIn(BaseModel):
    node_id: str = Field(..., examples=["CNS-LPZ-01"])
    texto: str = Field(..., max_length=255, examples=["Verifique espacio en disco"])


class SetIntervalIn(BaseModel):
    intervalo_seg: int = Field(..., ge=1, le=3600, examples=[5])


class MensajeOut(BaseModel):
    cmd_id: str
    node_id: str
    accion: str
    texto: str | None = None
    valor: int | None = None
    estado: str
    creado_en: datetime
    enviado_en: datetime | None = None
    ack_en: datetime | None = None
    rtt_ms: Decimal | None = None


class RespuestaComando(BaseModel):
    cmd_id: str
    estado: str
    detalle: str
