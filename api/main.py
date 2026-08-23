"""
API REST con FastAPI — tarea 4.1.  Responsable: Robert (junto con la BD).

    uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
    Documentacion automatica en  http://localhost:8000/docs

POR QUE FASTAPI NO HABLA DIRECTO CON EL SERVIDOR DE SOCKETS
-----------------------------------------------------------
Son dos procesos distintos: no comparten memoria. Cuando el dashboard quiere
mandar un mensaje a un nodo, la API NO abre un socket al servidor: inserta una
fila en la tabla `mensajes` con estado PENDIENTE, y el despachador del servidor
de sockets la recoge en menos de un segundo.

Ventajas: los tres procesos (sockets, API, dashboard) se reinician por separado,
y todo mensaje queda auditado en la BD aunque el nodo este caido.
Costo: hasta 1 segundo de latencia. Para monitoreo es irrelevante.

Si en la defensa preguntan como se comunican los procesos, esta es la respuesta,
y la alternativa (un socket de control interno en localhost) tambien conviene
tenerla en la punta de la lengua.
"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from comun import config
from db import repositorio as repo
from db.conexion import probar_conexion

from api.modelos import (ClusterOut, EventoOut, MensajeIn, MensajeOut,
                         NodoOut, RespuestaComando, SetIntervalIn)

app = FastAPI(
    title="Storage Cluster CNS",
    description="Monitoreo centralizado de los 9 servidores regionales",
    version="1.0.0",
)

# El dashboard es un archivo estatico servido en otro origen mientras
# desarrollan. Sin esto, el navegador bloquea las llamadas.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # en produccion se restringe; aqui es LAN cerrada
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/salud", tags=["sistema"])
def salud():
    """Chequeo rapido: ¿la API llega a MySQL?"""
    ok = probar_conexion()
    if not ok:
        raise HTTPException(status_code=503, detail="Sin conexion a MySQL")
    return {"estado": "ok", "base_datos": "conectada"}


@app.get("/api/nodes", response_model=list[NodoOut], tags=["cluster"])
def listar_nodos():
    """Los 9 servidores regionales con su ultima metrica y su estado."""
    return repo.listar_nodos()


@app.get("/api/cluster", response_model=ClusterOut, tags=["cluster"])
def resumen_cluster():
    """Totales consolidados: capacidad, libre, % global, nodos activos."""
    datos = repo.resumen_cluster()
    if not datos:
        raise HTTPException(status_code=404, detail="Sin datos del cluster")
    return datos


@app.get("/api/history/{node_id}", tags=["cluster"])
def historial(node_id: str,
              horas: int = Query(24, ge=1, le=720),
              limite: int = Query(500, ge=10, le=5000)):
    """Serie temporal de un nodo, para el grafico historico."""
    filas = repo.historial(node_id, horas=horas, limite=limite)
    if not filas:
        raise HTTPException(status_code=404, detail=f"Sin historial para {node_id}")
    return {"node_id": node_id, "horas": horas, "puntos": filas}


@app.get("/api/growth", tags=["cluster"])
def crecimiento(horas: int = Query(24, ge=1, le=720)):
    """Growth rate en GB/dia por nodo."""
    return repo.crecimiento(horas=horas)


@app.get("/api/availability", tags=["cluster"])
def disponibilidad():
    """Disponibilidad por nodo. La meta del enunciado es >= 99.9%."""
    return repo.disponibilidad()


@app.get("/api/events", response_model=list[EventoOut], tags=["cluster"])
def eventos(limite: int = Query(100, ge=1, le=1000), node_id: str | None = None):
    """Bitacora: conexiones, altas automaticas, caidas y recuperaciones."""
    return repo.listar_eventos(limite=limite, node_id=node_id)


@app.post("/api/message", response_model=RespuestaComando, tags=["mensajeria"])
def enviar_mensaje(cuerpo: MensajeIn):
    """
    Encola un mensaje para un nodo (requisito 7.1).
    Queda PENDIENTE; el despachador lo envia en <= 1 s y luego llega el ACK.
    """
    cmd_id = repo.crear_mensaje(cuerpo.node_id, "MENSAJE", cuerpo.texto)
    return RespuestaComando(cmd_id=cmd_id, estado="PENDIENTE",
                            detalle=f"Encolado para {cuerpo.node_id}")


@app.post("/api/config/{node_id}", response_model=RespuestaComando, tags=["mensajeria"])
def cambiar_intervalo(node_id: str, cuerpo: SetIntervalIn):
    """
    Cambia el intervalo de envio de un nodo desde el servidor (requisito 7.3).
    Persiste el valor y encola el comando SET_INTERVAL.
    """
    repo.actualizar_intervalo(node_id, cuerpo.intervalo_seg)
    cmd_id = repo.crear_mensaje(node_id, "SET_INTERVAL", None, cuerpo.intervalo_seg)
    return RespuestaComando(cmd_id=cmd_id, estado="PENDIENTE",
                            detalle=f"Intervalo de {node_id} -> {cuerpo.intervalo_seg}s")


@app.get("/api/messages", response_model=list[MensajeOut], tags=["mensajeria"])
def listar_mensajes(node_id: str | None = None, limite: int = Query(50, ge=1, le=500)):
    """Historial de mensajes con su ACK y el round-trip medido."""
    return repo.listar_mensajes(node_id=node_id, limite=limite)


# El dashboard se sirve desde la misma API: una sola URL para la demo.
app.mount("/", StaticFiles(directory="dashboard", html=True), name="dashboard")
