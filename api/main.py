"""
API REST con FastAPI — tarea 4.1.  Responsable: Robert (junto con la BD).

    uvicorn api.main:app --host 0.0.0.0 --port 8000
    Documentacion automatica en  http://localhost:8000/docs
    Dashboard en                 http://localhost:8000/

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

CUANTAS CONEXIONES A MySQL ABRE ESTE PROCESO
--------------------------------------------
Los endpoints son sincronos, asi que Starlette los corre en su pool de hilos.
Cada hilo abre su propia conexion a MySQL (ver db/conexion.py) y la reusa. El
limite por defecto de Starlette es 40 hilos: con el servidor de sockets
corriendo en paralelo y cinco personas contra la misma base de Aiven, eso
agota el limite de conexiones del plan gratuito. Por eso el lifespan de abajo
baja ese limite a config.API_HILOS.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import anyio.to_thread
from fastapi import (FastAPI, HTTPException, Query, WebSocket,
                     WebSocketDisconnect)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.difusion import gestor
from api.modelos import (ClusterOut, CrecimientoOut, DisponibilidadOut,
                         EventoOut, HistorialClusterOut, HistorialOut,
                         HistorialRecursoOut, MensajeIn, MensajeOut, NodoOut,
                         RecursoOut, RegionalOut, RespuestaComando, SaludOut,
                         SetIntervalIn, SetRecursosIn, TramoUso)
from comun import config, protocolo
from db import repositorio as repo
from db.conexion import conexiones_abiertas, probar_conexion

log = logging.getLogger("api")


@asynccontextmanager
async def ciclo_de_vida(app: FastAPI):
    config.asegurar_directorios()
    # Techo de hilos = techo de conexiones a MySQL de este proceso.
    anyio.to_thread.current_default_thread_limiter().total_tokens = config.API_HILOS
    log.info("API lista. Maximo %d hilos (y por tanto %d conexiones a MySQL)",
             config.API_HILOS, config.API_HILOS)
    # v2: la tarea que empuja el estado a los navegadores por WebSocket. No
    # consulta nada mientras no haya nadie conectado (ver api/difusion.py).
    gestor.arrancar()
    try:
        yield
    finally:
        await gestor.detener()


app = FastAPI(
    title="Storage Cluster CNS",
    description="Monitoreo centralizado de los 9 servidores regionales",
    version="1.0.0",
    lifespan=ciclo_de_vida,
)

# El dashboard se sirve desde esta misma app, asi que en la demo no hace falta
# CORS. Se deja abierto solo para que Alex pueda abrir el index.html con
# file:// o desde otro puerto mientras desarrolla. En una red que no fuera la
# LAN cerrada del laboratorio, esto se restringe.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _exigir_nodo(node_id: str) -> None:
    """
    404 explicito en vez del 500 que daria la clave foranea de `mensajes`.
    Pasa de verdad: si se abre el dashboard antes de levantar los nodos, el
    selector queda vacio y manda un node_id que no existe.
    """
    if not repo.existe_nodo(node_id):
        raise HTTPException(status_code=404, detail=f"No existe el nodo '{node_id}'")


@app.get("/api/salud", response_model=SaludOut, tags=["sistema"])
def salud():
    """Chequeo rapido: la API llega a MySQL, y cuantas conexiones tiene abiertas."""
    if not probar_conexion():
        raise HTTPException(status_code=503, detail="Sin conexion a MySQL")
    return SaludOut(estado="ok", base_datos="conectada",
                    conexiones_abiertas=conexiones_abiertas())


@app.get("/api/nodes", response_model=list[NodoOut], tags=["cluster"])
def listar_nodos():
    """Los 9 servidores regionales con su ultima metrica y su estado."""
    return repo.listar_nodos()


@app.get("/api/cluster", response_model=ClusterOut, tags=["cluster"])
def resumen_cluster():
    """
    Totales consolidados: capacidad, libre, % global, nodos activos.
    v_cluster siempre devuelve una fila, incluso sin nodos: en ese caso vienen
    ceros y NULL, no un error.
    """
    return repo.resumen_cluster()


@app.get("/api/history/{node_id}", response_model=HistorialOut, tags=["cluster"])
def historial(node_id: str,
              horas: int = Query(24, ge=1, le=720),
              limite: int = Query(500, ge=10, le=5000)):
    """
    Serie temporal de un nodo, para el grafico historico.

    Una lista vacia es una respuesta legitima, no un 404: un nodo recien dado
    de alta todavia no tiene puntos, y el grafico tiene que poder dibujar
    "sin datos" en vez de romperse.
    """
    return HistorialOut(node_id=node_id, horas=horas,
                        puntos=repo.historial(node_id, horas=horas, limite=limite))


@app.get("/api/history-cluster", response_model=HistorialClusterOut, tags=["cluster"])
def historial_cluster(horas: int = Query(24, ge=1, le=720),
                      puntos: int = Query(120, ge=10, le=600)):
    """
    Utilizacion global del cluster en el tiempo: una sola serie, ya agrupada.

    Se agrupa en el servidor porque 24 h de 10 nodos cada 10 s son 86.400 filas,
    y el navegador tendria que dibujar 86.400 segmentos en 800 pixeles.
    """
    return HistorialClusterOut(horas=horas,
                               puntos=repo.historial_cluster(horas, puntos))


@app.get("/api/sparklines", tags=["cluster"])
def sparklines(horas: int = Query(6, ge=1, le=720),
               puntos: int = Query(30, ge=5, le=200)):
    """Una mini-serie por nodo, para dibujarla dentro de su tarjeta. Una sola
    consulta en vez de un GET por nodo desde el navegador."""
    return repo.sparklines(horas, puntos)


@app.get("/api/distribution", response_model=list[TramoUso], tags=["cluster"])
def distribucion(tramos: int = Query(5, ge=2, le=10)):
    """
    Histograma: cuantos nodos hay en cada tramo de utilizacion.

    Responde algo que la lista de tarjetas no responde: si el cluster esta
    equilibrado o si hay unos pocos nodos llenos y el resto vacios.
    """
    return repo.distribucion_uso(tramos)


@app.get("/api/growth", response_model=list[CrecimientoOut], tags=["cluster"])
def crecimiento(horas: int = Query(24, ge=1, le=720)):
    """Growth rate en GB/dia por nodo."""
    return repo.crecimiento(horas=horas)


@app.get("/api/availability", response_model=list[DisponibilidadOut], tags=["cluster"])
def disponibilidad(horas: int = Query(24, ge=1, le=720)):
    """Disponibilidad por nodo en la ventana. La meta del enunciado es >= 99.9%."""
    return repo.disponibilidad(horas=horas)


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
    _exigir_nodo(cuerpo.node_id)
    cmd_id = repo.crear_mensaje(cuerpo.node_id, "MENSAJE", cuerpo.texto)
    return RespuestaComando(cmd_id=cmd_id, estado="PENDIENTE",
                            detalle=f"Encolado para {cuerpo.node_id}")


@app.post("/api/config/{node_id}", response_model=RespuestaComando, tags=["mensajeria"])
def cambiar_intervalo(node_id: str, cuerpo: SetIntervalIn):
    """
    Cambia el intervalo de envio de un nodo desde el servidor (requisito 7.3).
    Persiste el valor y encola el comando SET_INTERVAL.
    """
    _exigir_nodo(node_id)
    repo.actualizar_intervalo(node_id, cuerpo.intervalo_seg)
    cmd_id = repo.crear_mensaje(node_id, "SET_INTERVAL", None, cuerpo.intervalo_seg)
    return RespuestaComando(cmd_id=cmd_id, estado="PENDIENTE",
                            detalle=f"Intervalo de {node_id} -> {cuerpo.intervalo_seg}s")


@app.get("/api/messages", response_model=list[MensajeOut], tags=["mensajeria"])
def listar_mensajes(node_id: str | None = None, limite: int = Query(50, ge=1, le=500)):
    """Historial de mensajes con su ACK y el round-trip medido."""
    return repo.listar_mensajes(node_id=node_id, limite=limite)


# ============================================================ RECURSOS (v2)

@app.get("/api/resources", response_model=list[RecursoOut], tags=["recursos"])
def recursos(node_id: str | None = None):
    """
    Ultima medicion de cada recurso: RAM, CPU, red y TODAS las unidades de
    disco, incluidas las que no son la primera (el pendrive de Santa Cruz).

    La tabla `metricas` sigue guardando solo el primer disco, que es lo que
    pide el enunciado y lo que alimenta el KPI global. Todo lo demas vive aqui.
    """
    return repo.recursos_actuales(node_id)


@app.get("/api/resources/{node_id}/history", response_model=HistorialRecursoOut,
         tags=["recursos"])
def historial_recurso(node_id: str,
                      tipo: str = Query(..., examples=["RAM"]),
                      nombre: str = Query(..., examples=["fisica"]),
                      horas: int = Query(24, ge=1, le=720),
                      limite: int = Query(500, ge=10, le=5000)):
    """Serie temporal de UN recurso. Igual que /api/history pero para lo que no
    es el primer disco."""
    tipo = tipo.upper()
    if tipo not in protocolo.TIPOS_RECURSO:
        raise HTTPException(status_code=422,
                            detail=f"Tipo invalido. Validos: "
                                   f"{', '.join(protocolo.TIPOS_RECURSO)}")
    return HistorialRecursoOut(
        node_id=node_id, tipo=tipo, nombre=nombre, horas=horas,
        puntos=repo.historial_recurso(node_id, tipo, nombre,
                                      horas=horas, limite=limite))


@app.post("/api/resources/{node_id}", response_model=RespuestaComando,
          tags=["recursos"])
def cambiar_recursos(node_id: str, cuerpo: SetRecursosIn):
    """
    Le dice a un nodo QUE debe medir, en caliente y sin entrar a esa maquina.

    Es la otra mitad de la flexibilidad: no basta con que el sistema SEPA
    guardar RAM y CPU; el operador tiene que poder pedirselo a un nodo concreto
    desde el dashboard. Se persiste en la base, asi que el nodo lo readopta
    solo cuando reconecte.
    """
    _exigir_nodo(node_id)
    pedidos = [r.strip().lower() for r in cuerpo.recursos if r.strip()]
    invalidos = [r for r in pedidos if r not in config.RECURSOS_DISPONIBLES]
    if invalidos:
        raise HTTPException(
            status_code=422,
            detail=f"Recursos desconocidos: {', '.join(invalidos)}. "
                   f"Validos: {', '.join(config.RECURSOS_DISPONIBLES)}")
    repo.actualizar_recursos_pedidos(node_id, pedidos)
    cmd_id = repo.crear_mensaje(node_id, protocolo.ACCION_SET_RECURSOS,
                                ",".join(pedidos))
    return RespuestaComando(cmd_id=cmd_id, estado="PENDIENTE",
                            detalle=f"{node_id} medira: {', '.join(pedidos)}")


@app.post("/api/sync/{node_id}", response_model=RespuestaComando,
          tags=["recursos"])
def pedir_sincronizacion(node_id: str):
    """Le pide a un nodo que entregue YA lo que tenga guardado sin sincronizar,
    sin esperar a su proximo ciclo. Util en la defensa: se corta la red, se
    vuelve a conectar y se fuerza la recuperacion en el momento."""
    _exigir_nodo(node_id)
    cmd_id = repo.crear_mensaje(node_id, protocolo.ACCION_SOLICITAR_SYNC, None)
    return RespuestaComando(cmd_id=cmd_id, estado="PENDIENTE",
                            detalle=f"Sincronizacion pedida a {node_id}")


@app.get("/api/regions", response_model=list[RegionalOut], tags=["cluster"])
def regionales():
    """
    Consolidado por REGIONAL, no por maquina.

    El enunciado habla de nueve administraciones regionales. La Paz tiene dos
    servidores, asi que "la capacidad de La Paz" es la suma de sus dos nodos.
    Sin esta vista, el dashboard mostraria La Paz dos veces y nadie sabria cual
    es el total de la regional.
    """
    return repo.listar_regionales()


# =========================================================== WEBSOCKET (v2)

@app.websocket("/ws/cluster")
async def ws_cluster(ws: WebSocket):
    """
    Estado del cluster EMPUJADO al navegador. Sin F5, sin polling.

    Que manda:
      {"tipo":"estado",  ...}  el estado completo, cuando algo cambio
      {"tipo":"latido",  "ts"} cuando no cambio nada (asi el navegador
                               distingue "todo igual" de "se corto")
      {"tipo":"error",   ...}  la API no pudo leer la base

    Al conectar se manda el estado completo de inmediato: si no, la pantalla se
    quedaria en blanco hasta el primer cambio, que puede tardar.

    El navegador no necesita mandar nada. El receive_text() de abajo esta solo
    para enterarnos de que se fue: sin esa espera, la corrutina termina y
    Starlette cierra el socket al instante.
    """
    await ws.accept()
    if not await gestor.conectar(ws):
        await ws.close(code=1013, reason="Demasiados clientes conectados")
        return
    try:
        estado = await anyio.to_thread.run_sync(_estado_inicial)
        await ws.send_text(estado)
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception as e:                                        # noqa: BLE001
        log.debug("WebSocket cerrado: %s", e)
    finally:
        await gestor.desconectar(ws)


def _estado_inicial() -> str:
    import json as _json
    from api.difusion import leer_estado
    return _json.dumps(leer_estado(), ensure_ascii=False, default=str)


# El dashboard se sirve desde la misma API: una sola URL para la demo.
#
# La ruta va calculada desde este archivo, NO relativa al directorio de trabajo.
# StaticFiles valida el directorio al importar el modulo, asi que con una ruta
# relativa lanzar uvicorn desde otra carpeta no rompe el dashboard: impide que
# la API arranque, con un error que no menciona ni uvicorn ni el proyecto.
#
# Va al final a proposito: Starlette empareja las rutas en orden, asi que todas
# las de /api, /docs y /openapi.json ganan sobre este montaje en "/".
app.mount("/", StaticFiles(directory=config.DIR_DASHBOARD, html=True), name="dashboard")
