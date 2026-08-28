# Storage Cluster CNS — Práctica 1  ·  **v2**

Monitoreo centralizado de los servidores de archivos regionales de la Caja
Nacional de Salud. Sockets TCP bidireccionales, persistencia en MySQL y
dashboard web en tiempo real.

**Stack:** Python 3.12 (socket + threading) · MySQL 8.0 · FastAPI + WebSocket ·
SQLite (en cada nodo) · HTML/JS

## Qué trae la versión 2

| | Antes (v1) | Ahora (v2) |
|---|---|---|
| Una caída de red | el hueco quedaba **para siempre** | el nodo guarda todo en su base local y **lo entrega al reconectar** |
| Refrescar el dashboard | polling cada 5 s, y F5 por las dudas | **WebSocket**: la API empuja, no se aprieta F5 |
| Hora de las métricas | la del cliente | la del **servidor**, vía reloj monotónico: cambiar la hora de un nodo no mueve nada |
| Qué se mide | solo el primer disco | disco, **todas las unidades** (pendrives), **RAM, CPU, red** — y agregar una métrica nueva es una función, no una migración |
| Nodos por regional | uno | **varios**: La Paz es un departamento con dos sedes, La Paz y El Alto |
| Dashboard | tabla y tarjetas | ordenado por pregunta, color = estado, **ranking, histograma y capacidad por regional** |
| "No reporta" | sólo eso | **"se desconectó de la red el ... porque ..."**, y se distingue el nodo intermitente |

Todo el detalle, con el porqué de cada decisión, en
[`docs/ACTUALIZACIONES.md`](docs/ACTUALIZACIONES.md).

---

## Puesta en marcha

```bash
git clone <url-del-repo> && cd storage-cluster-cns

python3.12 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env               # pegar la clave de Aiven que pasa Robert
python scripts/prueba_offline.py   # 0. 45 comprobaciones SIN MySQL
python -m db.probar_aiven          # 1. verifica DNS, puerto, TLS y esquema
python -m db.probar_bd             # 2. comprobaciones sobre la capa de datos
```

`prueba_offline.py` no necesita base de datos ni red: corre en cualquier
máquina y cubre el framing del protocolo, el fechado por reloj monotónico, el
saneado de lo que llega por el socket, la base local del cliente y los
colectores de métricas. Es la primera que hay que correr y la que más rápido
avisa si un cambio rompió algo.

Y para comprobar que el sistema completo funciona (levanta servidor y clientes
de verdad, 23 comprobaciones):

```bash
python scripts/prueba_integracion.py
```

La base de desarrollo esta en **Aiven** y ya tiene el esquema cargado: no hay
que instalar MySQL para empezar. La clave se pide por el grupo, **nunca va al
repositorio**. El `db/ca.pem` si esta en el repo: es un certificado publico,
no una credencial.

Desde el **dia 6** se pasa a MySQL local para la demo — se comenta el bloque
de Aiven en el `.env`, se descomenta el local, y:

```bash
mysql -u root -p < db/schema_local.sql
mysql -u root -p cns_cluster < db/schema.sql
python -m db.probar_bd
```

**Si la base ya tenía datos de la v1** y no los quieren perder, en vez de
`schema.sql` (que borra todo) corran la migración, que es idempotente:

```bash
mysql -u root -p cns_cluster < db/migracion_v2.sql
```

Arranque manual (en tres terminales, o todo junto con `./scripts/start_demo.sh`):

```bash
python -m servidor.main                                   # 1. sockets
uvicorn api.main:app --host 0.0.0.0 --port 8000           # 2. API + dashboard + WS
python scripts/lanzar_nodos.py --host <IP_DEL_SERVIDOR>   # 3. los nodos
```

**Sumar una computadora al clúster (la laptop de cualquiera):**

```bash
python scripts/unirse.py --host <IP_DEL_SERVIDOR>
```

Pregunta el departamento y la sede, y aparece sola en el dashboard. En el
servidor **no hay que tocar nada** (requisito 7.2). El detalle —y la diferencia
entre departamento, sede y nodo— está en
[`docs/COMO-AGREGAR-UN-NODO.md`](docs/COMO-AGREGAR-UN-NODO.md).

Un nodo real, en otra máquina, con todo lo nuevo:

```bash
python -m cliente.main --node-id CNS-SCZ-03 --region "Santa Cruz" \
       --host <IP_DEL_SERVIDOR> --recursos disco,discos,ram,cpu
```

Y para demostrar el fallo intermitente sin desenchufar un cable:

```bash
python -m cliente.main --node-id CNS-SCZ-03 --region "Santa Cruz" \
       --host <IP> --caos 25        # corta la conexión ~1 de cada 4 ciclos
python -m cliente.almacen --node-id CNS-SCZ-03   # ver qué guardó mientras tanto
```

Dashboard: `http://<IP_DEL_SERVIDOR>:8000` · API docs: `/docs`

---

## Estructura

```
storage-cluster-cns/
├── comun/           CONTRATOS COMPARTIDOS — Robert — no se tocan sin avisar
│   ├── config.py       toda la configuracion, leida del .env
│   └── protocolo.py    mensajes JSON + framing + fechado monotonico (v2)
├── db/              Robert
│   ├── schema.sql        5 tablas + 5 vistas (Aiven y local)
│   ├── migracion_v2.sql  v1 -> v2 sin borrar datos (idempotente)
│   ├── schema_local.sql  crear base y usuario, solo para MySQL local
│   ├── ca.pem            certificado de Aiven (publico, si va al repo)
│   ├── conexion.py       una conexion por hilo, con TLS si hace falta
│   ├── repositorio.py    TODO el SQL del proyecto vive aqui
│   ├── probar_aiven.py   verifica DNS, puerto, TLS y esquema
│   └── probar_bd.py      prueba de concurrencia y agregaciones
├── scripts/         Alexander
│   ├── unirse.py             une esta computadora al cluster (v2)
│   ├── prueba_offline.py     45 comprobaciones SIN MySQL (v2)
│   ├── lanzar_nodos.py       levanta los nodos
│   ├── prueba_integracion.py prueba end-to-end automatica
│   └── start_demo.sh         runbook de la demo
├── servidor/        Edwin — accept loop, watchdog, despachador
├── cliente/         Martin
│   ├── main.py         socket, dos hilos, log, ACK, sincronizacion
│   ├── metricas.py     colectores: disco, discos, RAM, CPU, red (v2)
│   └── almacen.py      base local SQLite: guarda aunque no haya red (v2)
├── api/             Robert
│   ├── main.py         FastAPI: REST + /ws/cluster
│   ├── difusion.py     empuja el estado por WebSocket (v2)
│   └── modelos.py      modelos Pydantic
├── dashboard/       Alex — tabla, KPIs, auto-refresh, mensajeria
├── docs/            protocolo.md (contrato), CAMBIOS.md (revision del codigo)
├── logs/            cliente_<node_id>.log (requisito 7.1)
└── datos/           cliente_<node_id>.db — base local de cada nodo (v2, no va al repo)
```

---

## Cómo se comunican las piezas

```
  ┌─ SQLite local          servidor de sockets                    dashboard
  │  (guarda SIEMPRE)              │                                  ▲
  cliente ──TCP:5050──────────────►│──────► MySQL ◄── FastAPI:8000 ───┤ WebSocket
     ▲       METRIC / METRIC_BATCH │           ▲         (empuja)     │ (sin F5)
     └──────── CMD ────────────────┘           │                      │
              ACK / SYNC_OK ──────────► tablas mensajes / recursos ───┘
```

La API y el servidor de sockets son **procesos separados y no comparten
memoria**. Para mandar un mensaje a un nodo, la API inserta una fila en
`mensajes` con estado `PENDIENTE`; el despachador del servidor la recoge en
menos de un segundo y la envía por el socket. La base de datos es el bus.

---

## Reglas del equipo para no romperse entre módulos

1. **`comun/protocolo.py` y `comun/config.py` son de Robert.** Si necesitás un
   campo nuevo, lo pedís en el grupo; no lo agregás por tu cuenta.
2. **Nadie escribe SQL fuera de `db/repositorio.py`.** Si te falta una consulta,
   se la pedís a Robert.
3. **Nadie llama a `mysql.connector.connect()`**: siempre `with cursor() as cur`.
4. **Nadie arma un JSON a mano**: se usan las funciones de `protocolo.py`.
5. **Rama por tarea**, merge a `dev`. `main` solo recibe código que arranca.
6. **Si no corre en LAN entre dos máquinas, no está hecho.** Localhost no cuenta.
7. **Antes de pedir merge**, corré las tres:
   `python scripts/prueba_offline.py` (no necesita nada),
   `python -m db.probar_bd` y `python scripts/prueba_integracion.py`.
   Si algo se pone en rojo, tu cambio rompió algo de otro.
8. **Para agregar una métrica nueva** (temperatura, colas de E/S, lo que sea)
   se escribe un colector en `cliente/metricas.py` con el decorador
   `@colector("nombre")` y se agrega a `RECURSOS`. **No se toca la base, ni el
   protocolo, ni el servidor, ni el dashboard.** Si estás por hacer un ALTER
   TABLE para una métrica, parate y leé `docs/ACTUALIZACIONES.md`.

> `docs/CAMBIOS.md` explica por qué varias cosas están hechas de una forma que
> parece rara. `docs/ACTUALIZACIONES.md` explica todo lo que trae la v2 y cómo
> demostrarlo en la defensa. Léelos antes de "arreglar" algo.

---

## Requisitos obligatorios y dónde están implementados

| Requisito | Dónde |
|---|---|
| 7.1 Bidireccional + log + ACK | `cliente/main.py::escuchar`, `servidor/main.py::despachador`, tabla `mensajes` |
| 7.2 Alta automática de cliente | `db/repositorio.py::registrar_nodo`, evento `ALTA_AUTOMATICA` |
| 7.3 Intervalo parametrizable | `cliente/config.json` + `CMD SET_INTERVAL` + `POST /api/config/<node_id>` |
| Solo el primer disco | `cliente/metricas.py::primer_disco` |
| Estado "No Reporta" | `servidor/main.py::watchdog` + `repositorio.marcar_nodos_caidos` |
| Exactamente 9 clientes | `MAX_NODOS` en `.env`, control en `atender_cliente` |
| **v2** Sincronización tras caída | `cliente/almacen.py` + `METRIC_BATCH` + `repositorio.guardar_lote` |
| **v2** Hora del cliente no manipulable | `protocolo.fechar_muestra` + `mono_ns` |
| **v2** Sin F5 (WebSocket) | `api/difusion.py` + `/ws/cluster` + fallback REST en el dashboard |
| **v2** RAM/CPU/red/pendrive | `cliente/metricas.py::COLECTORES` + tabla `recursos` |
| **v2** Varios servidores por regional | `v_regionales`, `GET /api/regions` |
| **v2** "Se desconectó el ..." | `nodos.ultima_desconexion` + `repositorio.registrar_desconexion` |
| **v2** Fallo intermitente | hilo `latido` (PING) + `repositorio.marcar_intermitentes` |


---

## Quién edita qué

| Carpeta | Dueño |
|---|---|
| `comun/` `db/` `api/` `docs/` | Robert |
| `cliente/` | Martin |
| `servidor/` | Edwin |
| `dashboard/` | Alex |
| `scripts/` + presentación | Alexander |

Nadie edita la carpeta de otro. Si necesitás algo de ahí, lo pedís en el grupo.

## Qué nunca se commitea

`.env` · contraseñas · `logs/*.log` · `.venv/` · `__pycache__/`

Todo eso ya está en el `.gitignore`. El `db/ca.pem` **sí** va al repositorio:
es un certificado público que Aiven publica en su panel, no una credencial.
