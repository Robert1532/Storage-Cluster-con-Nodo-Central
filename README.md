# Storage Cluster CNS — Práctica 1

Monitoreo centralizado de los 9 servidores de archivos regionales de la Caja
Nacional de Salud. Sockets TCP bidireccionales, persistencia en MySQL y
dashboard web.

**Stack:** Python 3.12 (socket + threading) · MySQL 8.0 · FastAPI · HTML/JS

---

## Puesta en marcha

```bash
git clone <url-del-repo> && cd storage-cluster-cns

python3.12 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env               # pegar la clave de Aiven que pasa Robert
python -m db.probar_aiven          # 1. verifica DNS, puerto, TLS y esquema
python -m db.probar_bd             # 2. 45 comprobaciones sobre la capa de datos
```

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

Arranque manual (en tres terminales, o todo junto con `./scripts/start_demo.sh`):

```bash
python -m servidor.main                                   # 1. sockets
uvicorn api.main:app --host 0.0.0.0 --port 8000           # 2. API + dashboard
python scripts/lanzar_nodos.py --host <IP_DEL_SERVIDOR>   # 3. los 9 nodos
```

Dashboard: `http://<IP_DEL_SERVIDOR>:8000` · API docs: `/docs`

---

## Estructura

```
storage-cluster-cns/
├── comun/           CONTRATOS COMPARTIDOS — Robert — no se tocan sin avisar
│   ├── config.py       toda la configuracion, leida del .env
│   └── protocolo.py    mensajes JSON + framing por linea (tarea 0.3)
├── db/              Robert
│   ├── schema.sql        4 tablas + 3 vistas (Aiven y local)
│   ├── schema_local.sql  crear base y usuario, solo para MySQL local
│   ├── ca.pem            certificado de Aiven (publico, si va al repo)
│   ├── conexion.py       una conexion por hilo, con TLS si hace falta
│   ├── repositorio.py    TODO el SQL del proyecto vive aqui
│   ├── probar_aiven.py   verifica DNS, puerto, TLS y esquema
│   └── probar_bd.py      prueba de concurrencia y agregaciones
├── scripts/         Alexander
│   ├── lanzar_nodos.py       levanta los 9
│   ├── prueba_integracion.py prueba end-to-end automatica
│   └── start_demo.sh         runbook de la demo
├── servidor/        Edwin — accept loop, watchdog, despachador
├── cliente/         Martin — metricas de disco, socket, log, ACK
├── api/             Robert — FastAPI + modelos Pydantic
├── dashboard/       Alex — tabla, KPIs, auto-refresh, mensajeria
├── docs/            protocolo.md (contrato), CAMBIOS.md (revision del codigo)
└── logs/            cliente_<node_id>.log (requisito 7.1)
```

---

## Cómo se comunican las piezas

```
  cliente ──TCP:5050──► servidor de sockets ──► MySQL ◄── FastAPI:8000 ◄── dashboard
     ▲                          │                  ▲
     └──────── CMD ─────────────┘                  │
              ACK ──────────────────────────► tabla mensajes
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
7. **Antes de pedir merge**, corré `python -m db.probar_bd` y
   `python scripts/prueba_integracion.py`. Si algo se pone en rojo, tu cambio
   rompió algo de otro.

> `docs/CAMBIOS.md` explica por qué varias cosas están hechas de una forma que
> parece rara. Léelo antes de "arreglarlas".

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
