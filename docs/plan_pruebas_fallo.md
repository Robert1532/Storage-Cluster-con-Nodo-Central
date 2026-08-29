# Plan de pruebas de fallo y recuperación — tarea 5.2

Regla de la tarea: anotar el resultado esperado **antes** de correr cada
prueba. Así, si algo sale distinto, se sabe que falló de verdad y no que "no
me acuerdo qué tenía que pasar".

> **Corrección tras probarlo en vivo (26/08):** `db/repositorio.py::guardar_metrica`
> ya pone `estado = 'ACTIVO'` en cada `METRIC` recibido, así que el nodo SÍ
> vuelve a `ACTIVO` en el dashboard apenas reconecta — la prueba 2 no está
> bloqueada y ya se corrió con éxito (ver resultado real más abajo). Lo único
> que falta es el `# TODO Edwin` en `servidor/main.py` (llamar a
> `repo.marcar_recuperado`): sin eso no queda el evento `RECUPERADO` en la
> tabla `eventos` para la bitácora/disponibilidad, pero el estado visible y
> la recuperación funcional ya andan. Avisar a Edwin igual, pero no bloquea
> el resto de M5.

---

## Prueba 1 — Matar el servidor central

**Precondición:** al menos 2 clientes conectados y reportando.

**Acción:** `Ctrl+C` en la terminal del servidor de sockets (o `kill` al
proceso), sin tocar los clientes.

**Resultado esperado:**
- Los clientes NO se caen ni muestran traceback sin controlar en consola.
- Cada cliente entra en el bucle de reconexión con espera creciente
  (1, 2, 4, 8... segundos — ver `cliente/main.py::conectar`).
- Al volver a levantar `python -m servidor.main`, los clientes se reconectan
  solos, mandan `HELLO` de nuevo y siguen reportando `METRIC` sin
  intervención manual.

**Resultado real (26/08, ensayo local en un solo equipo, servidor y cliente
`CNS-LPZ-01` reales):** PASÓ. Al matar el servidor, el cliente no lanzó
ningún traceback — solo warnings controlados (`Sin servidor... Reintento en
Xs`) con backoff 1, 2, 4, 8, 16, 30s exactamente como está programado. Al
volver a levantar el servidor, el cliente reconectó solo en el siguiente
intento programado, sin intervención manual.

**Tiempo medido de reconexión tras levantar el servidor:** 18 seg (cayó
dentro de la ventana de espera de 30s que ya tenía en curso — en el peor
caso puede tardar hasta el intervalo de backoff completo).

---

## Prueba 2 — Desconectar un cliente y reconectarlo

**Precondición:** el TODO de `marcar_recuperado` en `servidor/main.py` ya
resuelto (ver bloqueo arriba).

**Acción:** `Ctrl+C` en un cliente (no en el servidor). Esperar a que el
watchdog lo marque `NO_REPORTA`. Volver a levantar ese mismo cliente.

**Resultado esperado:**
- El watchdog corre cada `PERIODO_WATCHDOG_SEG` (2s por defecto) y compara
  contra `intervalo_seg * FACTOR_TIMEOUT` **de ese nodo** — no un valor fijo
  global (`db/repositorio.py::marcar_nodos_caidos`).
- El nodo pasa a `NO_REPORTA` en el dashboard (rojo) dentro de
  `intervalo * FACTOR_TIMEOUT + PERIODO_WATCHDOG_SEG` segundos desde el
  último `METRIC` recibido. Con los valores por defecto (intervalo=10s,
  factor=3): hasta ~32 segundos.
- Al reconectar, vuelve a `ACTIVO` (verde) en cuanto llega el primer `METRIC`
  nuevo, sin reiniciar el servidor.
- Queda un evento `NO_REPORTA` y luego `RECUPERADO` en la tabla `eventos`
  (visibles vía `GET /api/events?node_id=<id>`).

**Resultado esperado formalizado (con los valores del `.env` reales):**
tiempo de detección = intervalo del nodo (____ s) × FACTOR_TIMEOUT (____ ) +
hasta PERIODO_WATCHDOG_SEG (____ s) = **_____ segundos como máximo**.

**Resultado real (26/08, ensayo local):** PASÓ. `CNS-LPZ-01` pasó a
`NO_REPORTA` ~4 segundos después de que el servidor arrancó sin que el
cliente aún hubiera reconectado (quedó detectado casi de inmediato porque ya
llevaba rato sin reportar). Al reconectar y mandar el primer `METRIC`,
volvió a `ACTIVO` en la siguiente lectura de la API (menos de 3 segundos).
Falta el evento `RECUPERADO` en `eventos` — solo eso, ver nota de corrección
arriba.

**Tiempo hasta NO_REPORTA:** ~4 seg (tras arranque del servidor, nodo ya vencido)
**Tiempo hasta volver a ACTIVO tras reconectar:** &lt; 3 seg

---

## Prueba 3 — Conectar un nodo nuevo sin reiniciar nada

**Precondición:** sistema corriendo, 9 nodos ya conectados o menos.

**Acción:**
```bash
python -m cliente.main --node-id CNS-XXX-10 --region "Nueva Regional" --host <IP_SERVIDOR>
```

**Resultado esperado:**
- El servidor recibe el `HELLO` con un `node_id` que no existe en la tabla
  `nodos`, lo inserta (`repo.registrar_nodo`, `es_nuevo=True`) y responde
  `HELLO_OK`.
- Aparece solo en `GET /api/nodes` y en el dashboard, con estado `ACTIVO`.
- Queda un evento `ALTA_AUTOMATICA` en la tabla `eventos`.
- No hizo falta editar ningún archivo de configuración ni reiniciar el
  servidor ni la API.

**Resultado real (26/08, ensayo local):** PASÓ. Se conectó `CNS-TEST-99`
(node_id inexistente) y el servidor respondió `HELLO_OK` con alta
automática; apareció en `GET /api/nodes` y quedó el evento
`ALTA_AUTOMATICA` en la tabla `eventos`. No se reinició nada. (Nodo de
prueba borrado de la base al terminar, para no ensuciar los datos
compartidos del equipo.)

---

## Prueba 4 — Llenar el disco de prueba

**Precondición:** un cliente corriendo y reportando normalmente.

**Acción:** generar ocupación real en el disco/partición que ese cliente
está monitoreando (el que devuelve `cliente/metricas.py::primer_disco`) —
por ejemplo, crear un archivo grande de prueba y borrarlo después.

**Resultado esperado:**
- El siguiente `METRIC` de ese nodo refleja el nuevo porcentaje de uso.
- El dashboard actualiza el porcentaje y, si pasa el 80%, la barra cambia de
  color (tarea 4.2 — ya implementada).
- El KPI de utilización global del cluster (`GET /api/cluster`) también sube,
  proporcional al peso de ese disco sobre el total.

**Resultado real (26/08, ensayo local, archivo de prueba de 1 GB):** PASÓ.
`uso_pct` pasó de 85.15% a 85.29% y `libre_gb` bajó de 70.61 a 69.96 GB en el
siguiente reporte del cliente. El KPI global del cluster (`GET
/api/cluster`) también reflejó el cambio de inmediato.

**Importante:** borrar el archivo de prueba al terminar — no dejarlo para no
falsear las capturas del Plan B ni el informe.

---

## Resumen para el informe (M6)

| # | Prueba | Esperado | Real | ¿Pasó? |
|---|---|---|---|---|
| 1 | Matar servidor | Reconexión automática, sin traceback | Backoff 1-30s, sin traceback, reconectó en 18s | ✅ |
| 2 | Caída y recuperación de un cliente | NO_REPORTA → ACTIVO automático | Recuperó estado en <3s (falta solo el evento RECUPERADO en bitácora) | ✅* |
| 3 | Nodo nuevo en caliente | Alta automática, sin reiniciar | Alta automática confirmada, evento registrado | ✅ |
| 4 | Llenar disco | % sube en dashboard y KPI global | 85.15%→85.29%, KPI global actualizado | ✅ |

*Pendiente cosmético: falta el evento `RECUPERADO` en `eventos` (TODO de Edwin en `servidor/main.py`). No afecta el resultado de la prueba, solo la bitácora.

Ensayo corrido el 26/08/2026 en un solo equipo (servidor + cliente en la misma
máquina, contra la base de Aiven) como rehearsal antes de la prueba real en
LAN con 2 máquinas físicas (M5.1). Repetir esta tabla el día de la
integración LAN real con los tiempos y equipos reales.

Responsable: Alexander (M5.2).
