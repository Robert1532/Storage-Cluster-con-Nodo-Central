# Revisión del código base — qué se arregló y cómo se comprobó

Antes de repartir el proyecto, el código base pasó por una revisión completa y
por pruebas ejecutadas contra un **MySQL 8.0.46 real**. Este documento existe
por dos razones: para que nadie "arregle" algo que ya está resuelto de una
forma concreta, y porque varias de estas decisiones son respuestas directas a
las preguntas de la defensa.

Estado actual, todo verificado ejecutándolo:

| Prueba | Resultado |
|---|---|
| `python -m db.probar_bd` | **45/45** contra MySQL 8.0.46 |
| `python scripts/prueba_integracion.py` | **23/23** end-to-end, con procesos reales |
| API + dashboard (20 comprobaciones) | **20/20** |
| Framing del protocolo (8 casos raros) | **8/8** |

---

## Lo que estaba mal y ahora no

### 1. La hora se guardaba desplazada y las consultas por tiempo salían vacías

`ahora_iso()` mandaba la hora **sin offset de zona**, y el servidor la
interpretaba con SU zona horaria. Como la base trabaja en UTC y Bolivia es
UTC-4, cada métrica quedaba cuatro horas "en el pasado". Consecuencia:
`historial()` y `crecimiento()` devolvían **0 puntos** con la tabla llena,
porque filtran con `WHERE timestamp >= NOW() - INTERVAL n HOUR`.

Ahora el timestamp viaja con offset (`…-04:00`) y la conversión a UTC no
depende de dónde corra el servidor.

### 2. El pool de conexiones serializaba lo que debía paralelizar

`MySQLConnectionPool` toma un **lock global del proceso** en cada
`get_connection()` y, mientras lo tiene, hace un PING al servidor. Contra una
base local eso no se nota; contra Aiven cada PING es un viaje de ida y vuelta
completo y los nueve hilos hacen fila de a uno. Medido: **11 inserciones en 20
segundos** con 9 hilos.

Ahora hay **una conexión por hilo** (`threading.local`), sin lock global. Un
hilo que termina cierra la suya (`cerrar_conexion_del_hilo()`), o quedaría
colgada del lado del servidor.

### 3. El gráfico histórico mostraba datos de hace 22 horas

`historial()` hacía `ORDER BY timestamp ASC LIMIT 500`, que recorta por el
**principio** de la ventana. Con 9 nodos cada 10 s hay 8.640 filas en 24 h: el
gráfico dibujaba las 500 más viejas y terminaba hace casi un día. No fallaba,
no avisaba: dibujaba una curva plausible y vieja.

Ahora toma las más nuevas en una subconsulta y las reordena para dibujar.

### 4. `v_ultima_metrica` recorría la tabla entera

Usaba `ROW_NUMBER()` sobre todo el histórico, así que MySQL materializaba
150.000 filas para quedarse con nueve, y el índice no se usaba nunca. Medido:
**~900 ms por consulta**, y el dashboard pide dos cada pocos segundos.

Ahora arranca desde `nodos` (9 filas) y busca la última métrica de cada uno por
índice: **~10 ms**. Lleva un `FORCE INDEX` a propósito, porque sin él el
optimizador prefiere recorrer el índice de fechas hacia atrás y eso se degrada
justo con el nodo caído (154 ms contra 62 ms, y la diferencia crece).

### 5. Dos condiciones de carrera reales

- **Alta de nodo**: era `SELECT` y después `INSERT`. Dos hilos que veían "no
  existe" a la vez provocaban un `Duplicate entry` que mataba el hilo del
  cliente. Reproducido 3 de 3 veces. Ahora es un solo
  `INSERT … ON DUPLICATE KEY UPDATE`.
- **Watchdog**: seleccionaba los candidatos y después los marcaba sin volver a
  comprobar la condición. Un nodo que reportaba justo en el medio quedaba
  marcado caído, con un evento falso que inflaba los `failover_events` que se
  muestran en la defensa. Ahora la condición va dentro del `UPDATE`.

### 6. Dos hilos escribiendo en el mismo socket corrompían mensajes

El despachador manda un `CMD` mientras el hilo del cliente manda `METRIC_OK`;
en el cliente, el receptor manda un `ACK` mientras el principal manda `METRIC`.
`sendall()` puede escribir de a partes, así que los bytes se intercalan.
Medido con un receptor lento: **se perdieron 148 de 600 mensajes**.

Ahora hay un candado **por socket** (no uno global). Con él, 600 de 600.

### 7. El cliente se moría con traceback

- `leer_disco()` lanzaba `RuntimeError` sin `/proc/diskstats`, y
  `ZeroDivisionError` con una unidad vacía (lector de tarjetas en Windows).
  El bucle solo capturaba `OSError`.
- Cerrar un socket **no desbloquea** un `recv()` en curso, así que cada
  reconexión dejaba un hilo receptor huérfano; al despertar, apagaba la
  conexión nueva.

Ahora `leer_disco()` nunca lanza, el bucle captura todo, y cada conexión tiene
su propio estado (`Sesion`) con `shutdown()` antes de `close()`.

### 8. El cliente tardaba un intervalo entero en notar una caída

El receptor detectaba el corte pero no despertaba al emisor, que seguía dormido
en `wait(intervalo)`. Con intervalo de 60 s, el nodo tardaba hasta un minuto en
reconectar y aparecía como `NO_REPORTA` sin necesidad.

### 9. La API se comía las conexiones de Aiven

Los endpoints son síncronos, así que corren en el pool de hilos de Starlette
—**40 por defecto**—, y cada hilo abría su propia conexión. Sumado al servidor
de sockets y a cinco personas contra la misma base, eso agota el plan gratuito.
Ahora el arranque baja ese límite a `API_HILOS` (6).

### 10. XSS almacenado en el dashboard

`region`, `hostname` y `disco_nombre` los manda el **cliente** por el socket y
se pintaban con `innerHTML` sin escapar. Cualquiera en la LAN podía conectarse
al puerto 5050 y registrarse con una región que contuviera código, y ese código
se ejecutaba en el navegador de todos. Ahora todo pasa por una función de
escapado.

### 11. Otros, más chicos pero reales

- `uvicorn` no arrancaba si se lanzaba desde otra carpeta (`StaticFiles` con
  ruta relativa). No fallaba el dashboard: fallaba el import del módulo.
- Los `Decimal` se serializaban como **string** en unos endpoints y como número
  en otros. En JavaScript, `"1000.00" + "500.00"` da `"1000.00500.00"`.
  Ahora todos los modelos declaran `float`.
- `POST /api/message` con un nodo inexistente daba **500**; ahora da 404.
  Pasa de verdad: si se abre el dashboard antes de levantar los nodos, el
  selector queda vacío.
- `GET /api/history` daba **404** cuando un nodo aún no tenía métricas, que es
  el estado normal al arrancar. Ahora devuelve una lista vacía.
- `start_demo.sh` imprimía "todo arriba" aunque la API se hubiera muerto:
  `set -e` no detecta procesos en segundo plano. Ahora comprueba cada uno.
- `lanzar_nodos.py` no atendía `SIGTERM`, así que matarlo dejaba los 9 clientes
  vivos reintentando contra un servidor que ya no existía.
- Datos del cliente sin validar: un `disco_tipo` fuera del ENUM, IOPS negativos
  (pasa de verdad cuando los contadores del sistema se reinician) o una latencia
  fuera de rango eran **errores** en MySQL estricto, no avisos, y mataban el
  hilo. Ahora se sanean antes de insertar.

---

## Decisiones que parecen raras y son a propósito

No las "arreglen" sin hablarlo: cada una tiene un motivo y varias son respuesta
directa a una pregunta de la defensa.

| Decisión | Por qué |
|---|---|
| Una conexión por hilo en vez de un pool | El pool serializa contra la nube (punto 2) |
| El watchdog es el único que escribe `estado` | Así toda transición deja su evento en la bitácora |
| El ACK se manda **antes** de ejecutar el comando | Si se manda después, un fallo lo deja sin confirmar para siempre |
| El mensaje se marca ENVIADO **antes** de enviarlo | Un fallo produce una pérdida visible en vez de un duplicado silencioso |
| `FORCE INDEX` en `v_ultima_metrica` | Sin él el plan se degrada justo con el nodo caído |
| `DECIMAL` y no `FLOAT` en la base | Sumar nueve FLOAT ensucia el % de utilización global |
| `float` y no `Decimal` en los modelos de la API | Para que el navegador reciba números, no strings |
| La tabla `mensajes` como bus entre la API y los sockets | Son dos procesos: no comparten memoria |

---

## Cómo comprobar que sigue todo bien

Después de cualquier cambio de peso, estos dos comandos:

```bash
python -m db.probar_bd              # 45 comprobaciones sobre la capa de datos
python scripts/prueba_integracion.py  # 23 comprobaciones end-to-end con procesos reales
```

La segunda levanta el servidor y clientes de verdad y comprueba el alta
automática, el ciclo del mensaje con su ACK, el cambio de intervalo en
caliente, la caída y recuperación de un nodo, la reconexión tras caerse el
servidor, el rechazo del nodo sobrante y que no queden conexiones colgadas.
Cubre buena parte de la tarea 5.2.
