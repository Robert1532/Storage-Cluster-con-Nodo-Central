# Actualizaciones de la versión 2 — qué cambió y por qué

Este documento recoge todo lo que se agregó al Storage Cluster CNS después de
la revisión inicial (`docs/CAMBIOS.md`). Está escrito para dos lectores: el
equipo, que necesita saber qué se movió antes de tocar nada, y el tribunal, que
va a preguntar *por qué* está hecho así y no de otra forma.

**Regla de oro que se respetó en todo el trabajo:** nada de lo que ya cumplía
el enunciado se rompió. La tabla `metricas` sigue guardando **sólo el primer
disco**, `v_cluster` sigue calculando el KPI global igual, y el límite de
clientes se sigue aplicando. Todo lo nuevo se agregó **al lado**, no encima.

---

## 1. Resumen

| # | Qué se pidió | Qué hay ahora |
|---|---|---|
| 1 | La Paz puede tener 2 servidores | La región dejó de ser la identidad del nodo. Vista `v_regionales` y dashboard agrupado por regional con subtotales. |
| 2 | Agregar una computadora nueva | Ya funcionaba (requisito 7.2); ahora el límite es un parámetro y no un número escrito en el código. |
| 3 | Mostrar la regional en vez de la IP | La tarjeta muestra regional y `node_id`; la IP pasó al detalle. |
| 4 | Fallo intermitente | Tres capas de detección + marca `intermitente` + backoff con jitter + modo `--caos` para demostrarlo en vivo. |
| 5 | WebSocket, no dar F5 | `/ws/cluster`: la API empuja el estado. Fallback automático a REST si el socket no se puede abrir. |
| 6 | No permitir el cambio de hora del cliente | La hora la calcula el servidor con el reloj monotónico del cliente. Cambiar la fecha de un nodo no mueve ni una fila. |
| 7 | "Se desconectó de la red el ..." | Fecha y motivo en la fila del nodo, visibles en la tarjeta y en el detalle. |
| 8 | El cliente tiene su propia BD / log | SQLite por nodo: muestras + bitácora consultable. El `.log` de texto se sigue escribiendo (lo pide el enunciado). |
| 9 | Guarda el comportamiento del disco | Toda medición se guarda localmente, se haya podido enviar o no. |
| 10 | Al reconectar, sincroniza lo perdido | `METRIC_BATCH` + `SYNC_OK` acumulativo + `seq` idempotente. |
| 11 | Un pendrive aumenta la capacidad | Se reportan todas las unidades; el servidor detecta solo el alta, la baja y el cambio de capacidad. |
| 12 | Flexible: RAM u otra información | Registro de colectores + tabla `recursos` con JSON. Agregar una métrica es escribir una función. |
| 13 | Departamento vs sede | La Paz es un **departamento** con dos **sedes** (La Paz y El Alto), cada una con su servidor. |
| 14 | Dashboard más intuitivo | Reordenado por pregunta, color = estado en toda la pantalla, y la mitad de alto. |
| 15 | Histogramas | Ranking por servidor, distribución del clúster y capacidad por regional. |
| 16 | Sumar una laptop | `python scripts/unirse.py --host <IP>` y aparece sola en el dashboard. |

**Verificación:** `scripts/prueba_offline.py` — 45 comprobaciones, sin MySQL,
todas en verde. Las otras dos pruebas (`db.probar_bd`, `prueba_integracion`)
siguen existiendo y se ampliaron.

---

## 2. Los cambios, uno por uno

### 2.1 Una regional puede tener varios servidores

**El problema.** El código trataba la región como si fuera la identidad del
nodo. Con dos servidores en La Paz, el dashboard mostraba "La Paz" dos veces,
sin subtotal, y no había forma de responder *"cuánto almacenamiento tiene la
regional La Paz"*.

**Lo que hay ahora.** El `node_id` es lo único único (`uq_node_id`); `region`
tiene un índice normal y se repite sin problema. Se agregó la vista
`v_regionales`, el endpoint `GET /api/regions` y una sección por regional en el
dashboard, con su capacidad, usado, libre y % de utilización.

**Por qué así.** El enunciado habla de **nueve administraciones regionales**, no
de nueve máquinas. Una regional grande no corre con un solo servidor, y ese es
justamente el caso que el sistema tenía que poder representar.

**El % de la regional sale de los totales, no del promedio de sus nodos.** Un
servidor de 400 GB al 25% y otro de 200 GB al 25% dan 25% global; pero uno de
400 GB al 10% y otro de 200 GB al 70% dan 30%, no 40%. Promediar porcentajes de
volúmenes distintos da un número que no significa nada. Hay una comprobación
específica de esto en `db/probar_bd.py`.

---

### 2.2 El límite de nodos es un parámetro

`MAX_NODOS` pasó de 9 a **12 por defecto**, leído del `.env`.

**Esto no incumple el enunciado.** El servidor sigue rechazando al nodo que
sobra con un `ERROR` explícito, y el control sigue estando dentro de una única
sección crítica para que dos hilos que aceptan el nodo 9 y el 10 a la vez no
puedan pasar los dos. Para demostrar "soporte exacto para 9 clientes" en la
defensa basta con `MAX_NODOS=9` en el `.env`.

**Por qué se cambió el valor por defecto.** Con nueve plazas y La Paz ocupando
dos, sólo quedan ocho regionales, y no queda sitio para la computadora que se
agrega en caliente durante la demo. Un límite que hay que editar en el código
para hacer la propia demostración es un límite mal puesto.

---

### 2.3 La hora la pone el servidor

**El problema.** Cualquier nodo podía escribir en el histórico del cluster la
hora que se le antojara. Bastaba con cambiar la fecha de esa máquina — a
propósito o por accidente, que es lo más común — para que sus métricas
aparecieran en el futuro, en el pasado, o directamente fuera de la ventana de
las consultas por tiempo.

**Lo que hay ahora.** Cada muestra viaja con `mono_ns`: el reloj **monotónico**
del proceso cliente. Ese reloj no lo afecta ningún ajuste de la hora del
sistema, ni NTP, ni el usuario. Sólo avanza. El mensaje lleva además
`mono_envio_ns`, el mismo reloj en el instante del envío. El servidor calcula:

```
edad_de_la_muestra = (mono_envio_ns − mono_ns) / 1e9
timestamp_guardado = hora_de_ESTE_servidor − edad_de_la_muestra
```

Implementado en `protocolo.fechar_muestra()`.

**Por qué el reloj monotónico y no simplemente "la hora de llegada".** Porque
un lote que llega dos horas tarde tiene muestras de hace dos horas. Si se
fecharan todas en el instante de llegada, el gráfico dibujaría una línea
vertical de 800 puntos en un mismo segundo, y el growth rate saldría absurdo.
Con la edad relativa, el lote queda **repartido** exactamente en las dos horas
que cubre.

**Por qué se sigue mandando la hora del cliente.** Porque tirarla sería perder
información útil: se guarda en `metricas.t_cliente` para auditoría y se usa para
calcular el desvío, que queda en `nodos.desvio_reloj_seg` y deja un evento
`RELOJ_DESVIADO`. El operador se entera de que esa máquina tiene la hora mal —
que es un problema real que hay que arreglar — sin que los datos se vean
afectados mientras tanto.

**Guardas.** Una edad negativa o mayor a un mes se descarta y la muestra se
fecha como "ahora": es la suposición menos dañina. El cliente además vigila su
propio reloj comparando `time.time()` contra `time.monotonic()`, y si detecta
un salto lo anota en su bitácora.

**Cómo se demuestra en 30 segundos:** se cambia la hora del sistema del nodo una
hora hacia atrás, y las métricas siguen apareciendo en el instante correcto en
el dashboard. Aparece la etiqueta `RELOJ -3600s` en la tarjeta y un evento
`RELOJ_DESVIADO` en la bitácora.

---

### 2.4 El cliente tiene su propia base de datos

**El problema.** Si el nodo perdía la red, sus mediciones se perdían: medía, no
podía enviar, y tiraba el dato. El hueco en el dashboard era **permanente**.
Para un sistema que monitorea historiales clínicos eso es exactamente al revés
de lo que hace falta: justo cuando algo va mal es cuando más importa saber qué
estaba pasando en el disco.

**Lo que hay ahora.** `cliente/almacen.py` — un SQLite por nodo, en
`datos/cliente_<node_id>.db`, con tres tablas:

| tabla | qué guarda |
|---|---|
| `muestras` | el comportamiento del disco, la RAM, la CPU y la red en el tiempo |
| `bitacora` | qué le pasó al nodo: mensajes del servidor, cortes, reconexiones, cambios de intervalo, pérdidas por buffer lleno |
| `estado` | pares clave/valor que sobreviven a un reinicio |

**La medición se guarda ANTES de intentar enviarla.** Ese orden es todo el
punto: cuando el cliente descubre que no hay red, el dato ya está a salvo en
disco.

**Por qué SQLite y no un archivo de texto.** Un archivo no sobrevive a un corte
de luz a mitad de escritura, no se puede consultar por rango, y borrar las
primeras N líneas obliga a reescribirlo entero. SQLite da transacciones, un
índice sobre lo pendiente y poda con un `DELETE`. Y viene en la biblioteca
estándar: no agrega ni una dependencia.

**El `.log` de texto se sigue escribiendo.** El requisito 7.1 lo pide con esas
palabras. La bitácora de SQLite es un añadido, no un reemplazo.

**La poda tiene un caso feo, y es a propósito.** Si el nodo pasa días sin red,
en algún momento hay que elegir entre perder lo más viejo o llenarle el disco al
hospital. Se pierde lo más viejo — y **queda anotado** en la bitácora, para que
nadie descubra el hueco por casualidad tres semanas después. Con los valores por
defecto (20.000 muestras, intervalo de 10 s) un nodo aguanta unas **55 horas**
de corte sin perder nada.

---

### 2.5 Sincronización al reconectar

**Cómo funciona.**

1. Al conectar, el `HELLO` lleva `pendientes`: cuántas muestras trae guardadas.
   El servidor lo registra y el dashboard lo muestra. Se sabe **cuánto se
   perdió en el momento mismo de la reconexión**, no después.
2. El cliente manda `METRIC_BATCH` con hasta 100 muestras, en orden.
3. El servidor las fecha (§2.3), las inserta marcadas `origen='SYNC'` y
   responde `SYNC_OK` con `hasta_seq`.
4. El cliente marca entregado todo lo `<=` a ese número y manda el siguiente
   lote.

**Por qué el ACK es acumulativo.** Porque así un `SYNC_OK` perdido no pierde
datos: el lote entero se reenvía la próxima vez y el servidor lo descarta por
duplicado, sin insertar nada.

**Por qué hace falta el `seq`.** Es la idempotencia. Sin él, una reconexión con
mala suerte duplica horas de histórico y el growth rate sale al doble. El `seq`
vive en la base local del cliente con `AUTOINCREMENT`, así que **nunca se
reutiliza**, ni siquiera después de podar. El servidor guarda `nodos.ultima_seq`
y descarta todo lo que sea menor o igual. Hay una comprobación que reenvía el
mismo lote dos veces y verifica que la segunda inserte cero filas.

**Por qué se espera el `SYNC_OK` antes del siguiente lote.** Es control de
flujo. Un nodo que estuvo un día sin red tiene ~8.600 muestras; mandarlas de
golpe le mete al servidor 8.600 `INSERT` en un solo mensaje mientras los otros
ocho nodos siguen reportando normal.

**Por qué el backlog se drena en paralelo con las métricas en vivo.** Si el
cliente sincronizara primero y recién después volviera al tiempo real, un nodo
con dos días de atraso tardaría minutos en volver a aparecer "vivo" en el
dashboard. El ciclo manda **un lote atrasado, después la métrica de ahora**: el
tiempo real vuelve de inmediato y el hueco se rellena por detrás.

**Se distingue el dato recuperado del dato en vivo.** `metricas.origen` vale
`VIVO` o `SYNC`, y el dashboard lo marca. Un hueco relleno no es lo mismo que un
dato que llegó en su momento, y no debería parecerlo.

---

### 2.6 Fallos intermitentes

Un cable a medio enchufar, un wifi que va y viene o un NAT que expira **no
producen un `FIN` de TCP**. Ahora hay tres capas:

| capa | qué detecta | en cuánto |
|---|---|---|
| timeout de `recv` | que **no llega** nada | `FACTOR_TIMEOUT × intervalo` |
| **`PING` de aplicación** | que **no se puede escribir**: el camino de ida está cortado | 15 s |
| watchdog sobre la base | que el nodo dejó de reportar, haya socket o no | 2 s |

**Por qué hacía falta el `PING`.** Las otras dos no cubren el caso asimétrico:
el nodo manda métricas y el servidor las recibe, pero sus `CMD` no llegan a
ninguna parte. Sin el latido, eso se descubre recién cuando alguien manda un
mensaje desde el dashboard y nunca vuelve el ACK. Escribir en el socket es lo
único que prueba que el camino de ida funciona. (El keepalive de TCP no sirve:
en Linux tarda **dos horas** por defecto en dispararse.)

**Marca `intermitente`.** Un nodo que se cae y vuelve 3 veces en 10 minutos se
marca aparte. En un dashboard donde sólo hay dos colores, el que parpadea se ve
verde la mitad del tiempo y no lo mira nadie.

**Backoff con jitter.** La reconexión pasó de `1, 2, 4, 8, 16, 30 s` a esos
mismos valores multiplicados por un factor aleatorio entre 0,7 y 1,3. Sin el
jitter, nueve nodos que se cayeron juntos porque se reinició el servidor
reintentan todos en el mismo instante, una y otra vez: nueve conexiones
simultáneas cada vez en lugar de repartidas. Es el *thundering herd*.

**Modo `--caos`.** El cliente acepta `--caos 25`, que corta la conexión con esa
probabilidad en cada ciclo. Permite demostrar el fallo intermitente y la
sincronización posterior **sin desenchufar un cable**, que en una defensa de
diez minutos importa.

---

### 2.7 "Se desconectó de la red el ..."

Antes esta información sólo existía como una fila más en `eventos`, y el
dashboard no tenía de dónde sacarla sin recorrer toda la bitácora. Ahora
`nodos` tiene `ultima_desconexion`, `motivo_desconexion` y `ultima_reconexion`:
una lectura por clave primaria.

Se registran **los dos tipos de caída**:

- **con cierre** (el cliente se apagó, el socket dio error): lo escribe
  `atender_cliente` en su `finally`, con el motivo real — timeout, error de
  red, protocolo violado.
- **silenciosa** (cable cortado, wifi caído): la escribe el watchdog cuando
  marca `NO_REPORTA`, fechándola en el **último reporte** y no en el momento en
  que se dio cuenta.

En la tarjeta se ve *"Se desconectó de la red el jueves 27 de agosto, 14:32 —
Sin datos dentro del umbral"*.

---

### 2.8 Métricas flexibles: RAM, CPU, red y el pendrive

**El problema.** `cliente/metricas.py` medía una cosa: el primer disco. Agregar
la RAM habría significado tocar el protocolo, el esquema, el repositorio, la
API y el dashboard — cinco archivos de tres personas distintas.

**Lo que hay ahora: un registro de colectores.**

```python
@colector("ram")
def _colector_ram():
    return [protocolo.recurso("RAM", "fisica", {"total_gb": …, "uso_pct": …})]
```

Para que el sistema mida algo nuevo — la temperatura, la cola de E/S, los
sensores de una UPS — se escribe una función de diez líneas y se agrega su
nombre a `RECURSOS`. **No hay que tocar el protocolo, ni el servidor, ni la
base de datos, ni el dashboard.** El recurso viaja como `{tipo, nombre,
metricas}` y se guarda con su JSON.

Vienen cinco de fábrica: `disco` (el primero, el del enunciado), `discos`
(todas las unidades), `ram` (física y swap), `cpu` (uso, núcleos, frecuencia,
carga) y `red` (KB/s de entrada y salida, errores, descartes).

**El operador decide qué mide cada nodo**, desde el dashboard, con `CMD
SET_RECURSOS`, sin entrar a esa máquina. Se persiste en
`nodos.recursos_pedidos`, así que el nodo lo readopta solo al reconectar.

#### Cómo se guarda: JSON con columnas materializadas

La tabla `recursos` tiene una columna `metricas JSON` y tres columnas
**generadas** que MySQL calcula sola a partir de ese JSON:

```sql
total_gb DECIMAL(12,2) GENERATED ALWAYS AS
  (JSON_VALUE(metricas, '$.total_gb' RETURNING DECIMAL(12,2) …)) STORED
```

**Por qué no una columna por métrica:** porque el objetivo era que agregar una
medida no obligue a un `ALTER TABLE` ni a coordinar a cinco personas.

**Por qué no una tabla clave-valor pura (EAV):** porque consultar "la RAM de
todos los nodos" requeriría un `JOIN` por cada métrica, y no se indexa bien.

**Por qué entonces las columnas generadas:** porque consultar JSON fila a fila
es lento y no se indexa. Las tres medidas que **siempre** se consultan se
materializan: se escriben solas, no se pueden desincronizar del JSON, y sí se
indexan. Se paga espacio y se gana un orden de magnitud en las consultas del
dashboard. Es el patrón *JSON con columnas materializadas* y es la respuesta si
en la defensa preguntan por el diseño.

#### El pendrive de Santa Cruz

La laptop reporta su disco interno como siempre **y además** el USB de 32 GB
como un recurso `DISCO` aparte, etiquetado `removible: si`.

**El pendrive NO entra en `total_gb`.** Esa columna es, por definición del
enunciado, el primer disco, y de ella sale el KPI global del cluster. Sumar un
pendrive de 32 GB como si fuera almacenamiento de un datacenter falsearía el
consolidado. Se reporta, se ve, se suma en `extra_disco_gb` y en
`capacidad_con_extras_gb` — etiquetado como lo que es.

Además, `primer_disco()` ahora **prefiere una unidad fija**: si alguien deja un
pendrive enchufado y el sistema lo lista primero, el nodo reportaría 32 GB como
capacidad de un datacenter.

**El servidor detecta los cambios solo.** Compara los discos de cada muestra
con los de la anterior y deja los eventos `DISCO_AGREGADO`, `DISCO_REMOVIDO` y
`CAPACIDAD_CAMBIADA`. No hace falta que nadie avise ni reiniciar nada. El
cliente hace su propia detección en paralelo y la anota en su bitácora local,
**aunque en ese momento no haya red**: los dos lados llegan a la misma
conclusión por su cuenta, que es lo que se quiere en un sistema distribuido.

El umbral es de 0,5 GB: redimensionar una partición o montar un snapshot mueve
el total unos MB, y no queremos un evento por eso.

---

### 2.9 Dashboard en tiempo real, sin F5

**El problema.** Cada navegador abierto pedía `/api/nodes`, `/api/cluster` y un
`/api/history` **por nodo**, cada pocos segundos. Con tres pantallas abiertas y
nueve nodos, más de treinta consultas a MySQL cada cinco segundos — y aun así
el operador veía el dato hasta cinco segundos tarde. Y si quería estar seguro,
apretaba F5.

**Lo que hay ahora.** La API mira la base **una vez por segundo, para todos**, y
empuja el estado a los navegadores conectados por `/ws/cluster`.

| | antes | ahora |
|---|---|---|
| consultas a MySQL | crecían con cada pantalla abierta | **fijas**, no dependen de cuánta gente mire |
| latencia | hasta 5 s | **< 1 s** |
| sin nadie mirando | seguía consultando | **no consulta nada** |

**Sólo se manda lo que cambió.** Difundir el estado completo cada segundo son
~8 KB por navegador por segundo, casi siempre idénticos. Se calcula una huella
de lo leído: si es la misma, se manda un **latido** de 40 bytes.

**El latido no es opcional.** Es lo que permite al navegador distinguir "no
cambió nada" de "se cortó la conexión" — que es exactamente el problema que
tenía el dashboard viejo cuando la API se caía y él seguía mostrando números
viejos como si nada.

**Fallback a REST.** Los endpoints REST no se tocaron. Si el WebSocket no se
puede abrir (un proxy sin *upgrade*, la API reiniciándose), el dashboard vuelve
solo al polling y reconecta por detrás con backoff hasta 15 s. **El indicador
de arriba a la derecha dice en qué modo está**: en vivo (verde, latiendo),
modo respaldo (ámbar), sin conexión (rojo). Una pantalla congelada que parece
actualizada es peor que una que avisa.

**El histórico no viaja por el WebSocket**: son cientos de puntos por nodo y no
cambian de forma perceptible entre un segundo y el siguiente. Se pide aparte
cada 15 s. Lo que llega empujado es el estado, que es lo que importa al segundo.

---

### 2.10 Departamento y sede: La Paz son dos oficinas, no dos departamentos

**El problema.** El código trataba `region` como si fuera a la vez el
departamento y el lugar. Con dos servidores en La Paz, el dashboard mostraba
"La Paz" dos veces sin decir cuál era cuál.

**Lo que hay ahora.** Tres conceptos separados:

| | Qué es | Ejemplo |
|---|---|---|
| `node_id` | El nombre único de **una computadora** | `CNS-ELA-10` |
| `region` | El **departamento**: una de las nueve regionales del enunciado. Es lo que se suma. | `La Paz` |
| `sede` | La **oficina** concreta donde está esa máquina | `El Alto` |

El departamento de La Paz atiende desde la ciudad de La Paz y desde El Alto,
cada una con su servidor de archivos: dos computadoras, dos `node_id`, dos
filas — **una sola regional** en el consolidado. En el dashboard la tarjeta
dice la sede en grande y el departamento debajo, y el gráfico *Capacidad por
regional* muestra una sola barra "La Paz" con la suma.

La sede viaja en el `HELLO`, así que una máquina nueva puede unirse con la sede
que quiera **sin tocar ningún archivo del servidor**.

---

### 2.11 El dashboard, reordenado por pregunta

La versión anterior tenía la información correcta en un orden que no ayudaba a
leerla. Se reordenó de arriba hacia abajo por **la pregunta que responde cada
bloque**:

| | Responde |
|---|---|
| Cabecera | ¿cuánto hay y cuánto queda? |
| **Alertas** | ¿hay algo que atender **ahora**? |
| **Utilización por servidor** | ¿cuál está más lleno? |
| **Distribución del clúster** | ¿está equilibrado? |
| **Capacidad por regional** | ¿cuánto tiene cada departamento? |
| **Histórico** | ¿está subiendo el uso? |
| Tarjetas | el detalle de cada servidor |

Cuatro decisiones concretas, y el porqué de cada una:

**El color significa estado, no identidad.** Verde / ámbar / rojo con los mismos
cortes (80 % y 90 %) en las barras, las tarjetas, la tabla y los KPI. Si algo
está rojo, está lleno: no hay que aprender una leyenda. El número siempre está
escrito al lado, así que **el color nunca es el único dato** — la pantalla
sigue sirviendo para alguien daltónico. La paleta se validó con un verificador
de separación para daltonismo sobre la superficie oscura real del dashboard,
no a ojo.

**El histórico es UNA línea, no diez.** Antes se dibujaba una línea por nodo en
el mismo gráfico. Con diez nodos eso es un plato de espaguetis: los colores
dejan de distinguirse y la pregunta "¿está subiendo el uso del clúster?" no se
lee. Ahora el gráfico grande muestra el total y **cada nodo tiene su
mini-línea dentro de su tarjeta**. La serie global se agrupa en el servidor:
24 h de 10 nodos cada 10 s son 86.400 filas, y el navegador tendría que dibujar
86.400 segmentos en 800 píxeles.

**Las mini-líneas se escalan a su propio rango.** Con escala fija de 0 a 100, un
nodo al 93 % dibuja una raya pegada al techo: se ve que está lleno —eso ya lo
dice el número enorme que está justo arriba— pero no se ve lo único que la
mini-línea puede aportar, que es la tendencia. Si el recorrido es menor a dos
puntos, se dibuja plana a propósito, para no amplificar ruido.

**Las alertas sólo aparecen si hay algo.** Un panel que siempre dice "todo bien"
deja de leerse a la semana.

**Y se le sacó la mitad del alto.** La versión anterior hacía una sección por
departamento con su título; como ocho de las nueve regionales tienen un solo
servidor, cada sección era una tarjeta sola en una fila de 1500 píxeles —
más de dos mil píxeles de pantalla en blanco para diez servidores. Ahora es una
rejilla densa ordenada por regional: **5.272 px → 3.055 px**, y el consolidado
por departamento está arriba en su propio gráfico.

---

### 2.12 Sumar una computadora: un comando

```bash
python scripts/unirse.py --host 192.168.1.100
```

Pregunta el departamento, la sede y el nombre del nodo (sugiere uno que no
choca, armado con la IP de esa máquina), y arranca. **En el servidor no se
toca nada**: se da de alta solo por el requisito 7.2.

`docs/COMO-AGREGAR-UN-NODO.md` tiene el detalle, incluida la diferencia entre
un cliente real y los diez que levanta `lanzar_nodos.py` en una sola máquina —
que es una pregunta que el tribunal hace siempre.

---

### 2.13 Dos correcciones

**La prueba de concurrencia fallaba con el código correcto.** `ids_prueba()`
sale de `config.REGIONALES`, que pasó de nueve a diez entradas al agregar el
segundo servidor de La Paz, pero el `check` tenía el 9 escrito a mano:

```
[FALLA] Los 9 hilos escribieron en paralelo  -> 10/9 nodos con metricas
```

Diez hilos escribieron en paralelo, que es más de lo que la prueba pedía. Ahora
el número sale de la lista. También se subió `DB_POOL_SIZE` de 10 a 16: con
diez nodos más el hilo principal saltaba el aviso "hay 11 conexiones abiertas".

**SQLite no arrancaba en carpetas de red.** El modo WAL necesita memoria
compartida entre procesos, y eso no existe en un recurso compartido, una unidad
de red o una carpeta sincronizada: el cliente moría con `disk I/O error` antes
de mandar la primera métrica. Habría pasado en la demo si alguien clonaba el
proyecto en una carpeta de red. Ahora se intenta WAL y, si el sistema de
archivos no lo soporta, se cae al diario clásico. Se pierde algo de
concurrencia; no se pierde ni un dato.


---

## 3. Archivos

### Nuevos

| Archivo | Qué es |
|---|---|
| `cliente/almacen.py` | Base local SQLite de cada nodo: muestras + bitácora + estado |
| `api/difusion.py` | Tarea de fondo que empuja el estado por WebSocket |
| `db/migracion_v2.sql` | v1 → v2 sin borrar datos. Idempotente |
| `scripts/prueba_offline.py` | 45 comprobaciones sin MySQL |
| `scripts/unirse.py` | Une esta computadora al clúster en un comando |
| `docs/ACTUALIZACIONES.md` | Este documento |
| `docs/COMO-AGREGAR-UN-NODO.md` | Departamento vs sede vs nodo, y cómo suma su laptop el ingeniero |
| `.gitattributes` | Finales de línea LF en todo el repositorio |

### Modificados

| Archivo | Qué cambió |
|---|---|
| `comun/protocolo.py` | Versión 2, recursos flexibles, `METRIC_BATCH`/`SYNC_OK`/`PONG`, `mono_ns`, `fechar_muestra`, saneado de recursos, `MAX_LINEA` 64 KB → 4 MB |
| `comun/config.py` | `MAX_NODOS` 12, La Paz con dos servidores, recursos, buffer, sync, reloj, ping, intermitencia, WebSocket |
| `cliente/main.py` | Guarda antes de enviar, sincroniza al reconectar, `SET_RECURSOS`, `PING`/`PONG`, vigila su reloj, jitter, `--caos` |
| `cliente/metricas.py` | Registro de colectores; RAM, CPU, red, todas las unidades; detección de USB y de cambios de capacidad |
| `servidor/main.py` | `METRIC_BATCH`, fechado por el servidor, desvío de reloj, hilo `latido`, detección de discos, desconexión con motivo |
| `servidor/mensajeria.py` | `--recursos` y `--sync` |
| `db/schema.sql` | Tabla `recursos`, 8 columnas nuevas en `nodos`, 3 en `metricas`, tipos de evento nuevos, `v_recursos_ultimo`, `v_regionales` |
| `db/repositorio.py` | 12 funciones nuevas; `guardar_lote` idempotente; desconexión e intermitencia |
| `api/main.py` | `/ws/cluster` y 5 endpoints nuevos |
| `api/modelos.py` | 15 campos nuevos en `NodoOut`, 4 modelos nuevos |
| `dashboard/index.html` | Reescrito: WebSocket con fallback, orden por pregunta, color = estado, ranking, histograma, apiladas por regional, histórico de una línea, mini-líneas por nodo, alertas, fecha de desconexión, recursos, bitácora, acciones por nodo, y responsive |
| `db/probar_bd.py` | Sección 9: recursos, sincronización idempotente, desconexión, reloj, dos servidores por regional |
| `db/probar_aiven.py` | Verifica las 5 tablas y 5 vistas |
| `scripts/lanzar_nodos.py` | `--recursos`, `--caos`, `--sede`, dos servidores en La Paz |
| `cliente/almacen.py` | WAL con respaldo al diario clásico en sistemas de archivos de red |
| `comun/config.py` | `SEDES` y `sede_de()`: departamento y oficina son cosas distintas |
| `README.md`, `docs/protocolo.md` | Actualizados |

Los archivos originales quedaron con extensión `.bak` al lado, por si hace
falta comparar. No van al repositorio (están en `.gitignore`).

---

## 4. Cómo actualizar

```bash
# 1. Base de datos — elegir UNA
mysql -u root -p cns_cluster < db/migracion_v2.sql   # conserva los datos
mysql -u root -p cns_cluster < db/schema.sql         # borra y reconstruye

# 2. Configuración
#    Copiar del .env.example los bloques nuevos: RECURSOS, BUFFER_*, SYNC_*,
#    UMBRAL_RELOJ_SEG, PERIODO_PING_SEG, INTERMITENCIA_*, PERIODO_WS_MS.
#    Todos tienen valor por defecto: si no se copian, el sistema arranca igual.

# 3. Verificar
python scripts/prueba_offline.py     # sin MySQL
python -m db.probar_aiven            # estructura
python -m db.probar_bd               # capa de datos
python scripts/prueba_integracion.py # end to end
```

No hace falta instalar nada nuevo: SQLite viene con Python y el WebSocket lo
trae `uvicorn[standard]`, que ya estaba en `requirements.txt`.

---

## 5. Guion para la defensa

Diez minutos, en este orden. Cada paso demuestra un requisito distinto.

**1. Arranque (1 min)** — servidor, API y nodos. El dashboard muestra las
regionales; La Paz aparece **una vez con dos servidores dentro** y su subtotal.

**2. Sin F5 (1 min)** — indicador en verde, latiendo. Se cambia algo en un nodo
y aparece en menos de un segundo. *Nadie toca el teclado.* Para mostrar el
fallback: se para la API, el indicador pasa a ámbar y avisa; se vuelve a
levantar y reconecta sola.

**3. Alta automática — requisito 7.2 (1 min)** — se arranca un cliente con un
`node_id` que no existe en ninguna configuración. Aparece solo en el dashboard,
con su evento `ALTA_AUTOMATICA` en la bitácora.

**4. El pendrive (1 min)** — se enchufa un USB en la laptop de Santa Cruz. En
menos de un intervalo aparece como unidad `EXTRAÍBLE` en el panel de recursos,
un evento `DISCO_AGREGADO` en la bitácora, y la capacidad extra del nodo sube.
Se explica **por qué no entra en el KPI global**.

**5. RAM y CPU en caliente (1 min)** — menú ⋮ de un nodo → se marcan `ram` y
`cpu` → Aplicar. Sin tocar esa máquina, el nodo empieza a reportarlas y
aparecen en el panel. Se explica que agregar una métrica nueva es escribir una
función, no migrar la base.

**6. Fallo intermitente y sincronización (3 min)** — *el momento fuerte.*

   - Se corta la red del nodo (o `--caos`). Pasa a `NO REPORTA` y la tarjeta
     dice **"Se desconectó de la red el ..."** con el motivo.
   - Mientras tanto: `python -m cliente.almacen --node-id <id>` muestra que el
     nodo **sigue midiendo y guardando** — N muestras pendientes.
   - Se restablece la red. La tarjeta muestra `SINCRONIZANDO N`, aparece el
     evento `SINCRONIZACION` en la bitácora, y **el hueco del gráfico se
     rellena solo**, con los puntos recuperados marcados en otro color.
   - Si se corta a mitad de la sincronización y se reconecta: `descartadas`
     distinto de cero en el log del servidor y **cero filas duplicadas** en la
     base. Ahí está la idempotencia.

**7. El reloj (1 min)** — se cambia la hora del sistema del nodo una hora hacia
atrás. Las métricas siguen apareciendo en el instante correcto. Aparece la
etiqueta `RELOJ -3600s` y un evento `RELOJ_DESVIADO`. Se explica el cálculo con
el reloj monotónico.

**8. Mensajería — requisito 7.1 (1 min)** — se manda "Verifique espacio en
disco" a un nodo; llega el ACK con su round-trip en ms, y el texto está en el
`.log` de esa máquina.

---

## 6. Preguntas de defensa que ahora tienen respuesta

**¿Por qué no confían en la hora del cliente?**
Porque no es confiable ni verificable. Se usa su reloj monotónico, que sólo
puede medir *cuánto hace* que se tomó la muestra, y la hora la pone el servidor.
Cambiar la fecha de un nodo no mueve ni una fila.

**¿Y si dos clientes mandan el mismo `seq`?**
No pueden: el `seq` es por nodo y el servidor lo guarda en
`nodos.ultima_seq`, por `node_id`. Además la identidad la fija el `HELLO` — un
`METRIC` con un `node_id` ajeno se descarta.

**¿Qué pasa si el mismo lote llega dos veces?**
Se descarta entero. El servidor sólo acepta muestras con `seq` mayor que
`ultima_seq`, y el avance de ese contador lleva la condición dentro del
`UPDATE`, así que dos hilos del mismo nodo no pueden hacerlo retroceder. Hay una
comprobación específica que reenvía el mismo lote y verifica cero inserciones.

**¿Y si el buffer local se llena?**
Se descarta lo más viejo y **queda anotado** en la bitácora del nodo. Con los
valores por defecto son ~55 horas de corte antes de perder nada.

**¿El WebSocket no carga más el servidor?**
Al revés: la carga sobre MySQL dejó de depender de cuántas pantallas hay
abiertas. Antes eran tres consultas por navegador cada cinco segundos; ahora son
tres por segundo en total, y **cero** si no hay nadie conectado.

**¿Qué pasa si se cae el WebSocket en plena defensa?**
El dashboard vuelve solo al polling REST y el indicador lo dice. Sigue
funcionando.

**¿Por qué JSON en la base y no columnas?**
Para que agregar una métrica no obligue a un `ALTER TABLE`. Y para no pagar la
lentitud del JSON, las tres medidas que siempre se consultan están
materializadas en columnas generadas indexables.

**¿El pendrive no falsea la capacidad del cluster?**
No, porque no entra en `total_gb`, que por definición del enunciado es el primer
disco. Se suma aparte, en `extra_disco_gb`, etiquetado como extraíble.

**Si un nodo se cae y vuelve todo el tiempo, ¿el dashboard lo muestra verde?**
No. Se marca `intermitente` a partir de 3 cortes en 10 minutos, con su propio
color y su evento. Un nodo que parpadea es un problema distinto a uno caído.

---

## 7. Qué NO se hizo, y por qué

Vale la pena decirlo antes de que lo pregunten:

- **No hay TLS entre cliente y servidor.** El enunciado dice LAN. En una WAN
  real esto iría dentro de un túnel; el protocolo no cambiaría.
- **No hay autenticación de nodos.** Cualquiera en la LAN puede conectarse al
  puerto 5050 y darse de alta. Es lo que pide el requisito 7.2 (alta automática)
  y en un sistema real se resolvería con un token por nodo. Sí está el saneado:
  un cliente hostil puede registrarse, pero no puede tumbar el servidor ni
  inyectar datos que rompan la base o el navegador.
- **La API tampoco tiene autenticación.** Mismo motivo.
- **Los indicadores `overcommit`, `fragmentación`, `quorum` y `replication
  health` siguen siendo N/A**, con su justificación en
  `servidor/consolidados.py`. Este cluster no tiene thin provisioning, ni
  volumen unificado, ni consenso, ni replicación entre nodos. Inventar un
  número para llenar un hueco del enunciado sería peor que explicar por qué no
  aplica.
- **El histórico no se purga.** Con 9 nodos cada 10 s son ~78.000 filas al día.
  Para la práctica no importa; en producción haría falta particionar por fecha
  o agregar a resolución más gruesa pasados unos días.
