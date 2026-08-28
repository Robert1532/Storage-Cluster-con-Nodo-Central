# Protocolo de comunicación — contrato del equipo  ·  **versión 2**

**Tarea 0.3. Este documento se cierra ANTES de que nadie escriba código.**
Cualquier cambio se avisa en el grupo antes de commitear. La implementación
está en `comun/protocolo.py` y es la única fuente de verdad: nadie arma un JSON
a mano en su módulo.

Transporte: **TCP**, puerto 5050 por defecto.
Formato: **un objeto JSON por línea, terminado en `\n`**, codificado en UTF-8.
Versión: **2** (viaja en el campo `v` del `HELLO` y del `HELLO_OK`).

## Las tres reglas nuevas de la versión 2

1. **La hora la pone el servidor.** El cliente manda su reloj *monotónico*
   (`mono_ns`), no su hora. El servidor calcula la hora real. Cambiar la fecha
   de un nodo no mueve ni una fila del histórico.
2. **Nada se pierde.** El cliente guarda cada medición en su base local antes
   de enviarla, y entrega lo atrasado en lotes (`METRIC_BATCH`) al reconectar.
   Cada muestra lleva un `seq` que nunca se repite, así una retransmisión no
   duplica datos.
3. **Se puede medir cualquier cosa.** Además del bloque `disco` (fijo, el del
   enunciado), un `METRIC` lleva una lista `recursos` con `{tipo, nombre,
   metricas}`. `metricas` es un diccionario abierto: una medida nueva no
   necesita cambios en el protocolo ni en la base.

## Por qué el salto de línea

TCP entrega un **flujo de bytes**, no mensajes. Si el cliente hace dos `send()`
seguidos, el servidor puede recibir:

- los dos mensajes pegados en un solo `recv()`, o
- medio mensaje ahora y la otra mitad después.

Sin un separador acordado, `json.loads()` falla o mezcla datos. Por eso todo
mensaje termina en `\n` y se lee con `protocolo.LectorLineas`, que acumula lo
que llega y solo entrega líneas completas.

> Es la pregunta 3 del banco de defensa y la que más gente falla.

`LectorLineas` está probado contra los ocho casos raros: mensajes pegados,
mensaje partido, líneas vacías, JSON corrupto, bytes que no son UTF-8, JSON
válido que no es un objeto (`null`, `123`, `[1,2]`), mensaje sin `\n` final al
cerrar, y carácter UTF-8 multibyte partido entre dos `recv()`. Una línea
corrupta se descarta y se sigue: nunca tumba la conexión.

## Por qué hay un candado por socket

`sendall()` puede escribir de a partes y reintentar. Si **dos hilos** escriben
en el **mismo** socket a la vez, sus bytes se intercalan y el receptor ve
líneas que no parsean. Pasa en los dos extremos:

- **servidor**: el despachador manda un `CMD` mientras el hilo del cliente manda `METRIC_OK`
- **cliente**: el hilo receptor manda un `ACK` mientras el principal manda `METRIC`

Medido con sockets reales y un receptor lento: **sin candado se perdieron 148
de 600 mensajes; con candado llegaron los 600**. Por eso `protocolo.enviar()`
recibe un `threading.Lock` y es **uno por conexión**, no uno global: dos nodos
distintos pueden escribir en paralelo sin estorbarse.

---

## Mensajes

### HELLO — cliente → servidor (al conectar)

```json
{"tipo":"HELLO","v":2,"node_id":"CNS-LPZ-01","region":"La Paz",
 "hostname":"pc-luis","so":"Windows 11","intervalo":10,
 "agente":"2.0.0",
 "capacidades":["disco","discos","ram","cpu","red"],
 "pendientes":842,
 "timestamp":"2026-08-25T14:03:11.482-04:00",
 "mono_ns":91827364550000}
```

`capacidades` es lo que ese nodo **sabe** medir; el servidor no tiene una lista
fija. `pendientes` es cuántas muestras trae guardadas de su última caída: es lo
que permite decir en el log y en el dashboard **cuánto se perdió**, en el
momento mismo de la reconexión y no después.

### HELLO_OK — servidor → cliente

```json
{"tipo":"HELLO_OK","v":2,"registrado":true,"nuevo":true,"intervalo":10,
 "recursos":["disco","ram","cpu"],"sync":true,
 "hora_servidor":"2026-08-25T14:03:11.501-04:00","desvio_seg":-3.2,
 "timestamp":"2026-08-25T14:03:11.501-04:00"}
```

`recursos` es lo que el servidor **quiere** que mande ese nodo, que puede ser
menos que lo que sabe medir. Se guarda en la base, así que el operador lo
cambia desde el dashboard y el nodo lo readopta solo al reconectar.
`desvio_seg` le dice al nodo cuánto miente su reloj respecto al del servidor;
el nodo lo anota en su log, pero **no corrige nada**: las métricas ya se fechan
con la hora del servidor.

`nuevo: true` significa que el nodo no existía y se dio de alta solo: es la
prueba en vivo del **requisito 7.2**. El `intervalo` que devuelve es el de la
base de datos, no el que mandó el cliente — si un operador lo cambió desde el
dashboard, el cliente lo adopta al reconectar (**requisito 7.3**). Para un nodo
nuevo coinciden, porque el alta guarda el que envió el cliente.

### METRIC — cliente → servidor (periódico)

```json
{"tipo":"METRIC","node_id":"CNS-LPZ-01","seq":1841,
 "timestamp":"2026-08-25T14:03:21.107-04:00",
 "mono_ns":91827374550000,"mono_envio_ns":91827374560000,
 "disco":{"nombre":"C:\\","tipo":"SSD",
          "total_gb":476.94,"usado_gb":312.41,"libre_gb":164.53,"uso_pct":65.50,
          "iops_lectura":142,"iops_escritura":88,"latencia_ms":0.712},
 "recursos":[
   {"tipo":"RAM","nombre":"fisica",
    "metricas":{"total_gb":16.0,"usado_gb":9.5,"libre_gb":6.5,"uso_pct":59.4},
    "etiquetas":{"unidad":"GB"}},
   {"tipo":"CPU","nombre":"total",
    "metricas":{"uso_pct":23.5,"nucleos":8,"frecuencia_mhz":2900.0}},
   {"tipo":"DISCO","nombre":"E:\\",
    "metricas":{"total_gb":28.8,"usado_gb":4.0,"libre_gb":24.8,"uso_pct":13.9},
    "etiquetas":{"tipo":"USB","removible":"si","principal":"no"}}]}
```

Las claves de `disco` son fijas y siguen siendo **el primer disco**, que es lo
que pide el enunciado y lo que alimenta `v_cluster` y el KPI global. `tipo`
admite `SSD`, `HDD`, `USB` o `DESCONOCIDO` (coincide con el ENUM de MySQL).

`recursos` es la parte **abierta**: cada elemento es `{tipo, nombre, metricas,
etiquetas}`. `tipo` es `DISCO`, `RAM`, `CPU`, `RED` o `CUSTOM`. `metricas` solo
admite números, `etiquetas` solo texto — la separación no es cosmética: las
métricas se agregan y se grafican, las etiquetas describen. En la base, `metricas`
entra como JSON y las tres medidas que se consultan siempre (`total_gb`,
`usado_gb`, `uso_pct`) se materializan en columnas generadas indexables.

**Para agregar una medida nueva no hay que tocar este documento.** Se escribe
un colector en `cliente/metricas.py` y se manda una clave más.

El servidor **sanea** todo antes de insertarlo: un tipo inesperado pasa a
`CUSTOM` o `DESCONOCIDO`, un número fuera de rango se acota, un texto largo se
recorta, un `NaN` o un booleano se descartan, y una lista de 10.000 recursos se
corta en 32. Un cliente con un bug —o alguien conectado al puerto 5050 a
mano— no puede tumbar el servidor.

`seq` es el número de muestra del cliente. Crece siempre y **sobrevive a un
reinicio** porque vive en su base local SQLite. Es lo que hace idempotente la
sincronización.

`mono_ns` es el reloj monotónico del cliente en el instante de la medición, y
`mono_envio_ns` en el instante del envío. Ver *Hora* más abajo.

### METRIC_OK — servidor → cliente

```json
{"tipo":"METRIC_OK","recibido":true,"seq":1841,
 "timestamp":"2026-08-25T14:03:21.130-04:00"}
```

Devuelve el `seq` para que el cliente marque **esa** muestra como entregada en
su base local. Hasta que llega, la muestra sigue contando como pendiente y se
reenviaría después de una caída.

### METRIC_BATCH — cliente → servidor (sincronización tras una caída)

```json
{"tipo":"METRIC_BATCH","v":2,"node_id":"CNS-LPZ-01",
 "mono_envio_ns":91830000000000,
 "timestamp":"2026-08-25T15:10:00.000-04:00",
 "muestras":[
   {"seq":1842,"mono_ns":91827384550000,
    "timestamp":"2026-08-25T14:03:31.107-04:00",
    "disco":{"…"},"recursos":[]},
   {"seq":1843,"mono_ns":91827394550000,"…":"…"}]}
```

Como máximo **100 muestras por lote** (`MAX_MUESTRAS_LOTE`). El cliente espera
el `SYNC_OK` antes de mandar el siguiente: es control de flujo. Un nodo que
estuvo un día sin red tiene ~8.600 muestras; mandarlas de golpe le metería al
servidor 8.600 `INSERT` en un solo mensaje mientras los otros ocho nodos siguen
reportando.

### SYNC_OK — servidor → cliente

```json
{"tipo":"SYNC_OK","hasta_seq":1941,"recibidas":100,"descartadas":0,
 "timestamp":"2026-08-25T15:10:00.240-04:00"}
```

`hasta_seq` es un **ACK acumulativo**: el cliente borra de su cola todo lo que
sea `<=` a ese número. Que sea acumulativo es lo que hace que un `SYNC_OK`
perdido no pierda datos — el lote entero se reenvía la próxima vez y el
servidor lo descarta por duplicado, sin insertar nada.

`descartadas` son las muestras del lote que el servidor ya tenía. Ver ese
número distinto de cero es normal después de un corte a mitad de una
sincronización, y es la prueba de que la idempotencia está funcionando.

### PONG — cliente → servidor

```json
{"tipo":"PONG","node_id":"CNS-LPZ-01",
 "timestamp":"2026-08-25T14:03:26.000-04:00","mono_ns":91827379550000}
```

Respuesta al `CMD` con acción `PING`. Ver *Fallos intermitentes*.

### CMD — servidor → cliente

```json
{"tipo":"CMD","cmd_id":"7f3a…","accion":"MENSAJE",
 "texto":"Verifique espacio en disco","valor":null,
 "timestamp":"2026-08-25T14:05:00.000-04:00"}

{"tipo":"CMD","cmd_id":"9b1c…","accion":"SET_INTERVAL",
 "texto":null,"valor":5,"timestamp":"2026-08-25T14:06:00.000-04:00"}

{"tipo":"CMD","cmd_id":"a4e2…","accion":"SET_RECURSOS",
 "texto":"disco,ram,cpu","valor":null,"timestamp":"…"}

{"tipo":"CMD","cmd_id":"","accion":"PING","texto":null,"valor":null,"timestamp":"…"}
```

| acción | qué hace | responde |
|---|---|---|
| `MENSAJE` | texto del operador; el cliente lo escribe en su `.log` | ACK |
| `SET_INTERVAL` | cambia cada cuánto reporta (requisito 7.3) | ACK |
| `SET_RECURSOS` | cambia **qué** mide, sin entrar a esa máquina | ACK |
| `SOLICITAR_SYNC` | "mandá ya lo que tengas guardado" | ACK |
| `PING` | latido: comprueba que el camino de ida funciona | PONG |

Una acción desconocida se registra en el `.log` del cliente y se responde el
ACK igual, pero no se ejecuta nada.

El `PING` va con **`cmd_id` vacío** a propósito: no es un mensaje del operador,
no se guarda en la tabla `mensajes` y el cliente no responde ACK, sólo `PONG`.

### ACK — cliente → servidor

```json
{"tipo":"ACK","cmd_id":"7f3a…","node_id":"CNS-LPZ-01",
 "recibido_en":"2026-08-25T14:05:00.214-04:00"}
```

**El `cmd_id` no es opcional.** Sin él, si el servidor manda dos mensajes
seguidos y vuelven dos ACK, no hay forma de saber cuál confirma cuál. Es la
pregunta 8 de la defensa.

El cliente manda el ACK **antes** de ejecutar el comando. Si lo mandara al
final, cualquier fallo al aplicarlo (disco lleno al escribir el `.log`, un
valor inválido) dejaría el mensaje sin confirmar para siempre.

### ERROR — servidor → cliente

```json
{"tipo":"ERROR","motivo":"Cluster lleno (9 nodos)",
 "timestamp":"2026-08-25T14:03:11.501-04:00"}
```

El servidor lo manda antes de cerrar cuando rechaza una conexión: cluster lleno
o `HELLO` sin `node_id`. El cliente lo registra y **termina** en vez de
reintentar para siempre, que es lo que haría si solo viera un socket cerrado.

---

## Hora: la pone el servidor, no el cliente

**Ninguna métrica se guarda con la hora que dice el cliente.** Un nodo puede
tener el reloj mal, puede que alguien se lo cambie, o puede sincronizar con NTP
y saltar tres horas a mitad de una medición.

Cada muestra viaja con `mono_ns`, el reloj **monotónico** del proceso cliente.
Ese reloj no se ve afectado por ajustes de la hora del sistema: sólo avanza. El
mensaje lleva además `mono_envio_ns`, el valor del mismo reloj en el instante
del envío. Con esos dos números el servidor calcula:

```
edad_de_la_muestra = (mono_envio_ns − mono_ns) / 1e9
timestamp_guardado = hora_de_ESTE_servidor − edad_de_la_muestra
```

- Para una métrica **en vivo**, la edad es de milisegundos: el timestamp es
  "ahora".
- Para un **lote** que llegó dos horas tarde, cada muestra queda fechada en el
  momento en que se tomó de verdad, repartida a lo largo de esas dos horas. El
  gráfico dibuja la curva real, no una línea vertical en el instante en que se
  restableció la red.

Sólo se comparan dos valores del **mismo** proceso: el origen de
`time.monotonic_ns()` es arbitrario, pero la **diferencia** es tiempo real.
Está implementado en `protocolo.fechar_muestra()` y probado sin base de datos
en `scripts/prueba_offline.py`.

Guardas: una edad negativa (reloj manipulado o cliente con un bug) o mayor a un
mes se descarta y la muestra se fecha como "ahora" — la suposición menos dañina.

El `timestamp` del cliente **sigue viajando**, pero es sólo informativo: se
guarda en `metricas.t_cliente` para auditoría y se usa para calcular el desvío,
que queda en `nodos.desvio_reloj_seg` y deja un evento `RELOJ_DESVIADO`. El
operador se entera de que esa máquina tiene la hora mal; los datos no se ven
afectados.

### Zona horaria del `timestamp` informativo

Va en ISO 8601 **con el offset de zona**:
`2026-08-25T14:03:11.482-04:00`.

El offset no es un adorno. La base guarda **siempre en UTC** (la conexión fija
`time_zone='+00:00'`, así que `NOW()` devuelve UTC). Si el mensaje no llevara
offset, el servidor tendría que suponer una zona, y como puede correr en una
máquina distinta a la del cliente —un contenedor en UTC, por ejemplo— esa
suposición desplazaría todas las métricas varias horas. Con el desfase de
Bolivia (UTC-4), cualquier consulta del tipo
`WHERE timestamp >= NOW() - INTERVAL 24 HOUR` devolvería **vacío** con la tabla
llena. Nos pasó de verdad; está en `docs/CAMBIOS.md`.

La API devuelve UTC y el dashboard lo convierte a hora local para mostrarlo.

---

## Fallos intermitentes

Un cable a medio enchufar, un wifi que va y viene o un NAT que expira no
producen un `FIN` de TCP. Tres capas distintas los detectan:

| capa | qué detecta | en cuánto |
|---|---|---|
| timeout de `recv` en el servidor | que **no llega** nada | `FACTOR_TIMEOUT × intervalo` |
| **`PING` de aplicación** (v2) | que **no se puede escribir**: el camino de ida está cortado | `PERIODO_PING_SEG` (15 s) |
| watchdog sobre la base | que el nodo dejó de reportar, haya socket o no | `PERIODO_WATCHDOG_SEG` |

El `PING` hace falta porque las otras dos no cubren el caso asimétrico: el nodo
manda métricas y el servidor las recibe, pero sus `CMD` no llegan a ninguna
parte. Sin el latido, eso se descubre recién cuando alguien manda un mensaje
desde el dashboard y nunca vuelve el ACK. **Escribir en el socket es lo único
que prueba que el camino de ida funciona.**

Un nodo que se cae y vuelve `INTERMITENCIA_CAIDAS` veces dentro de
`INTERMITENCIA_VENTANA_MIN` se marca `intermitente` en la base. En un dashboard
donde sólo hay dos colores, el nodo que parpadea se ve verde la mitad del
tiempo y no lo mira nadie.

---

## Estados del nodo

Solo dos: `ACTIVO` y `NO_REPORTA`. No inventar estados intermedios.
(`intermitente` es una **marca aparte**, no un tercer estado: un nodo
intermitente está `ACTIVO` o `NO_REPORTA` según el momento.)

Un nodo pasa a `NO_REPORTA` cuando:

```
ahora − ultimo_reporte > FACTOR_TIMEOUT × intervalo_seg_de_ese_nodo
```

El umbral es **por nodo**, no global: un nodo que reporta cada 30 s no puede
compartir timeout con uno que reporta cada 5 s. Vuelve a `ACTIVO` en cuanto
llega un reporte nuevo.

**El watchdog es el único que escribe `estado`**, en los dos sentidos. Ni el
alta ni el guardado de métricas lo tocan. Así cada transición deja su evento
(`NO_REPORTA` / `RECUPERADO`) en la bitácora y no hay dos escritores peleándose
por la misma columna.

---

## Los nodos

**La región no es la identidad del nodo: el `node_id` lo es.** La Paz tiene dos
servidores y las dos filas dicen `region = "La Paz"`. Por eso la clave única
está sobre `node_id` y no sobre `region`, y por eso existe la vista
`v_regionales`, que suma los nodos de cada regional.

| node_id | Regional | | node_id | Regional |
|---|---|---|---|---|
| CNS-LPZ-01 | La Paz | | CNS-CHU-06 | Chuquisaca |
| CNS-LPZ-10 | La Paz *(2.º servidor)* | | CNS-TJA-07 | Tarija |
| CNS-CBB-02 | Cochabamba | | CNS-BEN-08 | Beni |
| CNS-SCZ-03 | Santa Cruz | | CNS-PAN-09 | Pando |
| CNS-ORU-04 | Oruro | | | |
| CNS-PTS-05 | Potosí | | | |

Definidos en `comun/config.py::REGIONALES`. Esa lista **sólo la usa
`scripts/lanzar_nodos.py`** para la demo: un cliente con un `node_id` que no
esté ahí se da de alta solo (requisito 7.2). Agregar una computadora nueva es
arrancar el cliente, nada más.

---

## Ciclo de vida de una conexión

1. El cliente abre el socket y manda `HELLO`.
2. El servidor responde `HELLO_OK` (o `ERROR` y cierra).
3. El cliente manda `METRIC` cada `intervalo` segundos; el servidor responde
   `METRIC_OK`.
4. En cualquier momento el servidor puede mandar un `CMD`; el cliente responde
   `ACK`.
5. Si se corta la conexión, el cliente **sigue midiendo y guardando** en su
   base local, y reintenta con espera creciente **más jitter** (1, 2, 4, 8, 16,
   30 s, cada uno multiplicado por un factor aleatorio entre 0,7 y 1,3). El
   jitter no es un adorno: sin él, nueve nodos que se cayeron juntos porque se
   reinició el servidor reintentan todos en el mismo instante, una y otra vez.
6. Al reconectar manda `HELLO` con `pendientes`, y a continuación los
   `METRIC_BATCH` con todo lo que el servidor se perdió. El backlog se drena
   **en paralelo** con las métricas en vivo: un lote atrasado, después la de
   ahora. Así el dashboard vuelve a tiempo real de inmediato y el hueco se
   rellena por detrás.
7. El servidor cierra una conexión de la que no recibe nada durante
   `FACTOR_TIMEOUT × intervalo`, y registra la fecha y el motivo en
   `nodos.ultima_desconexion` / `motivo_desconexion`. Sin ese timeout, un cable
   desenchufado dejaría el hilo bloqueado durante horas ocupando una plaza.
