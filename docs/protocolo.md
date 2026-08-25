# Protocolo de comunicación — contrato del equipo

**Tarea 0.3. Este documento se cierra ANTES de que nadie escriba código.**
Cualquier cambio se avisa en el grupo antes de commitear. La implementación
está en `comun/protocolo.py` y es la única fuente de verdad: nadie arma un JSON
a mano en su módulo.

Transporte: **TCP**, puerto 5050 por defecto.
Formato: **un objeto JSON por línea, terminado en `\n`**, codificado en UTF-8.

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
{"tipo":"HELLO","node_id":"CNS-LPZ-01","region":"La Paz",
 "hostname":"pc-luis","so":"Windows 11","intervalo":10,
 "timestamp":"2026-08-25T14:03:11.482-04:00"}
```

### HELLO_OK — servidor → cliente

```json
{"tipo":"HELLO_OK","registrado":true,"nuevo":true,"intervalo":10,
 "timestamp":"2026-08-25T14:03:11.501-04:00"}
```

`nuevo: true` significa que el nodo no existía y se dio de alta solo: es la
prueba en vivo del **requisito 7.2**. El `intervalo` que devuelve es el de la
base de datos, no el que mandó el cliente — si un operador lo cambió desde el
dashboard, el cliente lo adopta al reconectar (**requisito 7.3**). Para un nodo
nuevo coinciden, porque el alta guarda el que envió el cliente.

### METRIC — cliente → servidor (periódico)

```json
{"tipo":"METRIC","node_id":"CNS-LPZ-01",
 "timestamp":"2026-08-25T14:03:21.107-04:00",
 "disco":{"nombre":"C:\\","tipo":"SSD",
          "total_gb":476.94,"usado_gb":312.41,"libre_gb":164.53,"uso_pct":65.50,
          "iops_lectura":142,"iops_escritura":88,"latencia_ms":0.712}}
```

Las claves de `disco` son fijas. `tipo` solo admite `SSD`, `HDD` o
`DESCONOCIDO` (coincide con el ENUM de MySQL). El servidor **sanea** todo antes
de insertarlo: un tipo inesperado pasa a `DESCONOCIDO`, un número fuera de
rango se acota y un texto largo se recorta. Un cliente con un bug no puede
tumbar el servidor.

### METRIC_OK — servidor → cliente

```json
{"tipo":"METRIC_OK","recibido":true,"timestamp":"2026-08-25T14:03:21.130-04:00"}
```

### CMD — servidor → cliente

```json
{"tipo":"CMD","cmd_id":"7f3a…","accion":"MENSAJE",
 "texto":"Verifique espacio en disco","valor":null,
 "timestamp":"2026-08-25T14:05:00.000-04:00"}

{"tipo":"CMD","cmd_id":"9b1c…","accion":"SET_INTERVAL",
 "texto":null,"valor":5,"timestamp":"2026-08-25T14:06:00.000-04:00"}
```

`accion` solo admite `MENSAJE` o `SET_INTERVAL`. Una acción desconocida se
registra en el `.log` del cliente y se responde el ACK igual, pero no se
ejecuta nada.

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

## Hora y zona horaria

`timestamp` va en ISO 8601 **con el offset de zona**:
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

## Estados del nodo

Solo dos: `ACTIVO` y `NO_REPORTA`. No inventar estados intermedios.

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

## Los 9 nodos

| node_id | Regional | | node_id | Regional |
|---|---|---|---|---|
| CNS-LPZ-01 | La Paz | | CNS-CHU-06 | Chuquisaca |
| CNS-CBB-02 | Cochabamba | | CNS-TJA-07 | Tarija |
| CNS-SCZ-03 | Santa Cruz | | CNS-BEN-08 | Beni |
| CNS-ORU-04 | Oruro | | CNS-PAN-09 | Pando |
| CNS-PTS-05 | Potosí | | | |

Definidos en `comun/config.py::REGIONALES`. El código, la base y la
presentación tienen que decir exactamente lo mismo.

---

## Ciclo de vida de una conexión

1. El cliente abre el socket y manda `HELLO`.
2. El servidor responde `HELLO_OK` (o `ERROR` y cierra).
3. El cliente manda `METRIC` cada `intervalo` segundos; el servidor responde
   `METRIC_OK`.
4. En cualquier momento el servidor puede mandar un `CMD`; el cliente responde
   `ACK`.
5. Si se corta la conexión, el cliente reintenta con espera creciente
   (1, 2, 4, 8, 16, 30 s) y vuelve a mandar `HELLO`.
6. El servidor cierra una conexión de la que no recibe nada durante
   `FACTOR_TIMEOUT × intervalo`. Sin ese timeout, un cable desenchufado dejaría
   el hilo bloqueado durante horas ocupando una de las 9 plazas.
