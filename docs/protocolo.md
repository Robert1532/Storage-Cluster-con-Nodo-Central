# Protocolo de comunicación — contrato del equipo

**Tarea 0.3. Este documento se cierra ANTES de que nadie escriba código.**
Cualquier cambio se avisa en el grupo antes de commitear.

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

---

## Mensajes

### HELLO — cliente → servidor (al conectar)

```json
{"tipo":"HELLO","node_id":"CNS-LPZ-01","region":"La Paz",
 "hostname":"pc-luis","so":"Windows 11","intervalo":10,
 "timestamp":"2026-08-25T14:03:11.482"}
```

### HELLO_OK — servidor → cliente

```json
{"tipo":"HELLO_OK","registrado":true,"nuevo":true,"intervalo":10,
 "timestamp":"2026-08-25T14:03:11.501"}
```

`nuevo: true` significa que el nodo no existía y se dio de alta solo: es la
prueba en vivo del **requisito 7.2**. El `intervalo` que devuelve es el de la
base de datos, no el que mandó el cliente — si un operador lo cambió desde el
dashboard, el cliente lo adopta al reconectar (**requisito 7.3**).

### METRIC — cliente → servidor (periódico)

```json
{"tipo":"METRIC","node_id":"CNS-LPZ-01",
 "timestamp":"2026-08-25T14:03:21.107",
 "disco":{"nombre":"C:\\","tipo":"SSD",
          "total_gb":476.94,"usado_gb":312.41,"libre_gb":164.53,"uso_pct":65.50,
          "iops_lectura":142,"iops_escritura":88,"latencia_ms":0.712}}
```

Las claves de `disco` son fijas. `tipo` solo admite `SSD`, `HDD` o
`DESCONOCIDO` (coincide con el ENUM de MySQL).

### METRIC_OK — servidor → cliente

```json
{"tipo":"METRIC_OK","recibido":true,"timestamp":"2026-08-25T14:03:21.130"}
```

### CMD — servidor → cliente

```json
{"tipo":"CMD","cmd_id":"7f3a…","accion":"MENSAJE",
 "texto":"Verifique espacio en disco","valor":null,
 "timestamp":"2026-08-25T14:05:00.000"}

{"tipo":"CMD","cmd_id":"9b1c…","accion":"SET_INTERVAL",
 "texto":null,"valor":5,"timestamp":"2026-08-25T14:06:00.000"}
```

`accion` solo admite `MENSAJE` o `SET_INTERVAL`.

### ACK — cliente → servidor

```json
{"tipo":"ACK","cmd_id":"7f3a…","node_id":"CNS-LPZ-01",
 "recibido_en":"2026-08-25T14:05:00.214"}
```

**El `cmd_id` no es opcional.** Sin él, si el servidor manda dos mensajes
seguidos y vuelven dos ACK, no hay forma de saber cuál confirma cuál. Es la
pregunta 8 de la defensa.

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
