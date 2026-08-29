# Arquitectura y tecnologías

Qué usamos, cómo está armado y **por qué se eligió cada cosa** frente a las
alternativas que había. Este documento es el que hay que leer antes de la
defensa: casi todas las preguntas técnicas salen de acá.

---

## 1. Qué es el sistema

Un sistema de monitoreo centralizado para los servidores de archivos de las
nueve administraciones regionales de la Caja Nacional de Salud. Cada regional
tiene una o más computadoras que guardan historiales clínicos; cada una corre
un **agente** que mide su disco y le reporta a un **nodo central** por sockets
TCP. El nodo central guarda todo en MySQL y lo publica en un dashboard web en
tiempo real, desde donde un operador puede además mandarle mensajes a
cualquier nodo.

En números: **10 servidores** en 9 departamentos, reportando cada 10 segundos,
lo que da unas **86.000 mediciones por día**.

---

## 2. Arquitectura

Son **cuatro programas distintos**, y esa separación es la decisión de diseño
más importante del proyecto.

```
   CADA SERVIDOR REGIONAL              EL NODO CENTRAL DE MONITOREO
   ─────────────────────────           ──────────────────────────────────────

   ┌─────────────────────┐
   │  cliente/main.py    │  TCP :5050  ┌────────────────────────┐
   │  ┌───────────────┐  │────────────►│  servidor/main.py      │
   │  │ SQLite local  │  │  METRIC     │  (sockets + hilos)     │
   │  │ guarda SIEMPRE│  │  METRIC_BATCH│                       │
   │  └───────────────┘  │◄────────────│  1 hilo por cliente    │
   │  2 hilos            │  CMD / PING │  + watchdog            │
   └─────────────────────┘  ACK / PONG │  + despachador         │
                                        │  + latido             │
                                        └───────────┬────────────┘
                                                    │
                                                    ▼
                                        ┌────────────────────────┐
                                        │      MySQL 8.0         │
                                        │  5 tablas + 5 vistas   │
                                        └───────────┬────────────┘
                                                    │
                                        ┌───────────▼────────────┐
                                        │  api/main.py (FastAPI) │
                                        │  REST + WebSocket      │
                                        └───────────┬────────────┘
                                                    │  empuja
                                        ┌───────────▼────────────┐
                                        │  dashboard/index.html  │
                                        └────────────────────────┘
```

### Por qué cuatro procesos y no uno

Podríamos haber metido los sockets y la API en un solo programa. No lo hicimos
por tres motivos concretos:

1. **Se reinician por separado.** Si Alex rompe el dashboard, los nodos siguen
   reportando. Si hay que reiniciar la API, no se pierde ni una métrica.
2. **Cada uno tiene su modelo de concurrencia.** El servidor de sockets es
   bloqueante con un hilo por conexión; la API es asíncrona. Mezclarlos
   obligaba a reescribir uno de los dos.
3. **Se pueden repartir entre personas.** Cinco personas trabajando sobre un
   solo proceso se pisan; sobre cuatro, no.

**El costo:** la API y el servidor de sockets **no comparten memoria**. Cuando
el dashboard manda un mensaje a un nodo, la API no puede escribir en su socket.
La solución es que **la base de datos es el bus**: la API inserta una fila
`PENDIENTE` en la tabla `mensajes`, y el despachador del servidor la recoge en
menos de un segundo y la manda por el socket.

Cuesta hasta 1 segundo de latencia. A cambio, **todo mensaje queda auditado en
la base aunque el nodo esté caído**. Para un sistema de monitoreo eso es un
buen negocio. (La alternativa —un socket de control interno en localhost—
también hay que tenerla en la punta de la lengua para la defensa.)

---

## 3. Las tecnologías, una por una

### Python 3.12

**Por qué:** el enunciado pide sockets a bajo nivel, y la biblioteca estándar
de Python los da sin dependencias. Es lo que el equipo ya sabe, y el proyecto
se defiende leyendo el código, no explicando un framework.

**3.12 y no 3.8:** por los tipos `str | None` sin `typing.Optional`, y porque
`time.monotonic_ns()` es estable ahí.

### `socket` + `threading` (no `asyncio`)

**Qué hace:** el servidor abre un socket TCP, hace `accept()` en bucle, y por
cada cliente aceptado lanza **un hilo** que sólo atiende a ese cliente.

**Por qué hilos y no asyncio:** son **10 conexiones**, no diez mil. Con esa
cantidad, un hilo por conexión es la solución más simple, la más fácil de
depurar (un traceback dice exactamente qué nodo falló) y la que el tribunal
puede leer sin conocer corrutinas. `asyncio` gana a partir de miles de
conexiones simultáneas, que no es este caso.

**Lo que sí obliga:** que dos hilos no escriban en el mismo socket a la vez.
`sendall()` puede escribir de a partes, así que sin un candado los bytes de dos
mensajes se intercalan y el receptor ve líneas que no parsean. **Medido con
sockets reales: sin candado se perdieron 148 de 600 mensajes; con candado
llegaron los 600.** Por eso hay un `threading.Lock` **por conexión** (no uno
global: dos nodos distintos pueden escribir en paralelo sin estorbarse).

### El protocolo: JSON por línea sobre TCP

**Por qué JSON:** legible en un `tcpdump`, sin generador de código, y todos los
lenguajes lo hablan.

**Por qué "una línea":** TCP entrega un **flujo de bytes**, no mensajes. Si el
cliente hace dos `send()` seguidos, el servidor puede recibir los dos pegados o
medio mensaje ahora y la otra mitad después. Por eso cada mensaje es un JSON en
una línea terminada en `\n`, y se lee con un acumulador que sólo entrega líneas
completas. **Es la pregunta más frecuente de la defensa.**

**Por qué no Protobuf o gRPC:** agregan un compilador y un `.proto` al
proyecto, y el ahorro de bytes no importa en una LAN con 10 nodos.

### MySQL 8.0

**Por qué una base relacional:** los datos son tabulares y las consultas son
agregaciones (`SUM`, `GROUP BY`, ventanas de tiempo). Es exactamente para lo
que sirve SQL.

**Por qué MySQL y no PostgreSQL:** es lo que pide la materia y lo que el equipo
tenía instalado. PostgreSQL habría servido igual.

**Por qué 8.0 y no 5.7:** hacen falta tres cosas que 5.7 no tiene: **funciones
de ventana** (`FIRST_VALUE`, para el growth rate en una sola consulta),
**columnas generadas desde JSON** (`JSON_VALUE`), y `CHECK` constraints reales.

**Por qué `DECIMAL` y no `FLOAT`:** sumar nueve `FLOAT` da cosas como
`4291.999999998`, y el porcentaje de utilización global sale con basura
decimal. `DECIMAL` suma exacto.

### JSON con columnas materializadas (la tabla `recursos`)

El requisito era que **agregar una métrica nueva no obligue a cambiar el
esquema**. Se evaluaron tres opciones:

| | Ventaja | Por qué se descartó |
|---|---|---|
| Una columna por métrica | rápido, indexado | cada métrica nueva = `ALTER TABLE` + coordinar a 5 personas |
| Tabla clave-valor (EAV) | flexible | "la RAM de todos los nodos" necesita un `JOIN` por métrica y no se indexa |
| **JSON + columnas generadas** ✅ | flexible **e** indexado | ocupa más espacio |

La columna `metricas` es `JSON` y guarda lo que mande el cliente. Las tres
medidas que **siempre** se consultan (`total_gb`, `usado_gb`, `uso_pct`) se
extraen a columnas **`GENERATED ALWAYS AS ... STORED`**: MySQL las calcula
sola, no se pueden desincronizar del JSON, y **sí se indexan**.

Resultado: para que el sistema mida la temperatura del disco alcanza con
escribir una función de diez líneas en el cliente. No se toca ni el protocolo,
ni el servidor, ni la base, ni el dashboard.

### SQLite en cada cliente

**El problema:** si el nodo perdía la red, sus mediciones se perdían. El hueco
en el dashboard era permanente. En un sistema que monitorea historiales
clínicos eso es al revés de lo que hace falta: justo cuando algo va mal es
cuando más importa saber qué pasaba en el disco.

**La solución:** cada nodo guarda **toda** medición en un SQLite propio
**antes** de intentar enviarla. Al reconectar entrega lo atrasado en lotes. Es
el patrón *store and forward*, el mismo de los agentes de monitoreo reales.

**Por qué SQLite y no un archivo de texto:** un archivo no sobrevive a un corte
de luz a mitad de escritura, no se consulta por rango, y borrar las primeras N
líneas obliga a reescribirlo entero. SQLite da transacciones, un índice sobre
lo pendiente, y poda con un `DELETE`. **Y viene en la biblioteca estándar: no
agrega ni una dependencia.**

### FastAPI + uvicorn

**Por qué FastAPI y no Flask:** la validación de entrada y la documentación
automática (`/docs`) salen de los mismos modelos Pydantic que ya definen el
contrato. Con Flask había que escribir la validación a mano y la documentación
aparte — dos lugares donde se desincronizan las cosas.

**Por qué no Django:** trae ORM, admin, migraciones y sistema de plantillas.
Para siete endpoints es cargar un camión para llevar una caja.

**Detalle que costó un bug real:** los endpoints son síncronos, así que
Starlette los corre en su pool de hilos, y cada hilo abre su propia conexión a
MySQL. El límite por defecto es **40 hilos**: sumado al servidor de sockets y a
cinco personas contra la misma base, eso agotaba el plan gratuito de Aiven. El
arranque baja ese límite a 6.

### WebSocket para el dashboard

**El problema:** cada navegador abierto pedía `/api/nodes`, `/api/cluster` y un
`/api/history` **por nodo**, cada pocos segundos. Con tres pantallas abiertas y
diez nodos eran más de treinta consultas a MySQL cada cinco segundos — y aun
así el operador veía el dato hasta cinco segundos tarde.

**La solución:** la API mira la base **una vez por segundo, para todos**, y
empuja el estado por `/ws/cluster`.

| | antes | ahora |
|---|---|---|
| consultas a MySQL | crecían con cada pantalla | **fijas** |
| latencia | hasta 5 s | **< 1 s** |
| sin nadie mirando | seguía consultando | **no consulta nada** |

**Y hay plan B:** los endpoints REST no se tocaron. Si el WebSocket no se puede
abrir (un proxy sin *upgrade*, la API reiniciándose), el dashboard vuelve solo
al polling y **el indicador de arriba dice en qué modo está**. Una pantalla
congelada que parece actualizada es peor que una que avisa.

### psutil

La única forma portable de leer disco, RAM, CPU y red igual en Windows y en
Linux. Lo que **no** da —si un disco es SSD o HDD— se resuelve aparte: en Linux
leyendo `/sys/block/<dev>/queue/rotational`, en Windows con un PowerShell.

### HTML + CSS + JavaScript sin framework

**Por qué no React, Vue ni Angular:** el dashboard es **un archivo** que se
sirve desde la misma API. Sin `npm install`, sin build, sin `node_modules`, sin
versiones que se rompan el día de la defensa. Se abre con doble clic y funciona.

**Por qué los gráficos son SVG escrito a mano y no Chart.js:** un archivo
autocontenido no puede depender de un CDN (si no hay internet en el aula, el
dashboard se queda sin gráficos), y las cuatro formas que hacen falta —barras,
histograma, barras apiladas y una línea con área— son unas pocas líneas de SVG.
Además así el color y las unidades siguen exactamente las reglas del proyecto.

### python-dotenv

Las credenciales viven en un `.env` que **nunca** se sube al repositorio. Todo
el código lee la configuración de un solo módulo (`comun/config.py`); nadie
llama a `os.environ` por su cuenta.

### Aiven (desarrollo) y MySQL local (demo)

Los cinco desarrollamos contra una **misma base en la nube**, así nadie trabaja
con datos inventados. Desde el día 6 se pasa a MySQL local para la demo, porque
la latencia de la nube (**medida: 439 ms por operación**) hace que las pruebas
de carga midan la red y no el código.

---

## 4. Decisiones de diseño que hay que saber defender

**La hora la pone el servidor, no el cliente.** Cada muestra viaja con el
**reloj monotónico** del cliente (`time.monotonic_ns()`), que no se puede
retrasar ni ajustar. El servidor calcula:

```
edad_de_la_muestra = (mono_del_envío − mono_de_la_muestra) / 1e9
timestamp_guardado = hora_del_SERVIDOR − edad_de_la_muestra
```

Cambiar la fecha de un nodo no mueve ni una fila. Y un lote que llega dos horas
tarde queda **repartido** en esas dos horas, no apilado en el instante en que
volvió la red.

**El watchdog es el único que escribe `nodos.estado`.** Ni el alta ni el
guardado de métricas lo tocan. Así cada transición deja su evento en la
bitácora y no hay dos escritores peleándose por la misma columna.

**El ACK se manda ANTES de ejecutar el comando.** Si se mandara después,
cualquier fallo al aplicarlo (disco lleno al escribir el log) dejaría el
mensaje sin confirmar para siempre.

**El mensaje se marca ENVIADO antes de enviarlo.** Un fallo produce una pérdida
visible en vez de un duplicado silencioso.

**Una conexión por hilo, no un pool.** El pool de `mysql-connector` toma un
**lock global del proceso** en cada `get_connection()` y hace un PING mientras
lo tiene. Contra la nube eso serializa justo lo que debería paralelizar:
**medido, 11 inserciones en 20 segundos con 9 hilos**. Con una conexión por
hilo (`threading.local`) el problema desaparece.

**La sincronización es idempotente.** Cada muestra lleva un `seq` que nunca se
repite (`AUTOINCREMENT` en el SQLite del cliente) y el servidor guarda hasta
cuál aceptó. Si el cliente reenvía un lote porque no le llegó la confirmación,
la segunda vez no entra ninguna fila. Sin esto, una reconexión con mala suerte
duplica horas de histórico y el *growth rate* sale al doble.

**El color del dashboard significa estado, no identidad.** Verde / ámbar / rojo
con los mismos cortes (80 % y 90 %) en toda la pantalla, y **el número siempre
escrito al lado**, así el color nunca es el único dato: la pantalla sigue
sirviendo para alguien daltónico. La paleta se validó con un verificador de
separación para daltonismo, no a ojo.

---

## 5. Modelo de datos

| Tabla | Qué guarda | Crece |
|---|---|---|
| `nodos` | quién existe y su **estado actual** | 1 fila por servidor |
| `metricas` | histórico del **primer disco** (lo que pide el enunciado) | ~86.000 filas/día |
| `recursos` | RAM, CPU, red y discos adicionales, en JSON | según qué mida cada nodo |
| `eventos` | bitácora: altas, caídas, recuperaciones, sincronizaciones | por evento |
| `mensajes` | canal servidor → cliente **y bus entre la API y los sockets** | por mensaje |

Cinco vistas sacan la lógica repetida de Python: `v_ultima_metrica`,
`v_recursos_ultimo`, `v_nodos_estado`, `v_cluster` y `v_regionales`.

**Tres conceptos que no hay que confundir:** `node_id` identifica **una
computadora**; `region` es el **departamento** (las nueve regionales del
enunciado, es lo que se suma); `sede` es la **oficina** concreta. Por eso el
departamento de La Paz tiene dos servidores: atiende desde La Paz y desde El
Alto.

**Un detalle de rendimiento que se midió:** `v_ultima_metrica` lleva un
`FORCE INDEX`. Sin él, el optimizador recorre el índice de fechas hacia atrás
hasta topar con el nodo buscado, y eso se degrada **justo con el nodo caído**,
que es el caso que esta práctica tiene que manejar. Medido con 180.000 filas y
un nodo caído: **154 ms sin el hint, 62 ms con él**, y la diferencia crece.

---

## 6. Cómo se verifica

| Prueba | Qué cubre | Necesita |
|---|---|---|
| `scripts/prueba_offline.py` | framing, fechado, saneado, base local, colectores, dashboard | **nada** |
| `python -m db.probar_aiven` | conectividad, TLS y estructura | MySQL |
| `python -m db.probar_bd` | capa de datos y concurrencia real | MySQL |
| `scripts/prueba_integracion.py` | end to end con procesos reales | MySQL |

La primera corre en cualquier máquina en dos segundos y es la que más rápido
avisa si un cambio rompió algo.

---

## 7. Lo que NO se usó, y por qué

- **Docker.** Habría simplificado el despliegue, pero el enunciado pide
  demostrar que corre en dos máquinas reales de la LAN. Un contenedor esconde
  justamente la parte de red que hay que mostrar.
- **Un ORM (SQLAlchemy).** Todo el SQL vive en un solo archivo
  (`db/repositorio.py`) y es SQL a la vista. Para una materia de sistemas
  distribuidos, esconder las consultas detrás de un ORM quita más de lo que da.
- **Redis o una cola de mensajes.** La tabla `mensajes` ya hace de bus, y agrega
  cero infraestructura.
- **TLS entre cliente y servidor.** El enunciado dice LAN. En una WAN real esto
  iría dentro de un túnel; el protocolo no cambiaría.
- **Autenticación de nodos.** Cualquiera en la LAN puede conectarse al 5050 y
  darse de alta: eso es literalmente lo que pide el requisito 7.2. En un sistema
  real se resolvería con un token por nodo. Lo que **sí** está es el saneado: un
  cliente hostil puede registrarse, pero no puede tumbar el servidor ni inyectar
  datos que rompan la base o el navegador.
