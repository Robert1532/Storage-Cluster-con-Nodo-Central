# Storage Cluster con Nodo Central de Monitoreo

### El problema

La Caja Nacional de Salud tiene servidores de archivos en nueve
administraciones regionales, cada uno guardando historiales clínicos. No existe
forma de saber, desde un solo lugar, cuánto espacio le queda a cada uno ni si
alguno dejó de funcionar. Cuando un disco se llena, se descubre cuando ya es
tarde.

### La solución

Un sistema de monitoreo centralizado con **cuatro programas**: un **agente** en
cada servidor regional que mide su disco, un **nodo central** que recibe por
sockets TCP y persiste en MySQL, una **API** que expone los datos, y un
**dashboard web** que los muestra en tiempo real. El operador ve el estado de
todo el clúster en una pantalla y puede mandarle mensajes a cualquier nodo, con
confirmación de recepción.

Está implementado en **Python 3.12** (sockets y threading de la biblioteca
estándar), **MySQL 8.0**, **FastAPI con WebSocket** y **SQLite** en cada
cliente. El dashboard es un único archivo HTML sin framework ni dependencias
externas: se sirve desde la misma API y funciona sin internet.

### Cómo funciona

Cada agente mide su disco —y, si se le pide, también RAM, CPU, red y las demás
unidades— y lo manda como un JSON por línea sobre TCP. El servidor central
atiende **un hilo por cliente**, guarda cada medición y responde. Un hilo
*watchdog* marca como caído a cualquier nodo que deje de reportar dentro de su
umbral, y un *despachador* lleva al socket los mensajes que el operador encola
desde el dashboard.

Tres decisiones sostienen el sistema:

**Nada se pierde.** Cada agente guarda toda medición en una base SQLite propia
**antes** de intentar enviarla. Si no hay red, la medición sigue ahí. Al
reconectar entrega en lotes todo lo que el servidor se perdió, y el hueco del
gráfico se rellena solo. La entrega es idempotente: reenviar un lote no
duplica ni una fila.

**La hora la pone el servidor.** Cada muestra viaja con el reloj *monotónico*
del cliente —que no se puede retrasar ni ajustar— y el servidor calcula la hora
real a partir de la suya. Cambiar la fecha de un nodo no mueve ni una fila del
histórico, y un lote atrasado queda repartido en el tiempo que realmente cubre.

**Agregar una métrica nueva no toca la base.** Las mediciones que no son el
disco principal se guardan como JSON, con las tres columnas que siempre se
consultan materializadas e indexadas. Para que el sistema mida algo nuevo
alcanza con una función de diez líneas en el agente.

### Qué se logró

| Indicador | Resultado |
|---|---|
| Servidores monitoreados | 10 en 9 departamentos (La Paz con dos sedes: La Paz y El Alto) |
| Frecuencia de reporte | configurable en caliente desde el dashboard, 10 s por defecto |
| Volumen | ~86.000 mediciones por día |
| Latencia del dashboard | menos de 1 segundo, sin recargar la página |
| Tolerancia a cortes | ~55 horas sin red sin perder una sola medición |

Una computadora nueva se suma al clúster con **un comando** y aparece sola en
el dashboard: no hay que registrarla, ni reiniciar el servidor, ni tocar la base
de datos. Cuando un nodo se cae, la pantalla muestra la fecha y el motivo; si se
cae y vuelve repetidamente, lo marca como intermitente, que es un problema
distinto a estar caído.

### Verificación

El sistema se comprueba con cuatro pruebas automáticas. La primera —**45
comprobaciones sobre el protocolo, el fechado, la base local y el
dashboard**— no necesita base de datos y corre en cualquier máquina en dos
segundos. Las otras tres se ejecutan contra un MySQL real y cubren la capa de
datos, la concurrencia con un hilo por nodo escribiendo a la vez, y un ciclo
completo end to end levantando servidor y clientes de verdad.

Durante el desarrollo esas pruebas detectaron condiciones de carrera reales en
el alta de nodos y en el watchdog, la pérdida de 148 de 600 mensajes por
escritura concurrente sobre un mismo socket, y una consulta que degradaba de 62
a 154 milisegundos justo con un nodo caído.

### Conclusión

El sistema cumple los requisitos del enunciado —comunicación bidireccional con
confirmación, alta automática de clientes, intervalo parametrizable, estado "No
Reporta" y consolidados del clúster— y agrega lo que un despliegue real
necesita: que una caída de red deje un hueco temporal y no permanente, que el
reloj de una máquina no pueda ensuciar el histórico, y que medir algo nuevo no
obligue a migrar la base de datos.
