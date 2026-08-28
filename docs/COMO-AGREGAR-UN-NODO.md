# Cómo se agrega una computadora al clúster

Este documento responde cinco preguntas que aparecen siempre, y que casi seguro
va a hacer el tribunal:

1. ¿Una computadora es un departamento?
2. Cuando levanto "La Paz", ¿qué es lo que se levanta?
3. ¿La Paz tiene dos servidores porque son La Paz y El Alto?
4. Si el ingeniero quiere sumar **su laptop** al clúster, ¿qué hace?
5. ¿Hay que registrarlo en algún lado primero?

---

## 1. El modelo en una frase

> **Una computadora = un nodo = un servidor de archivos.**
> **Un departamento = una regional = puede tener uno o varios nodos.**

Tres cosas distintas, y conviene no mezclarlas:

| | Qué es | Ejemplo |
|---|---|---|
| **`node_id`** | El nombre único de **una computadora**. Dos máquinas nunca lo comparten. | `CNS-ELA-10` |
| **`region`** | El **departamento**: una de las nueve administraciones regionales del enunciado. Es lo que se suma en el consolidado. | `La Paz` |
| **`sede`** | La **oficina** concreta donde está esa máquina. | `El Alto` |

Por eso La Paz tiene **dos** servidores: el departamento de La Paz atiende
desde la ciudad de La Paz y desde El Alto, y cada oficina tiene su propio
servidor de archivos. Son dos computadoras, dos `node_id`, dos filas en la
base — pero **una sola regional** en el consolidado.

En el dashboard eso se ve así: las dos tarjetas dicen `La Paz` como
departamento, una dice sede **La Paz** y la otra sede **El Alto**, y en el
gráfico *Capacidad por regional* aparece **una sola barra "La Paz"** con la
suma de las dos.

Si mañana Santa Cruz necesita un segundo servidor, es exactamente lo mismo: se
levanta otra computadora con `--region "Santa Cruz" --sede "Montero"` y listo.
No hay que tocar nada.

---

## 2. ¿Qué se levanta cuando "levanto La Paz"?

Depende de con qué comando:

### a) Una computadora real (lo que hay que mostrar en la defensa)

```bash
python -m cliente.main --node-id CNS-LPZ-01 --region "La Paz" --sede "La Paz" \
       --host 192.168.1.100
```

Eso arranca **un proceso** que mide **el disco de verdad de esa máquina** y lo
reporta. Si lo corrés en la laptop del ingeniero, los números que aparecen en
el dashboard son **su disco real**: su capacidad, su espacio usado, su RAM, su
CPU. No hay nada simulado.

### b) Varias a la vez, para llenar el dashboard

```bash
python scripts/lanzar_nodos.py --host 192.168.1.100
```

Esto levanta **diez procesos** en la misma computadora, uno por regional. No
son diez computadoras: son diez clientes en la misma máquina, así que **los
diez reportan el mismo disco** — el de esa máquina. Sirve para que el
consolidado se vea completo y para probar la concurrencia del servidor, no para
demostrar que el sistema es distribuido.

> **Para la defensa esto importa:** el enunciado pide **dos clientes reales en
> máquinas distintas** más el servidor central. `lanzar_nodos.py` rellena el
> resto; los dos reales son los que prueban que funciona en red.

---

## 3. Sumar una laptop al clúster — la forma corta

En la laptop nueva, con el proyecto clonado y las dependencias instaladas:

```bash
python scripts/unirse.py --host 192.168.1.100
```

Le pregunta tres cosas y arranca:

```
  A que administracion regional pertenece esta computadora?
    1. La Paz     4. Oruro      7. Tarija
    2. Cochabamba 5. Potosi     8. Beni
    3. Santa Cruz 6. Chuquisaca 9. Pando

  Numero (1-9): 3
  Sede [Santa Cruz]: Montero
  Nombre unico de este nodo [CNS-MON-47]:
```

El `node_id` que sugiere lleva las dos últimas cifras de la IP de esa máquina,
así que dos laptops distintas en la misma red nunca generan el mismo.

Sin preguntas, para un guion o para la demo:

```bash
python scripts/unirse.py --host 192.168.1.100 --region "Santa Cruz" --sede Montero --si
```

**Aparece en el dashboard en menos de un intervalo.** No hay que registrarlo,
ni reiniciar el servidor, ni tocar la base de datos: el servidor lo da de alta
solo la primera vez que lo ve. Eso es el **requisito 7.2**, y en la bitácora
queda el evento `ALTA_AUTOMATICA` como prueba.

---

## 4. Lo único que hace falta del otro lado: nada

Esta es la parte que suele sorprender, y conviene decirla con todas las letras
en la defensa:

> Para que una computadora se una al clúster, **en el servidor no se toca nada**.

El servidor escucha en el puerto 5050. Cuando llega un `HELLO` con un `node_id`
que no conoce:

1. lo inserta en la tabla `nodos` con estado `ACTIVO`,
2. deja el evento `ALTA_AUTOMATICA` en la bitácora,
3. le responde `HELLO_OK` con `nuevo: true`,
4. el dashboard lo muestra en el siguiente empujón — menos de un segundo.

La lista de `comun/config.py::REGIONALES` **no es un registro de máquinas
autorizadas**: es solamente la lista que usa `lanzar_nodos.py` para la demo. Un
nodo que no esté ahí se une igual.

### Lo que sí hay que verificar antes

| | Cómo se comprueba |
|---|---|
| Las dos máquinas se ven en la red | `ping 192.168.1.100` desde la laptop |
| El puerto 5050 está abierto | En el servidor, el firewall de Windows suele bloquearlo: hay que permitir Python en redes privadas |
| La laptop tiene Python 3.12 y las dependencias | `pip install -r requirements.txt` |
| El servidor está corriendo | Tiene que decir `Escuchando en 0.0.0.0:5050` |

El error más común de la demo es el **firewall de Windows**, no el código. Si
el cliente dice `Sin servidor ([WinError 10061]...)` una y otra vez, el
problema está ahí. Se comprueba rápido desde la laptop:

```powershell
Test-NetConnection 192.168.1.100 -Port 5050
```

---

## 5. Preguntas que va a hacer el tribunal

**¿Cada laptop es un departamento?**
Cada laptop es **un servidor**. El departamento es a qué regional pertenece ese
servidor, y puede haber más de uno por departamento — La Paz tiene dos, La Paz
y El Alto. Lo que identifica a la máquina es el `node_id`, no la región.

**¿Y si dos personas ponen el mismo `node_id`?**
El servidor cierra la conexión anterior y se queda con la nueva; las dos
máquinas escribirían en la misma fila y los datos saldrían mezclados. Por eso
`unirse.py` genera el id a partir de la IP en vez de dejarlo a mano. Es un
problema de operación, no del protocolo.

**¿Y si se llenan las plazas?**
`MAX_NODOS` en el `.env` (por defecto 12). El nodo que sobra recibe un `ERROR`
explicando por qué y **termina**, en vez de quedarse reintentando contra un
servidor que nunca lo va a aceptar. Para demostrar el "soporte exacto para 9
clientes" del enunciado, se pone `MAX_NODOS=9` y se levanta el décimo.

**¿Qué pasa si desconecto la laptop a mitad de la demo?**
Sigue midiendo y guardando en su base local. Al reconectarla entrega todo lo
que el servidor se perdió y el hueco del gráfico se rellena solo. Se puede
mirar mientras está desconectada:

```bash
python -m cliente.almacen --node-id CNS-MON-47
```

**¿La laptop del ingeniero tiene que quedarse conectada?**
No. Ctrl+C y listo. A los pocos segundos el dashboard lo marca `NO REPORTA` y
muestra **la fecha y el motivo** de la desconexión. Si la vuelve a conectar,
recupera lo que midió mientras tanto.

---

## 6. Guion de tres minutos para mostrarlo en vivo

1. **Antes**: el dashboard muestra los nodos que ya están. Se lee en voz alta el
   contador `Reportando 9 / 9`.
2. En la laptop del ingeniero: `python scripts/unirse.py --host <IP>`.
   Elegir la regional y la sede delante de él.
3. **Sin tocar nada más**, el contador pasa a `10 / 10`, aparece la tarjeta
   nueva con **su disco real**, y en la bitácora sale `ALTA AUTOMATICA`.
4. Desde el dashboard, clic en su tarjeta → *Enviar mensaje* → llega el **ACK**
   con el tiempo de ida y vuelta en milisegundos, y el texto queda escrito en
   `logs/cliente_<su-nodo>.log` **en su máquina**.
5. Ctrl+C en su laptop → a los segundos la tarjeta dice *"Se desconectó de la
   red el ..."* con el motivo.
6. Volver a arrancarlo → el hueco del histórico se rellena con los datos que
   guardó mientras estuvo fuera.
