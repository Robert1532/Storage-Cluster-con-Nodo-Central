# Plan B — video de respaldo y capturas (tarea 5.4)

Por si el día de la defensa falla el proyector, la red del aula, o una
máquina no enciende. Se prepara **antes**, con el sistema funcionando de
verdad — nunca se improvisa el día de la defensa.

---

## 1. Video de respaldo

**Contenido mínimo (grabar en una sola toma o con cortes claros):**

1. Arranque completo con el runbook (`start_demo.sh` / `.ps1`) desde cero,
   mostrando las 4 etapas levantando en orden.
2. Dashboard abierto mostrando los 9 nodos con datos reales (no en blanco).
3. Cliente Windows y cliente Linux en máquinas distintas, reportando por IP
   de red (la parte de M5.1) — mostrar la IP en pantalla en ambos lados.
4. Mandar un mensaje a un nodo desde el dashboard y mostrar el ACK volviendo.
5. Cambiar el intervalo de un nodo desde el dashboard y mostrar que se aplica
   sin reiniciar el cliente.
6. Matar el servidor central en vivo y mostrar que los clientes no se caen
   y se reconectan solos al volver a levantarlo (prueba 1 de
   [plan_pruebas_fallo.md](plan_pruebas_fallo.md)).
7. Conectar un nodo nuevo sin reiniciar nada y mostrar la alta automática.
8. Narración en audio explicando qué está pasando en cada paso — el video
   tiene que poder verse SIN que quien lo grabó esté presente explicando.

**Duración objetivo:** 5-8 minutos. Más largo se vuelve tedioso para el
tribunal si hay que usarlo; más corto no cubre lo pedido.

## 2. Capturas de pantalla

Guardar como imágenes (no solo en el video), con el sistema corriendo con
datos reales:

- [ ] Dashboard completo: tabla de 9 nodos + panel de KPIs.
- [ ] Un nodo en estado `NO_REPORTA` (rojo) para probar que ese estado existe.
- [ ] Gráfico histórico de utilización con datos acumulados.
- [ ] Log de un cliente (`logs/cliente_<id>.log`) mostrando mensajes CMD y
  su ACK.
- [ ] Consulta directa a MySQL mostrando filas reales en `nodos`, `metricas`,
  `eventos` y `mensajes` (evidencia de que el histórico existe de verdad).
- [ ] `/docs` de la API (Swagger de FastAPI) mostrando los endpoints.

## 3. Exportar evidencia de la base de datos automáticamente

Con el sistema arriba y reportando, correr:
```bash
python scripts/exportar_respaldo.py
```
Esto genera `respaldo_demo/<fecha_hora>/` con `nodos.json`, `cluster.json`,
`eventos.json`, `mensajes.json` y una copia de todos los `logs/*.log` tal
como estaban en ese momento. Es evidencia verificable de que los datos son
reales y no inventados para el informe — correrlo más de una vez en
distintos momentos de una sesión de prueba da además material para mostrar
la evolución en el gráfico histórico.

## 4. Dónde guardar todo

- **Pendrive** (al menos 2, por si uno falla) con: video, capturas, carpetas
  de `respaldo_demo/`.
- **Nube** (Drive/similar) como segunda copia — pero no depender solo de
  ella: el aula puede no tener internet el día de la defensa.
- **Laptop del presentador**, copia local además del pendrive.

## 5. Checklist final de M5.4

- [ ] Video grabado con audio, cubre los 7 puntos de la sección 1.
- [ ] Capturas de dashboard, logs y base de datos con datos reales.
- [ ] `respaldo_demo/` generado con `exportar_respaldo.py` al menos una vez
  durante una sesión de prueba completa.
- [ ] Todo guardado en pendrive (x2) y en la nube.
- [ ] Alguien del equipo distinto a quien lo preparó confirmó que el video
  reproduce bien y las capturas se entienden sin contexto adicional.

Responsable: Alexander (M5.4).
