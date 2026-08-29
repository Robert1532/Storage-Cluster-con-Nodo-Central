# Runbook de la demo — Storage Cluster CNS

Tarea 5.3. Imprimir y llevar en papel a la defensa: los nervios borran pasos
que uno cree tener memorizados. Que alguien que NO escribió este runbook lo
siga paso a paso antes del día de la defensa, sin ayuda de quien lo redactó.

---

## 0. Antes de salir de casa (el día anterior)

- [ ] `git pull` en la máquina que va a ser el servidor central.
- [ ] `.env` apuntando a MySQL **local** (no Aiven) — sección "OPCION B" del `.env.example`.
- [ ] `mysql -u root -p < db/schema_local.sql` y `mysql -u root -p cns_cluster < db/schema.sql` ya corridos.
- [ ] `python -m db.probar_bd` pasa completo.
- [ ] Firewall abierto en los puertos 5050 (sockets) y 8000 (dashboard) — ver [guia_integracion_lan.md](guia_integracion_lan.md).
- [ ] Batería cargada, cargador a mano, ambas laptops con la IP fija o reservada en el router (evitar que DHCP la cambie a mitad de defensa).
- [ ] Video de respaldo (M5.4) descargado localmente, no solo en la nube — el aula puede no tener internet.

## 1. Orden de arranque (no se puede alterar)

1. **MySQL** arriba y accesible.
2. **Servidor de sockets** (`servidor/main.py`) — hace `bind` en `0.0.0.0:5050`.
3. **API + dashboard** (`uvicorn api.main:app`) — sirve en `0.0.0.0:8000`.
4. **Los 9 nodos regionales** (`scripts/lanzar_nodos.py`).

Por qué este orden y no otro: el servidor de sockets necesita MySQL para
`registrar_nodo`; la API necesita MySQL para responder `/api/nodes`; los
nodos necesitan que el servidor de sockets ya esté escuchando o entran en el
bucle de reconexión con backoff (1, 2, 4, 8s) — no rompen nada, pero la demo
se ve mejor si no hay que esperar la reconexión en vivo.

## 2. Comando único

En la máquina que hace de servidor central, con la IP de esa máquina en la
red del aula (ver paso 3 de la guía LAN para obtenerla):

**Linux / macOS / Git Bash en Windows:**
```bash
./scripts/start_demo.sh 192.168.X.X
```

**Windows (PowerShell):**
```powershell
.\scripts\start_demo.ps1 -HostServidor 192.168.X.X
```

El script verifica MySQL, levanta el servidor de sockets, espera a que el
puerto 5050 responda, levanta la API, espera al puerto 8000, y recién
entonces lanza los 9 nodos. Si algo no levanta, el script se detiene solo con
un mensaje — no sigue a ciegas.

Dashboard: `http://192.168.X.X:8000` — abrirlo desde la otra laptop (el
cliente real), no solo desde el servidor, para confirmar que la red
realmente funciona.

## 3. En vivo, cuando el tribunal lo pida

**Alta automática de un nodo nuevo (requisito 7.2):**
```bash
python -m cliente.main --node-id CNS-XXX-10 --region "Nueva Regional" --host 192.168.X.X
```
Debe aparecer solo en el dashboard, sin reiniciar nada.

**Mandar un mensaje a un nodo (requisito 7.1):** desde el dashboard, elegir
un nodo, escribir o elegir un mensaje predefinido, enviar. Confirmar que
aparece el ACK en pantalla y la línea nueva en `logs/cliente_<node_id>.log`.

**Cambiar el intervalo en caliente (requisito 7.3):** desde el dashboard,
cambiar el intervalo de un nodo. No hace falta reiniciar el cliente — si
tarda en aplicarse más de un ciclo completo, es la señal de que se usó
`time.sleep()` en vez de `threading.Event` (avisar a Martin).

**Matar el servidor central a la mitad:** el tribunal puede pedirlo. Los
clientes deben seguir vivos, reintentando solos, y reconectarse apenas se
vuelve a levantar `servidor/main.py`. Ver [plan_pruebas_fallo.md](plan_pruebas_fallo.md).

## 4. Apagado

`Ctrl+C` en la terminal del script — baja los 9 nodos, la API y el servidor
de sockets en cascada. Si algún proceso queda colgado:

```bash
# Linux
pkill -f "servidor.main"; pkill -f "uvicorn"; pkill -f "cliente.main"
```
```powershell
# Windows
Get-Process python,uvicorn -ErrorAction SilentlyContinue | Stop-Process -Force
```

## 5. Si algo falla en vivo

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| El script se detiene en "[1/4]" | MySQL apagado o `.env` mal configurado | Revisar servicio MySQL, revisar `.env` (host/usuario/clave) |
| Se detiene en "[2/4]" | Puerto 5050 ocupado o bloqueado por firewall | Ver guía LAN; probar `netstat -ano \| findstr 5050` |
| Los clientes no aparecen en el dashboard | IP equivocada, o servidor escuchando en 127.0.0.1 | Confirmar `SOCKET_HOST=0.0.0.0` en `.env`; usar la IP de red, no `localhost` |
| El proyector o la red del aula fallan | — | Pasar al **Plan B**: [plan_b_respaldo.md](plan_b_respaldo.md) — video + capturas |

---
Responsable: Alexander (M5 — Integración, pruebas y respaldo de la demo).
