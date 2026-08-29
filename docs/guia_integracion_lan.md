# Guía de integración en LAN real — tarea 5.1

El enunciado exige un cliente Windows y un cliente Linux, **en máquinas
distintas**, reportando por IP de red. `localhost`/`127.0.0.1` no cuenta.
Hacer esto con varios días de anticipación (no el día antes de entregar):
casi siempre aparece alguno de estos tres problemas y cada uno cuesta su
rato resolverlo.

---

## 1. Confirmar que el servidor escucha en la interfaz correcta

En `.env` (o por defecto en `comun/config.py`):
```
SOCKET_HOST=0.0.0.0
```
**Ya está así en el proyecto** — `0.0.0.0` significa "todas las interfaces de
red", no solo la de loopback. Si alguien lo cambia a `127.0.0.1` por error,
ningún cliente externo va a poder conectarse aunque el firewall esté abierto
y la IP sea correcta. Verificar esto primero si algo no conecta.

## 2. Obtener la IP real de la máquina servidor

**Windows:**
```powershell
ipconfig
```
Buscar "Dirección IPv4" de la tarjeta conectada a la red del aula (Wi-Fi o
Ethernet, la misma red que van a usar los clientes).

**Linux:**
```bash
ip addr show
# o
hostname -I
```

Anotar esa IP — es la que va en `--host` de los clientes y en el argumento
de `start_demo.sh` / `start_demo.ps1`.

**Cuidado con DHCP:** si el router asigna la IP dinámicamente, puede cambiar
entre el día que probaron y el día de la defensa. Reservar la IP en el
router (DHCP reservation) o usar IP fija en ambas máquinas si el aula lo
permite.

## 3. Abrir los puertos en el firewall

Puertos usados: **5050** (sockets) y **8000** (API + dashboard).

**Windows (en la máquina servidor, PowerShell como administrador):**
```powershell
New-NetFirewallRule -DisplayName "Storage Cluster - Sockets" -Direction Inbound -LocalPort 5050 -Protocol TCP -Action Allow
New-NetFirewallRule -DisplayName "Storage Cluster - Dashboard" -Direction Inbound -LocalPort 8000 -Protocol TCP -Action Allow
```

**Linux (ufw):**
```bash
sudo ufw allow 5050/tcp
sudo ufw allow 8000/tcp
```

**Linux (firewalld):**
```bash
sudo firewall-cmd --add-port=5050/tcp --permanent
sudo firewall-cmd --add-port=8000/tcp --permanent
sudo firewall-cmd --reload
```

Si el firewall corporativo/de laboratorio bloquea igual, probar con una red
compartida por celular (hotspot) como alternativa — más lento pero funciona
para la demo.

## 4. Antivirus

Windows Defender rara vez bloquea Python directamente, pero algunos
antivirus de terceros sí filtran conexiones salientes/entrantes nuevas de un
proceso no firmado. Si el firewall de Windows está bien configurado y aun
así no conecta, es la primera sospecha siguiente: agregar una excepción para
`python.exe` o desactivar temporalmente para la prueba y confirmar que ese
era el problema.

## 5. Probar la conectividad TCP antes de correr el proyecto

No hace falta levantar todo el sistema para confirmar que la red deja pasar
el tráfico. Con el servidor de sockets ya escuchando:

**Desde la máquina cliente (Windows):**
```powershell
Test-NetConnection -ComputerName 192.168.X.X -Port 5050
```
`TcpTestSucceeded : True` confirma que el puerto es alcanzable.

**Desde la máquina cliente (Linux):**
```bash
nc -zv 192.168.X.X 5050
# o
curl -v telnet://192.168.X.X:5050
```

Si esto falla, el problema es de red/firewall, no del código Python — ahorra
tiempo de debugging en el lugar equivocado.

## 6. Ejecución real de la prueba

1. En la máquina servidor: `./scripts/start_demo.sh <IP_SERVIDOR>` (o el
   `.ps1` en Windows) — ver [runbook_demo.md](runbook_demo.md).
2. En la máquina Windows (cliente 1):
   ```powershell
   python -m cliente.main --node-id CNS-LPZ-01 --region "La Paz" --host <IP_SERVIDOR>
   ```
3. En la máquina Linux (cliente 2):
   ```bash
   python -m cliente.main --node-id CNS-CBB-02 --region "Cochabamba" --host <IP_SERVIDOR>
   ```
4. Abrir `http://<IP_SERVIDOR>:8000` desde una tercera máquina o desde el
   celular conectado a la misma red — confirma que el dashboard también es
   alcanzable en red, no solo desde el propio servidor.
5. Verificar que ambos nodos aparecen con datos **reales** de sus discos
   (no simulados) — el tipo de disco y las unidades van a ser distintas
   entre Windows (`C:\`) y Linux (`/`), eso es lo esperado.
6. **Prueba de resiliencia de red:** desconectar el Wi-Fi/cable de uno de
   los clientes 15-20 segundos y volver a conectar. Debe pasar a
   `NO_REPORTA` y volver solo a `ACTIVO` — mismo mecanismo que la
   [prueba 2 del plan de fallos](plan_pruebas_fallo.md).

## 7. Checklist final de M5.1

- [ ] Cliente Windows y cliente Linux, en máquinas físicas distintas.
- [ ] Ambos conectan por IP de red (no `localhost`/`127.0.0.1`).
- [ ] El dashboard muestra a ambos con datos reales de sus discos.
- [ ] Probado desconectando y reconectando la red de un cliente.
- [ ] IPs anotadas y, si es posible, reservadas para el día de la defensa.

Responsable: Alexander (M5.1).
