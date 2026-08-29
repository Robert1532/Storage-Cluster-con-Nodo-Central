#!/usr/bin/env bash
# Runbook de la demo — tarea 5.3.  Responsable: Alexander.
#
#   ./scripts/start_demo.sh 192.168.1.100
#
# ORDEN CORRECTO (no lo cambien, importa):
#   1. La base de datos responde
#   2. Servidor de sockets
#   3. API + dashboard
#   4. Los 9 nodos
#   5. (en vivo, durante la defensa) un nodo nuevo -> alta automatica
#
# Cada paso espera a que el puerto anterior este realmente escuchando antes
# de seguir (wait_for_port), no un sleep fijo: en la maquina del aula puede
# tardar mas o menos que en la nuestra, y ahi es donde la demo se traba en
# vivo.
#
# OJO WINDOWS: probado en Git Bash. El "kill" de bash NO mata de forma
# confiable un python.exe/uvicorn.exe nativo de Windows (no son procesos
# MSYS) — se queda "matado" en apariencia pero el proceso sigue vivo. "$!"
# tambien da el PID de MSYS, no el PID nativo que taskkill necesita:
# /proc/<pid>/winpid hace la traduccion. En Linux/macOS, kill normal
# funciona bien.

set -uo pipefail
cd "$(dirname "$0")/.."

# En Ubuntu sin el venv activado no existe el comando `python`, solo
# `python3`. Si hay venv en el proyecto (Linux o Windows), se usa ese.
if [[ -x ".venv/bin/python" ]]; then
  PY=".venv/bin/python"
elif [[ -x ".venv/Scripts/python" ]]; then
  PY=".venv/Scripts/python"
elif command -v python3 >/dev/null 2>&1; then
  PY="python3"
else
  PY="python"
fi

HOST_SERVIDOR="${1:-127.0.0.1}"
PUERTO_SOCKET="${SOCKET_PORT:-5050}"
PUERTO_API="${API_PORT:-8000}"

# Mata un proceso y todo su arbol de hijos (lanzar_nodos.py tiene 9 hijos).
# En Windows usa taskkill con el PID nativo (ver nota de arriba); en
# Linux/macOS, kill normal alcanza.
matar_arbol() {
  local pid="$1"
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*)
      local winpid="$pid"
      if [ -r "/proc/$pid/winpid" ]; then
        winpid="$(cat "/proc/$pid/winpid" 2>/dev/null || echo "$pid")"
      fi
      taskkill //F //T //PID "$winpid" >/dev/null 2>&1 || true
      ;;
    *)
      kill "$pid" 2>/dev/null || true
      ;;
  esac
}

# Se declara vacio ANTES de arrancar nada y el trap se registra YA, no al
# final: si alguien corta con Ctrl+C a mitad del arranque (paso 2 o 3), sin
# esto el trap todavia no existe y bash mata el script sin limpiar nada.
PIDS=()

limpiar() {
  # Se desarma el propio trap ya mismo: sin esto, terminar aqui dispara el
  # EXIT del trap de nuevo y "Bajando todo... Listo." se imprime 2-3 veces
  # por cada Ctrl+C.
  trap - INT TERM EXIT
  echo
  echo "Bajando todo..."
  # Al reves: primero los nodos, al final el servidor de sockets.
  for (( i=${#PIDS[@]}-1 ; i>=0 ; i-- )); do
    matar_arbol "${PIDS[i]}"
  done
  wait 2>/dev/null || true
  echo "Listo."
}
# INT/TERM (Ctrl+C) y EXIT (salida normal, incluida por error): asi ningun
# camino de salida deja procesos huerfanos corriendo.
trap limpiar INT TERM EXIT

# Espera activa a que un puerto TCP responda. Usa /dev/tcp de bash: no
# necesita instalar nc ni telnet en la maquina del aula.
wait_for_port() {
  local host="$1" port="$2" timeout="${3:-15}" esperado=0
  until (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; do
    exec 3>&- 2>/dev/null || true
    esperado=$((esperado + 1))
    if [ "$esperado" -ge "$timeout" ]; then
      echo "  -> Timeout esperando ${host}:${port} (${timeout}s). Revisa el log de arriba."
      return 1
    fi
    sleep 1
  done
  exec 3>&- 2>/dev/null || true
  return 0
}

# Lanza un proceso, lo registra para el apagado, y si tiene puerto propio
# confirma que quede escuchando de verdad (un proceso puede seguir "vivo"
# y aun asi haber fallado el bind).
lanzar() {
  local nombre="$1" puerto="$2"; shift 2
  "$@" &
  local pid=$!
  PIDS+=("$pid")
  if [ -n "$puerto" ] && ! wait_for_port 127.0.0.1 "$puerto" 20; then
    echo "FALLO: $nombre no llego a escuchar en el puerto $puerto. Abortando."
    exit 1
  fi
  echo "      $nombre OK (pid $pid)"
}

echo "=========================================="
echo " Storage Cluster CNS - arranque de la demo"
echo " Servidor central: $HOST_SERVIDOR"
echo " Python: $PY"
echo "=========================================="

echo
echo "[1/4] Verificando la base de datos..."
if ! "$PY" -m db.probar_aiven; then
  echo "La base no responde. Revisa el .env y que el servicio este arriba."
  exit 1
fi

echo "[2/4] Servidor de sockets (puerto $PUERTO_SOCKET)..."
lanzar "servidor de sockets" "$PUERTO_SOCKET" "$PY" -m servidor.main

echo "[3/4] API + dashboard en http://$HOST_SERVIDOR:$PUERTO_API ..."
lanzar "API" "$PUERTO_API" "$PY" -m uvicorn api.main:app --host 0.0.0.0 --port "$PUERTO_API"

echo "[4/4] Nueve nodos regionales..."
lanzar "nodos" "" "$PY" scripts/lanzar_nodos.py --host "$HOST_SERVIDOR" --puerto "$PUERTO_SOCKET"

echo
echo "Todo arriba. Dashboard: http://$HOST_SERVIDOR:$PUERTO_API"
echo "Ctrl+C para bajar todo."
echo
echo "PARA LA DEMO EN VIVO (alta automatica, requisito 7.2):"
echo "  $PY -m cliente.main --node-id CNS-XXX-10 --region 'Nueva Regional' --host $HOST_SERVIDOR --puerto $PUERTO_SOCKET"

wait
