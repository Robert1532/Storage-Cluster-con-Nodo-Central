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

set -uo pipefail
cd "$(dirname "$0")/.."

HOST_SERVIDOR="${1:-127.0.0.1}"

# En Ubuntu sin el venv activado no existe el comando `python`, solo `python3`.
# Si hay venv en el proyecto, se usa ese.
if [[ -x ".venv/bin/python" ]]; then
  PY=".venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY="python3"
else
  PY="python"
fi

PIDS=()

limpiar() {
  echo
  echo "Bajando todo..."
  # Se recorre al reves: primero los nodos, al final el servidor.
  for (( i=${#PIDS[@]}-1 ; i>=0 ; i-- )); do
    kill "${PIDS[i]}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  echo "Listo."
}
# El trap se registra ANTES de lanzar nada: un Ctrl+C durante el chequeo de la
# base tambien tiene que limpiar. Se incluye EXIT para la salida normal.
trap limpiar INT TERM EXIT

lanzar() {
  local nombre="$1"; shift
  "$@" &
  local pid=$!
  PIDS+=("$pid")
  sleep 2
  # `set -e` NO detecta que un proceso en segundo plano se murio, y `wait`
  # devuelve 0 igual. Sin esta comprobacion el script imprimiria "todo arriba"
  # con la API muerta.
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "FALLO: $nombre no se mantuvo en pie. Revisa el error de arriba."
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

echo "[2/4] Servidor de sockets..."
lanzar "servidor de sockets" "$PY" -m servidor.main

echo "[3/4] API + dashboard..."
lanzar "API" "$PY" -m uvicorn api.main:app --host 0.0.0.0 --port 8000

echo "[4/4] Nueve nodos regionales..."
lanzar "nodos" "$PY" scripts/lanzar_nodos.py --host "$HOST_SERVIDOR"

echo
echo "Todo arriba. Dashboard: http://$HOST_SERVIDOR:8000"
echo "Ctrl+C para bajar todo."
echo
echo "PARA LA DEMO EN VIVO (alta automatica, requisito 7.2):"
echo "  $PY -m cliente.main --node-id CNS-XXX-10 --region 'Nueva Regional' --host $HOST_SERVIDOR"
echo

wait
