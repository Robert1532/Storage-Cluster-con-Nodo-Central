#!/usr/bin/env bash
# Runbook de la demo — tarea 5.3.  Responsable: Alexander.
#
#   ./scripts/start_demo.sh 192.168.1.100
#
# ORDEN CORRECTO (no lo cambien, importa):
#   1. MySQL arriba
#   2. Servidor de sockets
#   3. API + dashboard
#   4. Los 9 nodos
#   5. (en vivo, durante la defensa) un nodo nuevo -> alta automatica

set -euo pipefail
cd "$(dirname "$0")/.."

HOST_SERVIDOR="${1:-127.0.0.1}"

echo "=========================================="
echo " Storage Cluster CNS - arranque de la demo"
echo " Servidor central: $HOST_SERVIDOR"
echo "=========================================="

echo
echo "[1/4] Verificando la base de datos..."
python -m db.probar_aiven \
  || { echo "MySQL no responde. Revisen el servicio y el archivo .env"; exit 1; }

echo "[2/4] Servidor de sockets..."
python -m servidor.main & PID_SOCKET=$!
sleep 2

echo "[3/4] API + dashboard en http://$HOST_SERVIDOR:8000 ..."
uvicorn api.main:app --host 0.0.0.0 --port 8000 & PID_API=$!
sleep 2

echo "[4/4] Nueve nodos regionales..."
python scripts/lanzar_nodos.py --host "$HOST_SERVIDOR" & PID_NODOS=$!

echo
echo "Todo arriba. Dashboard: http://$HOST_SERVIDOR:8000"
echo "Ctrl+C para bajar todo."
echo
echo "PARA LA DEMO EN VIVO (alta automatica, requisito 7.2):"
echo "  python -m cliente.main --node-id CNS-XXX-10 --region 'Nueva Regional' --host $HOST_SERVIDOR"

trap 'echo; echo "Bajando..."; kill $PID_NODOS $PID_API $PID_SOCKET 2>/dev/null || true' INT TERM
wait
