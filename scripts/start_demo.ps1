# Runbook de la demo (version Windows) — tarea 5.3. Responsable: Alexander.
#
#   .\scripts\start_demo.ps1 -HostServidor 192.168.1.100
#
# ORDEN CORRECTO (no lo cambien, importa):
#   1. MySQL arriba
#   2. Servidor de sockets
#   3. API + dashboard
#   4. Los 9 nodos
#   5. (en vivo, durante la defensa) un nodo nuevo -> alta automatica
#
# Equivalente a start_demo.sh para cuando la maquina que hace de servidor
# central el dia de la defensa es Windows. Cada paso espera a que el puerto
# anterior este realmente escuchando antes de seguir.

param(
    [string]$HostServidor = "127.0.0.1",
    [int]$PuertoSocket = 5050,
    [int]$PuertoApi = 8000
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

function Wait-Port {
    param([string]$TargetHost, [int]$Port, [int]$TimeoutSeg = 20)
    $esperado = 0
    while ($esperado -lt $TimeoutSeg) {
        $ok = Test-NetConnection -ComputerName $TargetHost -Port $Port -WarningAction SilentlyContinue
        if ($ok.TcpTestSucceeded) { return $true }
        Start-Sleep -Seconds 1
        $esperado++
    }
    return $false
}

Write-Host "=========================================="
Write-Host " Storage Cluster CNS - arranque de la demo"
Write-Host " Servidor central: $HostServidor"
Write-Host "=========================================="

Write-Host ""
Write-Host "[1/4] Verificando la base de datos..."
python -m db.probar_aiven
if ($LASTEXITCODE -ne 0) {
    Write-Host "MySQL no responde. Revisen el servicio y el archivo .env"
    exit 1
}

Write-Host "[2/4] Servidor de sockets (puerto $PuertoSocket)..."
$procSocket = Start-Process -PassThru -NoNewWindow python -ArgumentList "-m", "servidor.main"
if (-not (Wait-Port -TargetHost "127.0.0.1" -Port $PuertoSocket -TimeoutSeg 20)) {
    Write-Host "El servidor de sockets no llego a escuchar. Abortando."
    Stop-Process -Id $procSocket.Id -Force -ErrorAction SilentlyContinue
    exit 1
}
Write-Host "  -> escuchando."

Write-Host "[3/4] API + dashboard en http://${HostServidor}:${PuertoApi} ..."
$procApi = Start-Process -PassThru -NoNewWindow uvicorn -ArgumentList "api.main:app", "--host", "0.0.0.0", "--port", "$PuertoApi"
if (-not (Wait-Port -TargetHost "127.0.0.1" -Port $PuertoApi -TimeoutSeg 20)) {
    Write-Host "La API no llego a levantar. Abortando."
    Stop-Process -Id $procApi.Id, $procSocket.Id -Force -ErrorAction SilentlyContinue
    exit 1
}
Write-Host "  -> escuchando."

Write-Host "[4/4] Nueve nodos regionales..."
$procNodos = Start-Process -PassThru -NoNewWindow python -ArgumentList "scripts/lanzar_nodos.py", "--host", "$HostServidor", "--puerto", "$PuertoSocket"

Write-Host ""
Write-Host "Todo arriba. Dashboard: http://${HostServidor}:${PuertoApi}"
Write-Host "Cierra esta ventana o presiona Ctrl+C para bajar todo."
Write-Host ""
Write-Host "PARA LA DEMO EN VIVO (alta automatica, requisito 7.2):"
Write-Host "  python -m cliente.main --node-id CNS-XXX-10 --region 'Nueva Regional' --host $HostServidor --puerto $PuertoSocket"

try {
    Wait-Process -Id $procApi.Id
}
finally {
    Write-Host ""
    Write-Host "Bajando..."
    Stop-Process -Id $procNodos.Id, $procApi.Id, $procSocket.Id -Force -ErrorAction SilentlyContinue
}
