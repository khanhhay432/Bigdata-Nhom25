# run.ps1 - Khoi dong toan bo pipeline
# Chay: powershell -ExecutionPolicy Bypass -File run.ps1

$env:PYSPARK_PYTHON        = "C:\Python311\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "C:\Python311\python.exe"
$env:PYTHONIOENCODING      = "utf-8"
$env:PATH = ($env:PATH -split ";" | Where-Object { $_ -notmatch "WindowsApps" }) -join ";"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Fraud Detection Pipeline - Startup" -ForegroundColor Cyan  
Write-Host "============================================" -ForegroundColor Cyan

# Producer chay trong terminal rieng
Write-Host "
[1] Mo terminal Producer..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", 
    "cd d:\BigData; $env:PYTHONIOENCODING='utf-8'; Write-Host '=== PRODUCER (Kafka) ===' -ForegroundColor Yellow; & 'C:\Python311\python.exe' producer.py"

Start-Sleep -Seconds 2

# Spark Streaming chay trong terminal chinh
Write-Host "[2] Khoi dong Spark Streaming..." -ForegroundColor Green
Write-Host "    (Cho ~30 giay Spark khoi dong, roi thay batch output)" -ForegroundColor Gray
Write-Host ""

Remove-Item "d:\BigData\checkpoint" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item "d:\BigData\tmp" -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory "d:\BigData\checkpoint","d:\BigData\tmp" -Force | Out-Null

& "C:\Python311\python.exe" main.py 2>&1
