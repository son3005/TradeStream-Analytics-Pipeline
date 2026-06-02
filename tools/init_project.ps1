# =====================================================================
# FILE: init_project.ps1
# MUC DICH: Script khoi tao du an TradeStream Analytics Pipeline tren Windows.
#           Ho tro tu dong don dep du lieu cu, khoi dong Docker va tao Star Schema.
# =====================================================================

param (
    [switch]$Wipe     # Neu bat, se xoa sach toan bo volumes va du lieu cu
)

# Thiet lap console encoding de tranh loi hien thi
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host ">>> TRADESTREAM ANALYTICS PIPELINE - KICH BAN KHOI TAO HE THONG <<<" -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan

# 1. Don dep du lieu cu neu nguoi dung yeu cau
if ($Wipe) {
    Write-Host "[!] Dang dung cac container va XOA SACH du lieu (Docker Volumes)..." -ForegroundColor Red
    docker compose --profile core --profile processing --profile storage --profile query --profile orchestration --profile dashboard down -v
    Write-Host "[OK] Da don dep xong du lieu cu." -ForegroundColor Green
} else {
    Write-Host "[*] Dang dung cac container dang chay (giu lai du lieu)..." -ForegroundColor Yellow
    docker compose --profile core --profile processing --profile storage --profile query --profile orchestration --profile dashboard down
}

# 2. Khoi dong toan bo cum dich vu Docker
Write-Host "[*] Dang khoi dong toan bo dich vu Docker..." -ForegroundColor Yellow
docker compose --profile core --profile processing --profile storage --profile query --profile orchestration --profile dashboard up -d

# 3. Doi cac container chinh dat trang thai Healthy
Write-Host "[*] Dang cho cac dich vu khoi dong va san sang hoat dong..." -ForegroundColor Yellow

$services = @("timescaledb", "minio", "kafka")
foreach ($service in $services) {
    Write-Host "[*] Dang kiem tra trang thai dich vu: $service..." -ForegroundColor Gray
    
    # Doi toi da 60 giay cho moi service
    $timeout = 30
    $count = 0
    $isHealthy = $false
    
    while ($count -lt $timeout) {
        $status = docker inspect --format '{{if .State.Running}}{{if .State.Health}}{{.State.Health.Status}}{{else}}running{{end}}{{else}}stopped{{end}}' $service 2>$null
        
        if ($status -eq "healthy" -or $status -eq "running") {
            $isHealthy = $true
            break
        }
        
        Start-Sleep -Seconds 2
        $count++
    }
    
    if ($isHealthy) {
        Write-Host "[OK] Dich vu $service da san sang!" -ForegroundColor Green
    } else {
        Write-Host "[ERROR] Dich vu $service khong the khoi dong dung han. Vui long kiem tra docker log." -ForegroundColor Red
        exit 1
    }
}

# Doi them 3s de Spark Master va Airflow on dinh
Start-Sleep -Seconds 3

# 4. Tao Star Schema trong Lakehouse Iceberg
Write-Host "[*] Dang thuc thi Spark Job khoi tao Star Schema va nap du lieu Dim..." -ForegroundColor Yellow

$JARS = "/opt/spark/user-jars/postgresql-42.6.0.jar"
docker exec -u root -e PYTHONPATH=/opt/airflow spark-master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --jars $JARS `
    /opt/airflow/src/management/create_star_schema.py

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Da tao Star Schema va nap du lieu dim_date, dim_assets thanh cong!" -ForegroundColor Green
} else {
    Write-Host "[ERROR] Tao Star Schema that bai. Vui long kiem tra log loi cua Spark." -ForegroundColor Red
    exit 1
}

Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host ">>> HE THONG TRADESTREAM DA SAN SANG! <<<" -ForegroundColor Green
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "-> Airflow Webserver: http://localhost:8085 (admin / airflow)" -ForegroundColor White
Write-Host "-> Kafka UI:         http://localhost:8080" -ForegroundColor White
Write-Host "-> MinIO Console:    http://localhost:9001 (admin / minioadminpassword)" -ForegroundColor White
Write-Host "-> Grafana Dashboard:http://localhost:3000 (admin / tradestream)" -ForegroundColor White
Write-Host "=====================================================================" -ForegroundColor Cyan
