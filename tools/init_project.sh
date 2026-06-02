#!/usr/bin/env bash
# =====================================================================
# FILE: init_project.sh
# MỤC ĐÍCH: Script khởi tạo dự án TradeStream Analytics Pipeline trên Linux/macOS/Git Bash.
#           Hỗ trợ tự động dọn dẹp dữ liệu cũ, khởi động Docker và tạo Star Schema.
# =====================================================================

WIPE=false

# Phân tích tham số truyền vào
for arg in "$@"; do
  case $arg in
    -w|--wipe)
      WIPE=true
      shift
      ;;
  esac
done

# Màu sắc hiển thị
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}=====================================================================${NC}"
echo -e "${CYAN}🚀 TRADESTREAM ANALYTICS PIPELINE - KỊCH BẢN KHỞI TẠO HỆ THỐNG${NC}"
echo -e "${CYAN}=====================================================================${NC}"

# 1. Dọn dẹp dữ liệu cũ nếu yêu cầu
if [ "$WIPE" = true ]; then
    echo -e "${RED}[!] Đang dừng các container và XÓA SẠCH dữ liệu (Docker Volumes)...${NC}"
    docker compose --profile core --profile processing --profile storage --profile query --profile orchestration --profile dashboard down -v
    echo -e "${GREEN}[✓] Đã dọn dẹp xong dữ liệu cũ.${NC}"
else
    echo -e "${YELLOW}[*] Đang dừng các container đang chạy (giữ lại dữ liệu)...${NC}"
    docker compose --profile core --profile processing --profile storage --profile query --profile orchestration --profile dashboard down
fi

# 2. Khởi động toàn bộ cụm dịch vụ Docker
echo -e "${YELLOW}[*] Đang khởi động toàn bộ dịch vụ Docker...${NC}"
docker compose --profile core --profile processing --profile storage --profile query --profile orchestration --profile dashboard up -d

# 3. Đợi các container chính đạt trạng thái Healthy
echo -e "${YELLOW}[*] Đang chờ các dịch vụ khởi động và sẵn sàng hoạt động...${NC}"

services=("timescaledb" "minio" "kafka")
for service in "${services[@]}"; do
    echo -e "${NC}[*] Đang kiểm tra trạng thái dịch vụ: $service...${NC}"
    
    timeout=30
    count=0
    is_healthy=false
    
    while [ $count -lt $timeout ]; do
        status=$(docker inspect --format '{{if .State.Running}}{{if .State.Health}}{{.State.Health.Status}}{{else}}running{{end}}{{else}}stopped{{end}}' "$service" 2>/dev/null)
        
        if [ "$status" = "healthy" ] || [ "$status" = "running" ]; then
            is_healthy=true
            break
        fi
        
        sleep 2
        count=$((count+1))
    done
    
    if [ "$is_healthy" = true ]; then
        echo -e "${GREEN}[✓] Dịch vụ $service đã sẵn sàng!${NC}"
    else
        echo -e "${RED}[X] Dịch vụ $service không thể khởi động đúng hạn. Vui lòng kiểm tra logs.${NC}"
        exit 1
    fi
done

# Đợi thêm 3s để Spark Master và Airflow ổn định
sleep 3

# 4. Tạo Star Schema trong Lakehouse Iceberg
echo -e "${YELLOW}[*] Đang thực thi Spark Job khởi tạo Star Schema và nạp dữ liệu Dim...${NC}"

JARS="/opt/spark/user-jars/postgresql-42.6.0.jar"
docker exec -u root -e PYTHONPATH=/opt/airflow spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars $JARS \
    /opt/airflow/src/management/create_star_schema.py

if [ $? -eq 0 ]; then
    echo -e "${GREEN}[✓] Đã tạo Star Schema và nạp dữ liệu dim_date, dim_assets thành công!${NC}"
else
    echo -e "${RED}[X] Tạo Star Schema thất bại. Vui lòng kiểm tra log lỗi của Spark.${NC}"
    exit 1
fi

echo -e "${CYAN}=====================================================================${NC}"
echo -e "${GREEN}🎉 HỆ THỐNG TRADESTREAM ĐÃ SẴN SÀNG!${NC}"
echo -e "${CYAN}=====================================================================${NC}"
echo -e "👉 Airflow Webserver: http://localhost:8085 (admin / airflow)"
echo -e "👉 Kafka UI:         http://localhost:8080"
echo -e "👉 MinIO Console:    http://localhost:9001 (admin / minioadminpassword)"
echo -e "👉 Grafana Dashboard:http://localhost:3000 (admin / tradestream)"
echo -e "${CYAN}=====================================================================${NC}"
