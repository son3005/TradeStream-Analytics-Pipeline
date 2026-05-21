# 📈 TradeStream Progress Tracker

## Tổng quan Tiến độ
- **Trạng thái**: Đang thực hiện
- **Phase hiện tại**: Chuẩn bị sang Phase 7 (Grafana Dashboard)
- **Hoàn thành**: 4/8 Phases (Phase 0, 1, 2, 5)

---

## Breakdown theo Phase

### Phase 0: Foundation & Setup 🩸 [HOÀN THÀNH 🎉]
- [x] Tạo cấu trúc thư mục dự án (project structure)
- [x] Setup Docker Compose base
- [x] Cài đặt môi trường Python venv + requirements.txt

### Phase 1: Kafka Ingestion 📨 [HOÀN THÀNH 🎉]
- [x] Xây dựng Python Async Producer (`yahoo_batch_producer.py`)
- [x] Đẩy dữ liệu thô (Raw JSON) vào Kafka topic `raw_daily_prices`
- [x] Xác nhận dữ liệu nạp thành công vào Kafka

### Phase 2: Spark Processing ⚡ [HOÀN THÀNH 🎉]
- [x] Xây dựng Spark Batch Processor (`spark_batch_processor.py`)
- [x] Spark đọc stream từ Kafka, parse JSON động
- [x] Tính toán Daily Return và Price Range
- [x] Sử dụng Java JDBC Driver nạp dữ liệu vào TimescaleDB
- [x] Giải quyết triệt để lỗi phân quyền Ivy2, Duplicate Key (bằng cơ chế Delete-then-Insert), và Driver Classloader

### Phase 5: Airflow Orchestration 🎼 [HOÀN THÀNH 🎉]
- [x] Tích hợp Airflow DAG `daily_batch_decoupled`
- [x] Lập lịch điều phối: Trigger script Ingestion (Task 1) -> Spark Submit Job (Task 2)
- [x] Tối ưu hóa thời gian chạy: giảm từ ~50s xuống ~36s (JVM overhead cố định) nhờ mount offline JARs

### Phase 7: Grafana Dashboard 📊 [HOÀN THÀNH 🎉]
- [x] Kết nối Grafana với TimescaleDB
- [x] Xây dựng Dashboard hiển thị giá, volume, và biến động (đã chuyển query sang daily_prices)
- [x] Xác nhận hiển thị dữ liệu thô và biểu đồ trực quan (AAPL, MSFT, BTC-USD, ETH-USD) từ DB thành công


### Phase 3: Lakehouse Storage (Iceberg + MinIO) ⏳ [Chưa làm]
- [ ] Setup MinIO object storage và Apache Iceberg
- [ ] Thiết kế Star Schema cho dữ liệu trading
- [ ] Viết Spark job chuyển dữ liệu sang Iceberg format

### Phase 4: Trino SQL Analytics ⏳ [Chưa làm]
- [ ] Thiết lập Trino SQL engine
- [ ] Viết analytical queries (Window functions, CTEs)
- [ ] Tạo views phục vụ báo cáo phân tích

### Phase 6: Machine Learning Pipeline ⏳ [Chưa làm]
- [ ] Feature Engineering (SMA, EMA, RSI, VWAP...) từ dữ liệu giá
- [ ] Train models XGBoost (Phân loại) & LightGBM (Hồi quy)
- [ ] Thiết lập MLflow track experiment và tích hợp dự đoán vào Airflow DAG

