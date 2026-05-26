# 📈 TradeStream Analytics Pipeline

TradeStream Analytics Pipeline là một hệ thống **Data Lakehouse** thời gian thực hiện đại, thiết kế theo mô hình **Hot/Cold Path** kết hợp **Medallion Architecture** để thu thập, xử lý, lưu trữ và trực quan hóa dữ liệu giao dịch tài chính (Chứng khoán & Tiền điện tử) với độ trễ thấp và khả năng mở rộng cao.

---

## 🏗️ Kiến trúc Hệ thống (System Architecture)

Dự án tích hợp các công nghệ xử lý dữ liệu lớn (Big Data) và quản trị dữ liệu hồ chứa (Lakehouse Storage) để tách biệt luồng xử lý:

![TradeStream Pipeline Architecture Diagram](docs/pipeline_2d_detailed_architecture.png)

<!-- Biểu đồ Mermaid chi tiết mô tả cụ thể luồng dữ liệu của hệ thống -->

```mermaid
flowchart TD
    subgraph Data_Sources ["Data Sources"]
        Binance[Binance WebSockets]
        Yahoo[Yahoo Finance Chart API]
    end

    subgraph Ingestion_Layer ["Ingestion Layer (Kafka)"]
        subgraph Producers ["Producers"]
            CryptoProd[crypto_producer.py]
            StockProd[stock_producer.py]
        end
        Kafka[(Kafka Clusters)]
    end

    subgraph Lakehouse_Storage ["Lakehouse Storage (MinIO & Iceberg)"]
        Bronze[(Bronze Layer: MinIO JSON Raw)]
        Silver[(Silver Layer: Apache Iceberg Parquet)]
    end

    subgraph Processing_Layer ["Processing & Orchestration (Spark & Airflow)"]
        Airflow[Airflow Orchestrator & Monitor]
        SparkIngest[ingest_raw_to_bronze.py]
        SparkTransform[transform_bronze_to_silver.py]
        SparkSync[sync_silver_to_postgres.py]
        SparkStreaming[Spark Streaming Hot Path Job]
    end

    subgraph Serving_Analytics_Layer ["Serving, Query & MLOps"]
        Timescale[(TimescaleDB Serving DB)]
        Trino[Trino Query Engine]
        MLTrain[ML Training Script]
        MLflow[MLflow Model Registry]
        MLInference[ML Inference Script]
        Grafana[Grafana Dashboards]
    end

    %% Ingestion flows
    Binance -->|Websocket Combined Streams| CryptoProd
    Yahoo -->|Async Polling every 10s| StockProd
    CryptoProd -->|crypto_trades| Kafka
    StockProd -->|stock_trades| Kafka
    
    Kafka -->|Consume Ticks| SparkIngest
    SparkIngest -->|Write JSON| Bronze
    
    %% Cold Path
    Bronze -->|Read JSON Ticks| SparkTransform
    SparkTransform -->|Window Aggregations & MERGE INTO| Silver
    Silver -->|Incremental Select| SparkSync
    SparkSync -->|Upsert ON CONFLICT| Timescale
    
    %% Hot Path
    Bronze -->|Read JSON Ticks| SparkStreaming
    SparkStreaming -->|Micro-batch every 5-10s| Timescale

    %% Trino Connections
    Silver -->|lakehouse catalog| Trino
    Timescale -->|postgres catalog| Trino

    %% ML Retraining
    Trino -->|Extract Gold Features| MLTrain
    MLTrain -->|Register Model| MLflow

    %% ML Inference
    MLflow -->|Load Active Model| MLInference
    Trino -->|Read Latest Gold Features| MLInference
    MLInference -->|Run Prediction & Save| Timescale
    
    %% Grafana Serving
    Timescale -->|Query Daily, Indicators, Predictions| Grafana
    Trino -->|OLAP Analytics Views| Grafana

    %% Airflow Control Dotted Links
    Airflow -.->|Trigger every 5m| SparkIngest
    Airflow -.->|Trigger| SparkTransform
    Airflow -.->|Trigger| SparkSync
    Airflow -.->|Monitor & Control| SparkStreaming
    Airflow -.->|Schedule ML Training| MLTrain
    Airflow -.->|Schedule ML Inference| MLInference
```

### 1. Hot Path (Real-Time Ticks & Window Indicators)
*   **Mục tiêu**: Xử lý và tính toán chỉ báo động (SMA, VWAP) thời gian thực phục vụ hiển thị đồ thị live trên Grafana.
*   **Cơ chế tối ưu**: Tiêu thụ dữ liệu trực tiếp từ **Bronze Layer** (MinIO JSON Raw) thông qua Spark Streaming Job chạy theo chu kỳ **5 - 10 giây** (micro-batch) để tiết kiệm tài nguyên CPU/RAM trên môi trường local thay vì chạy continuous 24/7.
*   **Serving**: Kết quả được ghi trực tiếp vào bảng `crypto_indicators` trên TimescaleDB.
*   **Orchestration**: **Apache Airflow** chịu trách nhiệm giám sát (monitor) và kiểm soát trạng thái hoạt động của Streaming Job.

### 2. Cold Path (Micro-batch Medallion Lakehouse - Đã triển khai)
*   **Mục tiêu**: Lưu trữ lịch sử lâu dài, tính toán nến ngày OHLCV phục vụ phân tích OLAP, đồng bộ dữ liệu sạch.
*   **Orchestration**: Được điều phối bởi Airflow DAG `realtime_cold_path_pipeline` (chạy định kỳ mỗi **5 phút** một lần):
    1.  **Bronze (Raw Ingestion)**: Job Spark Structured Streaming chạy với cơ chế `.trigger(availableNow=True)` tiêu thụ ticks từ Kafka lưu vào Bronze Layer.
    2.  **Silver (Refined/OHLCV)**: Job Spark Batch đọc ticks từ Bronze, gom nhóm thành nến ngày OHLCV và tính toán chỉ báo kỹ thuật ngày (`daily_return`, `price_range`), sau đó `MERGE INTO` vào bảng **Apache Iceberg** (`fact_daily_prices`) lưu dạng Parquet trên MinIO.
    3.  **Serving (Gold Sync)**: Job Spark Batch đọc từ Silver Iceberg, thực hiện denormalize dữ liệu (JOIN với các bảng `dim_date`, `dim_assets`), và ghi đồng bộ gia tăng vào Serving Database **TimescaleDB** (`daily_prices`) bằng cơ chế `ON CONFLICT DO UPDATE` để Grafana truy vấn cực nhanh.

### 3. ML Pipeline & Inference (Dự báo học máy)
*   **Mục tiêu**: Huấn luyện và dự đoán xu hướng giá/giá đóng cửa tài sản tài chính.
*   **ML Retraining**: Định kỳ dưới sự điều phối của **Apache Airflow**, pipeline huấn luyện truy xuất các đặc trưng lịch sử chất lượng cao từ **Gold Layer** (TimescaleDB) thông qua cổng truy vấn **Trino SQL Engine** (sử dụng PostgreSQL connector). Mô hình được lưu trữ và quản lý phiên bản trên **MLflow Model Registry**.
*   **ML Inference (Dự báo)**: Airflow lập lịch chạy script suy diễn hàng ngày, tải mô hình active từ **MLflow**, đọc đặc trưng mới qua **Trino**, đưa ra dự báo và ghi kết quả vào bảng `daily_predictions` (TimescaleDB) phục vụ hiển thị biểu đồ so sánh xu hướng (Forecast vs Actual) trên Grafana.

### 4. Lưu trữ & Legacy (Phase 1)
*   **`yahoo_batch_producer.py`**: Producer cũ ở Phase 1 để kéo dữ liệu nến ngày thô trực tiếp từ Yahoo Finance API (với tham số `range=1d&interval=1d`) và đẩy vào Kafka topic `raw_daily_prices`.
*   **Trạng thái**: Đã được lưu trữ và thay thế hoàn toàn bằng luồng Ingest Ticks thô tự động từ các active producers thời gian thực ở trên. Bảng Iceberg Silver giờ đây tự tổng hợp OHLCV trực tiếp từ ticks thay vì nạp nến 1 ngày thô.


---


## 📂 Cấu trúc Thư mục Dự án

```text
TradeStream Analytics Pipeline/
├── config/
│   └── symbols.json                  # Cấu hình danh sách mã cổ phiếu và coin cần tracking
├── dags/
│   └── daily_batch.py                # Airflow DAG điều phối Cold Path micro-batch (5 phút)
├── dashboards/
│   └── grafana.json                  # Mẫu dashboard xuất bản sẵn cho Grafana
├── docs/
│   ├── progress.md                   # Nhật ký theo dõi tiến độ các pha của dự án
│   └── learning-log.md               # Nhật ký học tập & chi tiết các bài học công nghệ
├── infrastructure/
│   ├── db/
│   │   └── init.sql                  # Schema khởi tạo các bảng và Hypertable trong TimescaleDB
│   ├── spark/
│   │   ├── jars/                     # Thư mục lưu trữ các tệp JARs phụ thuộc của Spark
│   │   └── spark-defaults.conf       # Cấu hình mặc định cho Spark master/worker
│   └── trino/
│   │   └── catalog/                  # Cấu hình kết nối Trino đến Iceberg (MinIO/Postgres Catalog)
│   └── docker-compose.yml            # Tệp docker-compose điều khiển toàn bộ cluster
├── src/
│   ├── producers/
│   │   ├── crypto_producer.py        # Async Producer stream dữ liệu Binance websockets
│   │   └── stock_producer.py         # Async Producer poll dữ liệu Yahoo Finance API mỗi 10 giây
│   └── processing/
│       ├── ingest_raw_to_bronze.py   # Job Spark nạp dữ liệu từ Kafka xuống MinIO Bronze
│       ├── transform_bronze_to_silver.py # Job Spark lọc trùng, tính toán OHLCV & ghi vào Iceberg
│       └── sync_silver_to_postgres.py # Job Spark đồng bộ dữ liệu Iceberg sang Serving DB
├── requirements.txt                  # Các thư viện Python phụ thuộc của môi trường local
└── .env                              # Quản lý toàn bộ cấu hình, tài khoản hệ thống (Không commit)
```

---

## ⚙️ Cấu hình Môi trường (.env)

Tạo file `.env` tại thư mục gốc với các thông số cấu hình tham khảo sau:

```env
# KAFKA CONFIGURATION
KAFKA_BROKER_URL=localhost:9092

# BINANCE WEBSOCKET API (CRYPTO)
BINANCE_WSS_URL=wss://stream.binance.com:9443/stream

# YAHOO FINANCE REST API (STOCK)
YAHOO_FINANCE_URL=https://query1.finance.yahoo.com/v8/finance/chart

# TIMESCALEDB
TIMESCALE_PASSWORD=postgres
TIMESCALE_CONN=host=timescaledb port=5432 dbname=tradestream user=postgres password=postgres

# GRAFANA
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=tradestream

# AIRFLOW
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow
AIRFLOW_DB_CONN=postgresql+psycopg2://airflow:airflow@airflow-metadata:5432/airflow
AIRFLOW_JWT_SECRET=tradestream-jwt-secret-key-2024
AIRFLOW_SECRET_KEY=tradestream-secret-key
AIRFLOW_ADMIN_USERS=admin:airflow:Admin

# MINIO
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=minioadminpassword
MINIO_LAKEHOUSE_BUCKET=lakehouse
MINIO_ENDPOINT=http://minio:9000

# SPARK
SPARK_MASTER=spark://spark-master:7077
SPARK_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262
```

---

## 🚀 Hướng dẫn Triển khai & Vận hành Local

### Bước 1: Khởi động Hạ tầng Docker Services
Chạy lệnh sau tại thư mục gốc dự án để khởi chạy toàn bộ container (Zookeeper, Kafka, MinIO, Spark, Airflow, TimescaleDB, Trino, Grafana):

```bash
docker compose up -d
```

### Bước 2: Thiết lập Môi trường Python Local
Khởi tạo và kích hoạt virtual environment:

```bash
python -m venv venv
# On Windows
venv\Scripts\activate
# On Linux/macOS
source venv/bin/activate
```

Cài đặt các gói thư viện phụ thuộc:

```bash
pip install -r requirements.txt
```

### Bước 3: Khởi động các Producer Stream Daemons
Chạy các Producers ở các terminal độc lập (hoặc chạy ngầm):

```bash
# Stream dữ liệu Crypto thời gian thực
python src/producers/crypto_producer.py

# Poll dữ liệu giá cổ phiếu thời gian thực (10s/lần)
python src/producers/stock_producer.py
```

### Bước 4: Kiểm thử / Kích hoạt Pipelines thủ công
Nếu muốn kiểm thử nhanh tiến trình Cold Path mà không cần đợi Airflow trigger, bạn có thể gửi trực tiếp các Spark submit job lên cluster bằng Docker:

```bash
# 1. Ingest raw ticks từ Kafka sang MinIO Bronze Layer
docker exec -u root spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/user-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/opt/spark/user-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/opt/spark/user-jars/kafka-clients-3.4.1.jar,/opt/spark/user-jars/commons-pool2-2.12.0.jar,/opt/spark/user-jars/postgresql-42.6.0.jar /opt/airflow/src/processing/ingest_raw_to_bronze.py

# 2. Xử lý window aggregation ticks sang OHLCV nến ngày ghi vào Iceberg Silver Layer
docker exec -u root spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/user-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/opt/spark/user-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/opt/spark/user-jars/kafka-clients-3.4.1.jar,/opt/spark/user-jars/commons-pool2-2.12.0.jar,/opt/spark/user-jars/postgresql-42.6.0.jar /opt/airflow/src/processing/transform_bronze_to_silver.py

# 3. Đồng bộ dữ liệu sạch gia tăng từ Silver sang TimescaleDB Serving DB
docker exec -u root spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/user-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/opt/spark/user-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/opt/spark/user-jars/kafka-clients-3.4.1.jar,/opt/spark/user-jars/commons-pool2-2.12.0.jar,/opt/spark/user-jars/postgresql-42.6.0.jar /opt/airflow/src/processing/sync_silver_to_postgres.py
```

### Bước 5: Giám sát trên Web UIs
Sau khi khởi chạy hệ thống, bạn có thể truy cập các địa chỉ UI sau:
*   **Airflow Webserver (Orchestrator)**: [http://localhost:8085](http://localhost:8085) (Tài khoản: `admin` / `airflow`)
*   **Kafka UI (Broker Monitor)**: [http://localhost:8080](http://localhost:8080)
*   **MinIO Console (Object Storage)**: [http://localhost:9001](http://localhost:9001) (Tài khoản: `admin` / `minioadminpassword`)
*   **Grafana Dashboard (Visualization)**: [http://localhost:3000](http://localhost:3000) (Tài khoản: `admin` / `tradestream`)

---

## 📈 Lộ trình Phát triển tiếp theo (Next Steps)
1.  **Phase 6 (Machine Learning & MLOps)**: Triển khai Feature Engineering và huấn luyện mô hình dự báo xu hướng (XGBoost/LightGBM) được theo dõi qua MLflow.
2.  **Phase 7 (Grafana Dashboards)**: Tích hợp Datasource Trino để so sánh phân tích dữ liệu dài hạn trong Iceberg và TimescaleDB trên cùng biểu đồ.
3.  **Phase 8 (Data Quality Gates)**: Thêm các chốt chặn kiểm tra chất lượng dữ liệu bằng Great Expectations trước khi merge dữ liệu vào Silver.
