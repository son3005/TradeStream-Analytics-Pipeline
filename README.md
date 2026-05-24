# 📈 TradeStream Analytics Pipeline

TradeStream Analytics Pipeline là một hệ thống **Data Lakehouse** thời gian thực hiện đại, thiết kế theo mô hình **Hot/Cold Path** kết hợp **Medallion Architecture** để thu thập, xử lý, lưu trữ và trực quan hóa dữ liệu giao dịch tài chính (Chứng khoán & Tiền điện tử) với độ trễ thấp và khả năng mở rộng cao.

---

## 🏗️ Kiến trúc Hệ thống (System Architecture)

Dự án tích hợp các công nghệ xử lý dữ liệu lớn (Big Data) và quản trị dữ liệu hồ chứa (Lakehouse Storage) để tách biệt luồng xử lý:

```mermaid
flowchart TD
    subgraph Data Sources
        Binance[Binance WebSockets]
        Yahoo[Yahoo Finance Chart API]
    end

    subgraph Ingestion Layer
        subgraph Producers
            CryptoProd[crypto_producer.py]
            StockProd[stock_producer.py]
        end
        Kafka[(Kafka Clusters)]
    end

    subgraph Processing Layer (Spark & Airflow)
        Airflow[Airflow TaskFlow API Scheduler]
        SparkIngest[ingest_raw_to_bronze.py]
        SparkTrans[transform_bronze_to_silver.py]
        SparkSync[sync_silver_to_postgres.py]
    end

    subgraph Lakehouse Storage (MinIO & Apache Iceberg)
        Bronze[(Bronze Layer: MinIO JSON Raw)]
        Silver[(Silver Layer: Apache Iceberg Parquet)]
    end

    subgraph Serving & Analytics Layer
        Timescale[(TimescaleDB Serving DB)]
        Trino[Trino Query Engine]
        Grafana[Grafana Dashboards]
    end

    %% Flows
    Binance -->|Websocket Combined Streams| CryptoProd
    Yahoo -->|Async Polling every 10s| StockProd
    CryptoProd -->|crypto_trades| Kafka
    StockProd -->|stock_trades| Kafka
    
    Airflow -->|Trigger Micro-batch every 5m| SparkIngest
    Airflow -->|Trigger| SparkTrans
    Airflow -->|Trigger| SparkSync

    Kafka -->|Consume Ticks| SparkIngest
    SparkIngest -->|Write JSON| Bronze
    Bronze -->|Read JSON Ticks| SparkTrans
    SparkTrans -->|Window Aggregations & MERGE INTO| Silver
    Silver -->|Incremental Select| SparkSync
    SparkSync -->|Upsert ON CONFLICT| Timescale

    Silver ---|Metadata JDBC Catalog| Timescale
    Silver -->|Parquet Files| Trino
    Timescale -->|Real-time Ticker| Grafana
    Trino -->|OLAP Analytics Views| Grafana
```

### 1. Hot Path (Real-Time Stream)
*   **Mục tiêu**: Phục vụ biểu đồ nến thời gian thực cực nhanh với độ trễ dưới 1 giây.
*   **Luồng hoạt động**: Dữ liệu khớp lệnh (Trades) từ các sàn Binance và Yahoo Finance API được Producer đẩy trực tiếp vào các topic Kafka tương ứng (`crypto_trades`, `stock_trades`). 

### 2. Cold Path (Micro-batch Medallion Lakehouse)
*   **Mục tiêu**: Lưu trữ lâu dài, dọn dẹp dữ liệu bẩn, chạy tính toán phân tích chỉ báo và huấn luyện mô hình Machine Learning.
*   **Orchestration (Airflow)**: Một quy trình tự động được điều phối bởi Airflow (TaskFlow API) chạy định kỳ mỗi **5 phút** thực hiện 3 bước kế tiếp:
    1.  **Bronze (Raw Ingestion)**: Tiêu thụ dữ liệu thô từ các topics Kafka thông qua PySpark Structured Streaming và lưu dưới dạng tệp JSON thô kèm siêu dữ liệu tại bucket `lakehouse/bronze/raw_trades` của **MinIO**.
    2.  **Silver (Structured/Cleaned)**: Đọc luồng JSON từ Bronze, parse cấu trúc, loại bỏ bản ghi hỏng (đẩy lỗi vào DLQ), áp dụng **Spark Window Functions** để gom nhóm ticks giao dịch thành chỉ số nến ngày **OHLCV** (Open, High, Low, Close, Volume) và MERGE INTO (Upsert) vào bảng phân tán **Apache Iceberg** sử dụng PostgreSQL làm JDBC Catalog.
    3.  **Serving Layer Sync**: Thực hiện đồng bộ gia tăng từ bảng Iceberg Silver sang Serving Database **TimescaleDB** bằng cơ chế `ON CONFLICT DO UPDATE` (Upsert) để phục vụ Dashboards nhanh chóng.

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
