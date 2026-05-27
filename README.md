# 📈 TradeStream Analytics Pipeline

TradeStream Analytics Pipeline là một hệ thống **Data Lakehouse** thời gian thực hiện đại, thiết kế theo mô hình **Hot/Cold Path** kết hợp **Medallion Architecture** để thu thập, xử lý, lưu trữ và trực quan hóa dữ liệu giao dịch tài chính (Chứng khoán & Tiền điện tử) với độ trễ thấp và khả năng mở rộng cao.

---

## 🎯 Mục tiêu Dự án (Project Goals)

Mục tiêu cốt lõi của dự án TradeStream Analytics Pipeline là xây dựng một hệ thống xử lý và phân tích dữ liệu lớn hoàn chỉnh, giải quyết triệt để các thách thức về độ trễ, lưu trữ và dự báo trong tài chính:

1. **Kiến trúc luồng kép tối ưu (Dual Hot/Cold Path)**: Phân tách rõ rệt giữa luồng xử lý thời gian thực độ trễ thấp (Hot Path: tính toán chỉ báo động phục vụ live monitor) và luồng xử lý lô tin cậy lưu trữ lâu dài (Cold Path: tổng hợp nến ngày phục vụ OLAP và huấn luyện mô hình).
2. **Quản trị hồ dữ liệu hiện đại (Medallion Data Lakehouse)**: Tổ chức dữ liệu theo kiến trúc Medallion (Bronze ➔ Silver ➔ Gold) trên nền tảng **MinIO** kết hợp định dạng bảng **Apache Iceberg**, giải quyết triệt để bài toán "nhiều file nhỏ" (Small Files Problem) và hỗ trợ các tính năng doanh nghiệp như Schema Evolution, Time Travel.
3. **Luồng máy học khép kín và tự động (End-to-End MLOps)**: Xây dựng hệ thống học máy hoàn chỉnh từ trích xuất dữ liệu tập trung qua **Trino Query Engine**, huấn luyện mô hình (XGBoost/LightGBM) và theo dõi thông số qua **MLflow Registry**, cho đến tự động dự báo giá hàng ngày thông qua **Airflow** và lưu kết quả vào Serving Database.
4. **Trực quan hóa hợp nhất (Unified Visualization)**: Tích hợp bảng điều khiển **Grafana** hiển thị đồng thời cả dữ liệu giao dịch thời gian thực (Hot Path), biểu đồ lịch sử dài hạn từ Lakehouse (Cold Path), và biểu đồ đối chiếu hiệu năng mô hình (Giá dự báo vs Giá thực tế - Forecast vs Actual).
5. **Khả năng chịu tải và tối ưu phần cứng (Hardware Constraints & Database Load)**: Tích hợp các giải pháp chống nghẽn I/O ghi trên database (TimescaleDB Hypertables, compression) và chống tràn RAM (OOM) trên Spark (RocksDB State Store, watermarking, parallelism).

---

## 🏗️ Kiến trúc Hệ thống (System Architecture)

Dự án tích hợp các công nghệ xử lý dữ liệu lớn (Big Data) và quản trị dữ liệu hồ chứa (Lakehouse Storage) để tách biệt luồng xử lý:

![TradeStream Pipeline Architecture Diagram](docs/pipeline_2d_detailed_architecture.png)



### 1. Hot Path (Real-Time Ticks & Window Indicators)
*   **Mục tiêu**: Xử lý và tính toán các chỉ báo kỹ thuật động (SMA, VWAP) thời gian thực với độ trễ cực thấp phục vụ hiển thị live trên Grafana.
*   **Cơ chế hoạt động**:
    1.  **Tiêu thụ trực tiếp**: Một Spark Structured Streaming job chạy liên tục, tiêu thụ trực tiếp luồng ticks thô từ các Kafka topics (`crypto_trades`, `stock_trades`) vào RAM để đảm bảo độ trễ thấp nhất.
    2.  **Tính toán Sliding Window**: Sử dụng cơ chế Watermarking (ví dụ: `1 minute` để xử lý dữ liệu trễ) kết hợp với Sliding Windows (ví dụ: window kích thước 1 phút, trượt mỗi 10 giây) để tính toán SMA, VWAP trực tiếp trên RAM.
    3.  **Ghi Bulk Upsert xuống Serving DB**: Gom dữ liệu sau mỗi micro-batch (ví dụ: trigger 5 giây) và ghi trực tiếp vào bảng `realtime_indicators` trên **TimescaleDB** bằng JDBC bulk insert, giúp tránh quá tải ghi đĩa.
    4.  **Live Visualization**: Grafana kết nối trực tiếp TimescaleDB hypertable, quét dữ liệu với tần suất refresh 2s-5s để vẽ đồ thị live không độ trễ.
*   **Orchestration**: Apache Airflow chịu trách nhiệm giám sát (monitor) và kiểm soát trạng thái của Streaming Job.

### 2. Cold Path (Micro-batch Medallion Lakehouse - Đã triển khai)
*   **Mục tiêu**: Lưu trữ lịch sử lâu dài, tính toán nến ngày OHLCV phục vụ phân tích OLAP, đồng bộ dữ liệu sạch.
*   **Orchestration**: Được điều phối bởi Airflow DAG `realtime_cold_path_pipeline` (chạy định kỳ mỗi **5 phút** một lần):
    1.  **Bronze (Raw Ingestion)**: Job Spark Structured Streaming [ingest_raw_to_bronze.py](file:///E:/DuAn/TradeStream%20Analytics%20Pipeline/src/processing/tradestream/ingest_raw_to_bronze.py) chạy với cơ chế `.trigger(availableNow=True)` tiêu thụ ticks từ Kafka lưu vào Bronze Layer dạng tệp JSON thô trên MinIO.
    2.  **Silver (Refined/OHLCV)**: Job Spark Batch [transform_bronze_to_silver.py](file:///E:/DuAn/TradeStream%20Analytics%20Pipeline/src/processing/tradestream/transform_bronze_to_silver.py) đọc ticks từ Bronze, lọc trùng, gom nhóm thành nến ngày OHLCV và tính toán chỉ báo kỹ thuật ngày (`daily_return`, `price_range`), sau đó thực hiện `MERGE INTO` vào bảng **Apache Iceberg** (`fact_daily_prices`) lưu dạng Parquet trên MinIO.
    3.  **Serving (Gold Sync)**: Job Spark Batch [sync_silver_to_postgres.py](file:///E:/DuAn/TradeStream%20Analytics%20Pipeline/src/processing/tradestream/sync_silver_to_postgres.py) đọc từ Silver Iceberg, JOIN với các bảng `dim_date`, `dim_assets` để dàn phẳng dữ liệu (denormalization), và ghi đồng bộ gia tăng vào Serving Database **TimescaleDB** (`daily_prices`) bằng cơ chế `ON CONFLICT DO UPDATE` để Grafana truy vấn cực nhanh.

### 3. ML Pipeline & Inference (Huấn luyện & Dự đoán học máy - ML Path)
*   **Mục tiêu**: Huấn luyện và dự đoán xu hướng giá/giá đóng cửa tài sản tài chính ngày tiếp theo.
*   **Quy trình Huấn luyện (ML Training Flow)**:
    1.  **Trích xuất**: Định kỳ (ví dụ: hàng tuần) dưới sự điều phối của Airflow, script `ml/training/train.py` truy xuất dữ liệu lịch sử chất lượng cao từ Silver Iceberg thông qua cổng truy vấn **Trino SQL Engine**.
    2.  **Feature Engineering**: Chạy qua module dùng chung `ml/features/build_features.py` để tạo các lag features (t-1 đến t-7) và các chỉ số biến động.
    3.  **MLflow Registration**: Huấn luyện mô hình (XGBoost/LightGBM) dự báo giá đóng cửa tiếp theo, ghi log tự động các tham số và độ đo đánh giá (RMSE, MAE) lên **MLflow Server**, và lưu mô hình tốt nhất vào Registry dưới nhãn `Production`.
*   **Quy trình Dự đoán (ML Prediction/Inference Flow)**:
    1.  **Kích hoạt**: Hàng ngày, Airflow DAG gọi script `ml/serving/predict.py`.
    2.  **Load model**: Script tải phiên bản mô hình active có tag `Production` từ MLflow Model Registry.
    3.  **Dự báo**: Truy vấn Trino để lấy các đặc trưng của 7 ngày gần nhất, thực hiện dự báo giá đóng cửa ngày tiếp theo.
    4.  **Serving**: Lưu trữ kết quả dự báo vào bảng `daily_predictions` (TimescaleDB hypertable).
    5.  **Visualization**: Hiển thị biểu đồ đối chiếu giá dự báo với giá thực tế (Forecast vs Actual) trực tiếp trên Grafana Dashboard để giám sát chất lượng mô hình.

### 4. Lưu trữ & Legacy (Phase 1)
*   **`yahoo_batch_producer.py`**: Producer cũ ở Phase 1 để kéo dữ liệu nến ngày thô trực tiếp từ Yahoo Finance API (với tham số `range=1d&interval=1d`) và đẩy vào Kafka topic `raw_daily_prices`.
*   **Trạng thái**: Đã được lưu trữ trong thư mục `tools/` và thay thế hoàn toàn bằng luồng Ingest Ticks thô tự động từ các active producers thời gian thực ở trên. Bảng Iceberg Silver giờ đây tự tổng hợp OHLCV trực tiếp từ ticks thay vì nạp nến 1 ngày thô.


---


## 📂 Cấu trúc Thư mục Dự án

```text
TradeStream Analytics Pipeline/
├── config/
│   └── symbols.json                  # Cấu hình danh sách mã cổ phiếu và coin cần tracking
├── dags/
│   ├── tradestream/                  # Thư mục chứa các DAGs vận hành chính thức
│   │   ├── daily_batch.py            # Airflow DAG điều phối Cold Path (5 phút)
│   │   ├── data_quality.py           # Airflow DAG kiểm định chất lượng dữ liệu
│   │   └── backfill.py               # Airflow DAG backfill dữ liệu lịch sử
│   └── examples/                     # Thư mục chứa các DAGs ví dụ / học tập
│       └── hello_dag.py              # DAG demo hello world mẫu
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
│       └── catalog/                  # Cấu hình kết nối Trino đến Iceberg
│   └── docker-compose.yml            # Tệp docker-compose điều khiển toàn bộ cluster
├── src/
│   ├── producers/
│   │   ├── crypto_producer.py        # Async Producer stream dữ liệu Binance websockets
│   │   └── stock_producer.py         # Async Producer poll dữ liệu Yahoo Finance API mỗi 10 giây
│   ├── processing/
│   │   └── tradestream/              # Spark jobs của luồng TradeStream chính thức
│   │       ├── ingest_raw_to_bronze.py      # Kafka -> MinIO Bronze (JSON raw)
│   │       ├── transform_bronze_to_silver.py # Bronze -> Silver Iceberg (OHLCV nến ngày)
│   │       └── sync_silver_to_postgres.py    # Silver Iceberg -> Serving DB
│   ├── management/                   # Các script khởi tạo và bảo trì định kỳ
│   │   ├── create_star_schema.py     # Setup Iceberg tables & nạp dữ liệu dim ban đầu
│   │   ├── generate_dim_date.py      # Sinh dữ liệu dim_date CSV
│   │   ├── maintain_lakehouse.py     # Bảo trì Iceberg compaction, expire snapshots
│   │   └── dim_date.csv              # Dữ liệu tĩnh dim_date phục vụ load ban đầu
│   └── utils/
│       └── spark_helper.py           # Khởi tạo Spark Session & DB credentials
├── tools/                            # Các công cụ kiểm thử kết nối, thí nghiệm và legacy
│   ├── test_kafka_producer.py        # Script test gửi tin nhắn nhanh tới Kafka
│   ├── test_minio_connection.py      # Script test kết nối MinIO
│   ├── verify_iceberg_features.py    # Script demo xác minh tính năng Iceberg
│   └── yahoo_batch_producer.py       # Script legacy Phase 1 batch producer
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
docker exec -u root spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/user-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/opt/spark/user-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/opt/spark/user-jars/kafka-clients-3.4.1.jar,/opt/spark/user-jars/commons-pool2-2.12.0.jar,/opt/spark/user-jars/postgresql-42.6.0.jar /opt/airflow/src/processing/tradestream/ingest_raw_to_bronze.py

# 2. Xử lý window aggregation ticks sang OHLCV nến ngày ghi vào Iceberg Silver Layer
docker exec -u root spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/user-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/opt/spark/user-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/opt/spark/user-jars/kafka-clients-3.4.1.jar,/opt/spark/user-jars/commons-pool2-2.12.0.jar,/opt/spark/user-jars/postgresql-42.6.0.jar /opt/airflow/src/processing/tradestream/transform_bronze_to_silver.py

# 3. Đồng bộ dữ liệu sạch gia tăng từ Silver sang TimescaleDB Serving DB
docker exec -u root spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/user-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/opt/spark/user-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/opt/spark/user-jars/kafka-clients-3.4.1.jar,/opt/spark/user-jars/commons-pool2-2.12.0.jar,/opt/spark/user-jars/postgresql-42.6.0.jar /opt/airflow/src/processing/tradestream/sync_silver_to_postgres.py
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
