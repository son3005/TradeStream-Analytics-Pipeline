# 🔍 Đánh giá & Phân tích chi tiết Kiến trúc Pipeline TradeStream

Tài liệu này cung cấp kết quả kiểm tra toàn diện toàn bộ mã nguồn của dự án **TradeStream Analytics Pipeline**. Dưới đây là mô tả chi tiết và chính xác về cấu trúc hoạt động thực tế của hệ thống để bạn phê duyệt trước khi chúng ta tiến hành vẽ sơ đồ visual 2D cho README.md.

---

## 1. Bản đồ luồng dữ liệu thực tế (Dataflow Mapping)

Hệ thống TradeStream được chia thành hai nhánh chính: **Hot Path** (Xử lý thời gian thực - Độ trễ thấp) và **Cold Path** (Xử lý hàng loạt - Phân tích & Lưu trữ hồ dữ liệu).

### 🌌 Tóm tắt dòng chảy dữ liệu (Dataflow Diagram)

```mermaid
flowchart TD
    subgraph Data_Sources ["1. Nguồn Dữ Liệu (Producers Active)"]
        Binance["Binance WebSocket\n(crypto_producer.py)"]
        Yahoo["Yahoo Finance API\n(stock_producer.py)"]
    end

    subgraph Legacy_Sources ["Nguồn Lưu Trữ Cũ (Legacy Phase 1)"]
        YahooBatch["Yahoo Finance Batch\n(yahoo_batch_producer.py)"]
    end

    subgraph Message_Broker ["2. Hàng Đợi Tin Nhắn (Active Kafka)"]
        Kafka[{"Apache Kafka Cluster"}]
        TopicCrypto["Topic: crypto_trades\n(Ticks Crypto)"]
        TopicStock["Topic: stock_trades\n(Ticks Stock)"]
    end

    subgraph Legacy_Topics ["Topics Cũ (Legacy Phase 1)"]
        TopicBatch["Topic: raw_daily_prices\n(Daily Batch JSON)"]
    end

    subgraph Cold_Path ["3. Cold Path (Airflow & Spark Medallion)"]
        SparkIngest["Spark Ingest\n(ingest_raw_to_bronze.py)"]
        Bronze[("Bronze Layer (MinIO JSON)\ns3a://lakehouse/bronze/raw_trades")]
        SparkTrans["Spark Transform\n(transform_bronze_to_silver.py)"]
        Silver[("Silver Layer (Iceberg Parquet)\nlakehouse.trading.fact_daily_prices")]
        SparkSync["Spark Sync\n(sync_silver_to_postgres.py)"]
    end

    subgraph Serving_Analytics ["4. Serving & Analytics Layer"]
        Timescale[("TimescaleDB (Postgres)\nTable: daily_prices\nTable: crypto_indicators")]
        Trino["Trino Query Engine"]
        Grafana["Grafana Dashboards"]
    end

    %% Connections
    Binance -->|Websocket| TopicCrypto
    Yahoo -->|Polling 10s| TopicStock
    YahooBatch -.->|Lấy nến 1 ngày thô\nKhông chạy trong DAG active| TopicBatch

    TopicCrypto --> Kafka
    TopicStock --> Kafka

    %% Cold Path Flow
    Kafka -->|Spark Structured Streaming\nAvailableNow Trigger| SparkIngest
    SparkIngest -->|Write JSON| Bronze
    Bronze -->|Read JSON Ticks| SparkTrans
    SparkTrans -->|Window Aggregation\n& Technical Indicators| Silver
    Silver -->|Denormalize & JDBC Staging| SparkSync
    SparkSync -->|ON CONFLICT DO UPDATE| Timescale

    %% Catalog & Query
    Silver ---|JDBC Catalog| Timescale
    Silver -->|Query Parquet| Trino
    Timescale -->|Real-time Daily Price| Grafana
    Trino -->|OLAP Analytics Views| Grafana
```

---

## 2. Chi tiết các thành phần trong Pipeline

### A. Nguồn dữ liệu & Producers (`src/producers/`)
1. **`crypto_producer.py`**:
   - Sử dụng connection **Binance WebSockets** (`wss://stream.binance.com:9443/stream`) kết nối multi-stream song song cho các đồng tiền số cấu hình trong `symbols.json` (ví dụ: `BTCUSDT`, `ETHUSDT`).
   - Chuẩn hóa mã symbol (e.g. `BTCUSDT` -> `BTC-USD`).
   - Đẩy dữ liệu tick khớp lệnh thời gian thực (`symbol`, `price`, `quantity`, `trade_time`) trực tiếp vào Kafka topic `crypto_trades`.
2. **`stock_producer.py`**:
   - Chạy dưới dạng daemon polling bất đồng bộ, gửi HTTP requests lấy giá qua **Yahoo Finance Chart API** mỗi **10 giây** cho các mã cổ phiếu trong `symbols.json` (ví dụ: `AAPL`, `MSFT`).
   - Đẩy dữ liệu ticks nhận được vào Kafka topic `stock_trades`.
3. **`yahoo_batch_producer.py` (Lưu trữ / Legacy Phase 1)**:
   - Đây là producer thô được viết ở Phase 1 để gọi Yahoo Finance API lấy dữ liệu nến ngày thô (`range=1d&interval=1d`) của các symbol cấu hình và đẩy vào topic `raw_daily_prices`.
   - **Trạng thái hiện tại**: Không tham gia vào luồng active của Airflow DAG `realtime_cold_path_pipeline` nữa vì hệ thống đã được nâng cấp lên xử lý dữ liệu tick thời gian thực qua `stock_producer.py` và `crypto_producer.py`. Bảng `fact_daily_prices` hiện tại được tổng hợp trực tiếp từ ticks thô trong Bronze layer, chứ không qua nến thô 1 ngày nữa.


---

### B. Cold Path: Kiến trúc Medallion Lakehouse (`src/processing/`)
Điều phối bằng **Apache Airflow 3.x** thông qua TaskFlow API (`dags/daily_batch.py`), lên lịch chạy định kỳ mỗi **5 phút** một lần (`*/5 * * * *`).

#### 🧱 1. Bronze Layer (Raw Ingestion) - `ingest_raw_to_bronze.py`
- **Hoạt động**: Spark Structured Streaming job đăng ký (subscribe) đồng thời 2 topics Kafka: `crypto_trades` và `stock_trades`.
- **Cơ chế**: Sử dụng `.trigger(availableNow=True)` (Spark batch-like trigger cho streaming sources) giúp Spark xử lý toàn bộ message hiện có trong Kafka rồi tự động tắt, không duy trì container chạy 24/7 để tiết kiệm tài nguyên.
- **Kết quả**: Dữ liệu thô từ Kafka được lưu trực tiếp dưới dạng tệp JSON thô tại `s3a://lakehouse/bronze/raw_trades` trên MinIO.

#### 🥈 2. Silver Layer (Structured & Cleaned) - `transform_bronze_to_silver.py`
- **Hoạt động**: Spark Batch Job đọc dữ liệu từ Bronze layer.
- **Cơ chế xử lý**:
  - Parse chuỗi JSON thô dựa trên schema cụ thể để xử lý kiểu dữ liệu (`symbol`, `price`, `quantity`, `trade_time`).
  - Sử dụng **Spark Window Functions** phân vùng theo `symbol` và `fetch_date` để lấy giá khớp lệnh đầu tiên trong ngày làm `open_price`, giá khớp lệnh cuối cùng làm `close_price`, giá cao nhất `high_price`, thấp nhất `low_price` và tính tổng khối lượng giao dịch `volume` (OHLCV).
  - Áp dụng các Window Functions khác để tính toán 2 chỉ báo kỹ thuật cơ bản: **Daily Return** (Tỷ suất sinh lời ngày) và **Price Range** (Biến động biên độ giá trong ngày).
  - Thực hiện `INNER JOIN` với bảng chiều thời gian `lakehouse.trading.dim_date` trên MinIO để tạo liên kết Star Schema.
- **Kết quả**: Thực hiện câu lệnh SQL `MERGE INTO` (Upsert) để cập nhật hoặc thêm mới bản ghi vào bảng phân tán **Apache Iceberg** (`lakehouse.trading.fact_daily_prices`) lưu trữ dưới dạng Parquet trên MinIO (dùng PostgreSQL làm Metadata JDBC Catalog).

#### 🥇 3. Serving Layer (Gold Sync) - `sync_silver_to_postgres.py`
- **Hoạt động**: Spark Batch Job đọc dữ liệu từ tầng Silver.
- **Cơ chế**:
  - `JOIN` bảng fact `fact_daily_prices` với các bảng dimension `dim_date` và `dim_assets` để dàn phẳng dữ liệu (Denormalization) nhằm tăng tốc độ truy vấn cho Dashboards.
  - Ghi đè dữ liệu phẳng vào bảng tạm thời `daily_prices_staging` trong PostgreSQL/TimescaleDB.
  - Sử dụng JVM connection của Spark kết nối trực tiếp đến PostgreSQL thực thi câu lệnh native SQL: `INSERT INTO daily_prices (...) SELECT ... ON CONFLICT (symbol, fetch_date) DO UPDATE SET...` để cập nhật dữ liệu gia tăng.
  - `DROP` bảng tạm staging sau khi hoàn tất.
- **Kết quả**: Dữ liệu được đồng bộ vào TimescaleDB hypertable `daily_prices` sẵn sàng cho các panel trên Grafana hiển thị.

---

## 3. Làm rõ sự khác biệt giữa Hot Path và Cold Path

Hiện tại có một sự hiểu lầm nhỏ về luồng **Hot Path** trong thiết kế sơ đồ cũ:
- **Thực tế trong code**: Cả `crypto_producer.py` và `stock_producer.py` đều gửi dữ liệu thời gian thực lên Kafka. Tuy nhiên, luồng đi từ Kafka xuống Database hiện tại **chỉ chạy qua Cold Path (Airflow 5 phút/lần)**. Chưa có một Spark Streaming Job chạy liên tục 24/7 để tiêu thụ Kafka và ghi trực tiếp vào TimescaleDB nhằm giảm thiểu tài nguyên CPU/RAM trên môi trường local.
- **Thiết kế Hypertable trong Database**: Trong file `init.sql`, bảng `crypto_indicators` đã được khai báo làm Hypertable phục vụ real-time ticks/indicators (với các cột `window_start`, `window_end`, `sma`, `vwap`, `trade_count`), nhưng hiện tại chưa có script xử lý nào ghi vào bảng này.
- **Đề xuất làm rõ trên sơ đồ**:
  - **Hot Path (Conceptual/Planned)**: Dữ liệu Trades từ Kafka được tiêu thụ bởi một Streaming Job liên tục (độ trễ dưới 1s), tính toán các chỉ báo nhanh (như SMA, VWAP qua slide window) và ghi trực tiếp vào bảng `crypto_indicators` trên TimescaleDB phục vụ Real-time candle charts trên Grafana.
  - **Cold Path (Implemented)**: Tích lũy dữ liệu thô trong Kafka -> nạp Bronze -> gom nhóm OHLCV ngày & tính Daily Return tại Silver (Iceberg) -> đồng bộ Gold/Serving (`daily_prices` trên TimescaleDB) -> phục vụ Trino Analytics & Grafana.

---

## 4. Mô hình dữ liệu trong hồ (Lakehouse Data Modeling)

### Tầng Iceberg Silver (Mô hình Star Schema phân tán)
- **`dim_assets`**: Lưu thông tin danh mục tài sản (mã symbol, tên tài sản, đơn vị tiền tệ, loại tài sản).
- **`dim_date`**: Lưu trữ chiều thời gian tĩnh từ năm 2000 đến 2050 (được sinh sẵn bởi `generate_dim_date.py`).
- **`fact_daily_prices`**: Lưu trữ các chỉ số OHLCV kèm các chỉ số kỹ thuật (`daily_return`, `price_range`) liên kết qua `date_key`.

### Tầng Gold / Serving (TimescaleDB Hypertables)
- **`daily_prices`**: Bảng dữ liệu phẳng đã được denormalize từ Iceberg, phân vùng thời gian theo ngày (interval 7 ngày) qua TimescaleDB để phục vụ Grafana truy vấn nến ngày cực nhanh.
- **`crypto_indicators`**: Thiết kế cho Hot Path chứa các chỉ báo dạng micro-window (ví dụ: SMA 1 phút, VWAP 1 phút), phân vùng thời gian theo ngày (interval 1 ngày).

---

## 5. Machine Learning Pipeline (Phase 6 - Đang thực hiện)

Dựa trên cấu trúc thư mục `ml/` và file định nghĩa `.agent/ml-pipeline-learning/SKILL.md`:
- **Đầu vào**: Đọc dữ liệu lịch sử giao dịch từ Apache Iceberg thông qua cổng kết nối **Trino Query Engine**.
- **Tính toán Feature**: Feature Engineering thực hiện trực tiếp trên Trino/Spark để tạo ra các tập thuộc tính kỹ thuật như tỷ số giá so với SMA/VWAP, tỷ suất sinh lời kỳ trước (`return_1`, `return_4`, `return_24`), độ biến động (`volatility_20`), và tín hiệu RSI (`rsi_signal`).
- **Mô hình huấn luyện**:
  - **Price Direction** (Dự đoán xu hướng giá tăng/giảm trong 1 giờ tới): Sử dụng mô hình phân loại **XGBoost Classifier**.
  - **Anomaly Detection** (Phát hiện volume giao dịch bất thường): Sử dụng thuật toán học không giám sát **Isolation Forest**.
  - **Volatility Forecast** (Dự báo biên độ biến động giá): Sử dụng mô hình hồi quy **LightGBM**.
- **MLOps**: Sử dụng dịch vụ **MLflow Tracking Server** chạy ở cổng 5000 (`http://localhost:5000`) để theo dõi siêu tham số (parameters), độ chính xác (accuracy/metrics) và đăng ký phiên bản mô hình (model registry).

---

## 6. Điểm Độc đáo về mặt Kỹ thuật (Technical Highlights)

1. **Spark JDBC Catalog**: Sử dụng Postgres để quản lý metadata cho bảng Iceberg thay vì Hive Metastore phức tạp.
2. **Spark Structured Streaming Trigger.AvailableNow**: Kỹ thuật tối ưu hóa tài nguyên chạy batch-incremental trên luồng streaming.
3. **Federated Query (Trino)**: Cho phép kết hợp dữ liệu lịch sử lưu trữ dạng Parquet trên MinIO và dữ liệu cấu hình trong Postgres trong cùng một câu lệnh JOIN SQL.
4. **TimescaleDB Hypertables**: Phân vùng tự động bảng cơ sở dữ liệu quan hệ theo thời gian để tối ưu hóa hiệu năng ghi và truy vấn chuỗi thời gian.
