# 📈 TradeStream Analytics Pipeline - Progress Tracker

## 📊 Tổng quan Lộ trình thực hiện

Hệ thống TradeStream Analytics Pipeline được tổ chức phát triển xoay quanh 3 luồng xử lý dữ liệu chính (Cold Path, Hot Path, ML Path) trên nền tảng Ingestion chung.

*   **Luồng Chung (Shared Ingestion)**:
    *   **Phase 0**: Foundation & Setup 🩸 `[HOÀN THÀNH]`
    *   **Phase 1**: Kafka Ingestion & Producer Resilience 📨 `[HOÀN THÀNH]`
*   **Luồng COLD PATH (Medallion Lakehouse Batch)**:
    *   **Phase 2**: Spark Processing & Medallion Architecture ⚡ `[HOÀN THÀNH]`
    *   **Phase 3**: Lakehouse Storage (MinIO + Apache Iceberg) 📁 `[HOÀN THÀNH]`
    *   **Phase 4**: Query Engine & Trino SQL Analytics 🔍 `[HOÀN THÀNH]`
    *   **Phase 5**: Airflow Orchestration & Modern Workflow 🎼 `[HOÀN THÀNH]`
*   **Luồng HOT PATH (Real-Time Spark Streaming & UI)**:
    *   **Phase 7**: Grafana Dashboard & Real-Time Hot Path 📊 `[ĐANG THỰC HIỆN]`
*   **Luồng ML PATH (MLOps & Predictions)**:
    *   **Phase 6**: Machine Learning Pipeline & MLOps ⏳ `[ĐANG THỰC HIỆN]`
*   **Vận hành & Chất lượng (Production Hardening)**:
    *   **Phase 8**: Data Quality, Lineage & Alerting ⏳ `[CHƯA THỰC HIỆN]`
    *   **Phase 9**: CI/CD & Automated Testing ⏳ `[CHƯA THỰC HIỆN]`
    *   **Phase 10**: [ĐÃ BỎ QUA] Infrastructure as Code (IaC) & Kubernetes ❌

---

## 🗺️ Lộ trình chi tiết từng Phase (Đã tinh giản & tối ưu hóa)

### Phase 0: Foundation & Setup 🩸 [HOÀN THÀNH 🎉]
*   [x] Khởi tạo cấu trúc thư mục dự án (Infrastructure, Code, Docs).
*   [x] Thiết lập Docker Compose cơ bản cho dịch vụ nền tảng.
*   [x] Tạo môi trường Python ảo (venv) và cài đặt dependencies (`requirements.txt`).
*   [x] Tìm hiểu & lưu nhật ký kiến trúc: REST API vs WebSocket, Metaphor về Docker, và 4 nguyên tắc viết Dockerfile tối ưu.
* ### Phase 1: Kafka Ingestion & Producer Resilience 📨 [HOÀN THÀNH 🎉]
*   [x] Xây dựng Python Async Producer (`yahoo_batch_producer.py`) lấy giá nến ngày thô (Legacy Phase 1 - Đã lưu trữ).
*   [x] Tạo Kafka Topic `raw_daily_prices` trên Kafka Cluster Docker (Legacy Phase 1 - Đã lưu trữ).
*   [x] Đẩy dữ liệu thô (Raw JSON) thành công vào Kafka (Legacy Phase 1 - Đã lưu trữ).
*   [x] Tìm hiểu & lưu nhật ký: Cơ chế Partition & Offset, Replication, Zookeeper và giám sát Consumer Lag trên Kafka-UI.
*   [x] **Lý thuyết nâng cao**: Tìm hiểu bản chất **Change Data Capture (CDC)**, cơ chế Log-based CDC của **Debezium** và quy trình tích hợp với Kafka.
*   [x] **Tuning & Management**: Cấu hình tối ưu Producer (`acks=all` để bảo toàn dữ liệu, `linger.ms=5` và `batch.size=16384` kết hợp nén dữ liệu `snappy` để giảm tải I/O mạng).
*   [x] **Tuning & Management**: Cấu hình Spark Consumer sử dụng Manual Commit (`kafka.enable.auto.commit=false`) kết hợp với `failOnDataLoss=false` để đảm bảo xử lý chính xác (At-least-once).
*   [x] **Resilience & Retry Mechanism**: Bổ dung cơ chế retry với exponential backoff (1s, 2s, 4s...) và rate-limiting tối đa 100 API request đồng thời trong Python Producer.
*   [x] **Nghiên cứu Schema Registry**: Tìm hiểu và lưu tài liệu nghiên cứu về **Schema Registry** (Avro/Protobuf) để quản lý Schema tiến hóa và bảo toàn dữ liệu nhị phân siêu nhẹ (Thay thế container thực tế bằng tài liệu nghiên cứu lý thuyết để tối ưu tài nguyên RAM/CPU cho máy cá nhân).
*   [x] **Nâng cấp Real-Time (Active Ingest)**: Phát triển `crypto_producer.py` để stream song song nhiều coin (`BTC-USD`, `ETH-USD`) từ Binance websocket với cơ chế chuẩn hóa Symbol vào Kafka topic `crypto_trades`.
*   [x] **Nâng cấp Real-Time (Active Ingest)**: Phát triển `stock_producer.py` hoạt động dưới dạng async daemon polling Yahoo Finance API mỗi 10 giây để gửi dữ liệu ticks vào Kafka topic `stock_trades`.

### Phase 2 [COLD PATH]: Spark Processing & Medallion Architecture ⚡ [HOÀN THÀNH 🎉]
*   [x] Viết script PySpark Batch Processor (`spark_batch_processor.py`) đọc dữ liệu thô từ Kafka (Legacy Phase 2 - Đã refactor thành Medallion Pipeline).
*   [x] Parse JSON động theo schema định nghĩa sẵn (Schema Enforcement).
*   [x] **Lý thuyết nâng cao**: Tìm hiểu sự khác biệt và cơ chế của **Spark RDD vs DataFrame & Dataset**, vai trò của **Catalyst Optimizer** và **Tungsten Engine**.
*   [x] **Refactor Logic tính toán (Window Functions)**: Loại bỏ hoàn toàn vòng lặp Python / Python UDFs không tối ưu. Viết lại logic tính toán kỹ thuật (Daily Return, Price Range) sử dụng **Spark SQL Window Functions** kết hợp phân vùng (`partitionBy("symbol").orderBy("fetch_date")`) để đảm bảo không bị tính toán lệch chéo dữ liệu giữa các mã tài sản (Đã triển khai trong `transform_bronze_to_silver.py`).
*   [x] **Thay đổi quy trình Dataflow (Medallion)**: Hủy bỏ việc Spark ghi JDBC trực tiếp xuống PostgreSQL/TimescaleDB ở bước thô. Chuyển sang ghi dữ liệu thô dạng JSON/Parquet lên MinIO (Bronze Layer) (Kafka ➔ Bronze ➔ Silver).
*   [x] **Xác lập chế độ chạy (Spark Batch Mode)**: Chạy Spark dưới dạng Batch Job được trigger bởi Airflow định kỳ để tiết kiệm tài nguyên RAM/CPU thay vì chạy Streaming 24/7.
*   [x] **Xử lý Schema Drift & DLQ**: Thiết lập cơ chế bắt lỗi khi cấu trúc dữ liệu đầu vào thay đổi đột ngột (Schema Drift) và đẩy các bản ghi lỗi vào Dead Letter Queue (DLQ) trên MinIO (`s3a://lakehouse/dlq/malformed_daily_prices`).
*   [x] **Tuning & Management**: Cấu hình `spark.sql.shuffle.partitions=4` (thay vì mặc định 200) ở tất cả các script xử lý của Spark để tối ưu hiệu năng tính toán trên lượng dữ liệu nhỏ, loại bỏ empty tasks.
*   [x] **Nâng cấp Enterprise**: Xử lý dữ liệu đến trễ hoặc dữ liệu bị xáo trộn thứ tự (Late & Out-of-order Data) sử dụng cơ chế Deduplication (`dropDuplicates`) kết hợp với lệnh SQL `MERGE INTO` (Upsert) của Iceberg.
*   [x] **Nâng cấp Medallion (Active Process)**: Refactor `ingest_raw_to_bronze.py` để tiêu thụ dữ liệu từ các active topics `crypto_trades` và `stock_trades` và ghi dạng JSON ticks thô xuống Bronze Layer.
*   [x] **Nâng cấp Medallion (Active Process)**: Refactor `transform_bronze_to_silver.py` để tính toán Daily OHLCV từ luồng JSON ticks trong Bronze layer và upsert vào Iceberg Silver layer.

### Phase 3 [COLD PATH]: Lakehouse Storage (MinIO + Apache Iceberg) 📁 [HOÀN THÀNH 🎉]
*   [x] Cấu hình và chuẩn bị khởi động Object Storage **MinIO** và **mc (MinIO Client)** trên Docker.
*   [x] Thiết lập **Apache Iceberg Catalog** (Đã viết script `test_spark_iceberg.py` để chạy thử nghiệm JDBC Catalog Postgres + MinIO).
*   [x] Thiết kế mô hình dữ liệu **Star Schema** chuẩn cho dữ liệu trading (Thay thế bảng phẳng hiện tại):
    *   `dim_assets` (Dimension bảng tài sản).
    *   `dim_date` (Dimension thời gian).
    *   `fact_daily_prices` (Fact bảng giá).
*   [x] **Khởi tạo Dim Date tĩnh**: Viết script SQL/Python pre-populate dữ liệu bảng `dim_date` từ năm 2000 đến 2050 để tối ưu hóa truy vấn JOIN và đồng nhất báo cáo.
*   [x] Cấu hình PySpark ghi dữ liệu thô vào tầng **Bronze** (MinIO JSON/Parquet).
*   [x] Viết Spark Job biến đổi dữ liệu từ Bronze sang **Silver** (lọc trùng, validate chất lượng dữ liệu, áp dụng Window Functions tính toán chỉ báo kỹ thuật, mô hình hóa Star Schema) và ghi đè vào Iceberg tables.
*   [x] Thiết lập quy trình đồng bộ dữ liệu gia tăng (Incremental Sync) từ tầng Gold/Silver lên Serving DB (Postgres/TimescaleDB) một cách an toàn và gọn nhẹ.
*   [x] **Bảo trì hồ dữ liệu**: Viết các job thực thi bảo trì Iceberg tables bao gồm **Compaction (Optimize)** để gộp file nhỏ, **Expire Snapshots** giải phóng bộ nhớ, và **Delete Orphan Files** dọn dẹp file rác.
*   [x] **Nâng cấp Enterprise**: Thực hành và kiểm chứng các tính năng độc quyền của Apache Iceberg: **Time Travel** (Truy vấn ngược lịch sử bảng), **Schema Evolution** (tiến hóa cấu trúc bảng), và **Partition Evolution** (Tiến hóa phân vùng).

### Phase 4 [COLD PATH]: Query Engine & Trino SQL Analytics 🔍 [HOÀN THÀNH 🎉]
*   [x] Thiết lập service **Trino Query Engine** kết nối tới Iceberg catalog của MinIO.
*   [x] Viết các truy vấn phân tích SQL nâng cao (Window Functions, CTEs, Aggregations) trên Trino.
*   [x] Thực hiện tối ưu hóa truy vấn bằng cách đọc Execution Plan (`EXPLAIN ANALYZE`).
*   [x] Tạo các Views phục vụ trực tiếp cho báo cáo và ML (Đã tạo view `v_daily_market_summary` ở tầng PostgreSQL/TimescaleDB).
*   [x] **Nâng cấp Enterprise**: Thực thi **Federated Query** (Truy vấn liên kết) - Thực hiện JOIN trực tiếp giữa bảng dữ liệu lịch sử khổng lồ trong Apache Iceberg (MinIO) và bảng cấu hình nhỏ trong PostgreSQL trên cùng một câu lệnh SQL.

### Phase 5 [COLD PATH]: Airflow Orchestration & Modern Workflow 🎼 [HOÀN THÀNH 🎉]
*   [x] Tích hợp Airflow DAG điều phối: Ingestion -> Spark Batch Job.
*   [x] **Refactor & Modernize (Áp dụng tư duy Airflow 3 / Modern Airflow)**:
    *   [x] Chuyển đổi toàn bộ DAG sang cấu trúc **TaskFlow API (`@dag`, `@task`)** giúp loại bỏ code boilerplate của Operator truyền thống.
    *   [x] Cấu hình cơ chế **Asset-Based / Dataset Scheduling** (Lập lịch hướng sự kiện dữ liệu) & sequential medallion orchestration.
    *   [x] Áp dụng **Dynamic Task Mapping** hoặc sequential task flow để điều phối đồng bộ linh hoạt tại runtime.
    *   [x] Xây dựng luồng Lakehouse chuẩn: Ingestion (Producers -> Kafka) -> Bronze Layer (MinIO raw_trades JSON) -> Silver Layer (Iceberg Star Schema Fact/Dim) -> Gold/Serving Layer (TimescaleDB).
*   [x] **Chuyển đổi Executor**: Chuyển cấu hình Airflow sang **LocalExecutor** (hoặc Sequential với multi-threading hỗ trợ) để chạy các task ổn định.
*   [x] **Lập lịch Micro-batch**: Điều chỉnh DAG chạy mỗi 5 phút một lần (`*/5 * * * *`) để xử lý thời gian thực Cold Path.
*   [x] **Tự động hóa bảo trì**: Thiết kế các job bảo trì Iceberg tables (Compaction, Expire Snapshots) chạy định kỳ để dọn dẹp các tệp nhỏ sinh ra do micro-batch.
*   [ ] **Tuning & Management (Chưa làm)**: Chuyển toàn bộ mật khẩu, thông tin nhạy cảm vào *Airflow Connections & Variables* (thay vì code cứng).
*   [ ] **Tuning & Management (Chưa làm)**: Tích hợp cơ chế tự động chạy lại (Retries) và cơ chế báo lỗi thông qua Slack/Telegram Alert.
*   [ ] **Nâng cấp Enterprise (Chưa làm)**: Thay thế việc đồng bộ cứng Spark Job bằng **Deferrable Operators** (Triggerer không đồng bộ) để giải phóng tài nguyên worker của Airflow trong lúc chờ Spark chạy xong.
### Phase 6 [ML PATH]: Machine Learning Pipeline & MLOps ⏳ [ĐANG THỰC HIỆN ⏳]

*   **Tổng quan luồng máy học (ML Path)**:
    ```
    [Apache Iceberg] ➔ [Trino Query Engine] ➔ [build_features.py (Pandas)] ➔ [train.py (XGBoost)] ➔ [MLflow Server (Cổng 5000)]
                                                   │
                                                   ▼
                                         [predict.py (Inference)] ➔ [TimescaleDB (daily_predictions)]
    ```

*   **ML Training Flow (Hướng dẫn thực hiện)**:
    *   [ ] **Bước 6.1: Tích hợp MLflow vào Docker Compose**:
        *   Thêm service `mlflow` vào file `docker-compose.yml` (profile `ml`):
            ```yaml
            mlflow:
              image: ghcr.io/mlflow/mlflow:latest
              container_name: mlflow
              profiles: ["ml"]
              ports:
                - "5000:5000"
              command: >
                mlflow server
                  --host 0.0.0.0
                  --port 5000
                  --backend-store-uri sqlite:///mlflow.db
                  --default-artifact-root /mlflow/artifacts
              volumes:
                - mlflow_data:/mlflow
              networks:
                - tradestream-net
            ```
        *   Thêm volume `mlflow_data` và cập nhật `_PIP_ADDITIONAL_REQUIREMENTS` trong container Airflow để cài thêm: `mlflow trino xgboost scikit-learn pandas numpy sqlalchemy psycopg2-binary`.
    *   [ ] **Bước 6.2: Xây dựng module trích xuất đặc trưng `ml/features/build_features.py`**:
        *   *Nhiệm vụ*: Kết nối Trino client (`trino.dbapi.connect`) tới `http://trino:8080` (hoặc `localhost:8089` từ máy local).
        *   *Tính toán đặc trưng*:
            - Đọc dữ liệu lịch sử từ bảng `lakehouse.trading.fact_daily_prices`.
            - Dùng Pandas tính chỉ báo kỹ thuật: SMA 5, SMA 10, SMA 20 của giá đóng cửa.
            - Tính lag features: `close_lag_1` (giá đóng cửa ngày hôm trước), `volume_lag_1`, tỷ suất sinh lời trễ `return_lag_1` (daily_return của ngày hôm trước).
            - Tạo nhãn `target`: So sánh giá đóng cửa ngày tiếp theo với ngày hiện tại (1 nếu tăng, 0 nếu giảm). Cần sử dụng `.shift(-1)` trên chuỗi giá trị đóng cửa theo từng Symbol.
    *   [ ] **Bước 6.3: Phát triển script huấn luyện `ml/training/train.py`**:
        *   *Nhiệm vụ*: Lấy dữ liệu từ `build_features.py`, chia tập Train/Test theo thời gian (ví dụ: 80% thời gian đầu để train, 20% sau để test, tránh dùng random train_test_split làm rò rỉ dữ liệu).
        *   *Huấn luyện*: Sử dụng `XGBoostClassifier` để học xu hướng lên/xuống của thị trường.
        *   *MLflow Integration*:
            - Thiết lập tracking URI: `mlflow.set_tracking_uri("http://localhost:5000")`.
            - Mở context `with mlflow.start_run():` để log các tham số (`max_depth`, `n_estimators`, `learning_rate`) và các metric đánh giá (`accuracy`, `precision`, `recall`, `f1_score`).
            - Log mô hình bằng `mlflow.xgboost.log_model(model, "price_direction_model")` và đăng ký phiên bản mô hình lên Model Registry với tag alias `Production`.
    *   [ ] **Bước 6.4: Thiết lập Airflow DAG điều phối việc huấn luyện định kỳ (`dags/tradestream/ml_pipelines.py`)**:
        *   Tạo DAG `ml_training_pipeline` chạy định kỳ hàng tuần (ví dụ: `0 0 * * 0` vào Chủ Nhật) để trigger script `train.py`.

*   **ML Prediction Flow (Hướng dẫn thực hiện)**:
    *   [ ] **Bước 6.5: Thiết lập bảng `daily_predictions` trong TimescaleDB**:
        *   *Nhiệm vụ*: Tạo bảng đích để lưu trữ dự báo của mô hình.
        *   *Schema mẫu (Khởi tạo trên database `tradestream`)*:
            ```sql
            CREATE TABLE IF NOT EXISTS daily_predictions (
                symbol VARCHAR(20) NOT NULL,
                prediction_date DATE NOT NULL,      -- Ngày được dự báo (ví dụ ngày mai)
                predicted_direction INT NOT NULL,   -- 1: Tăng, 0: Giảm
                probability DOUBLE PRECISION,       -- Độ tin cậy (xác suất dự báo)
                model_version VARCHAR(50),          -- Version mô hình từ MLflow
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, prediction_date)
            );
            -- Chuyển thành Hypertable phân vùng theo ngày
            SELECT create_hypertable('daily_predictions', 'prediction_date', if_not_exists => TRUE);
            CREATE INDEX IF NOT EXISTS ix_predictions_symbol_time ON daily_predictions (symbol, prediction_date DESC);
            ```
    *   [ ] **Bước 6.6: Phát triển script batch inference `ml/serving/predict.py`**:
        *   *Nhiệm vụ*: Tải mô hình `Production` từ MLflow Server (`mlflow.pyfunc.load_model("models:/price_direction_model/Production")`).
        *   *Luồng xử lý*:
            - Lấy đặc trưng ngày hiện tại (`fetch_date` hôm nay) từ Trino.
            - Chạy dự đoán xu hướng cho ngày mai.
            - Ghi kết quả vào bảng `daily_predictions` trong TimescaleDB, thiết lập `prediction_date` là ngày tiếp theo để có thể so khớp với dữ liệu thực tế sau đó.
    *   [ ] **Bước 6.7: Thiết lập Airflow DAG chạy dự báo hàng ngày**:
        *   Tạo DAG `ml_prediction_pipeline` chạy hàng ngày lúc 23:00 (sau khi dữ liệu giá của ngày hiện tại đã đồng bộ thành công qua Cold Path).

---

### Phase 7 [HOT PATH & UI]: Grafana Dashboard & Real-Time Hot Path 📊 [ĐANG THỰC HIỆN ⏳]

*   **Tổng quan luồng Hot Path**:
    ```
    [Binance/Yahoo API] ➔ [Kafka Topics] ➔ [Spark Structured Streaming (stream_hot_path.py)]
                                                              │ (Slide Window 1m / Slide 10s)
                                                              ▼
                                               [TimescaleDB (crypto_indicators)] ➔ [Grafana]
    ```

*   **Real-Time Hot Path (Hướng dẫn thực hiện)**:
    *   [ ] **Bước 7.1: Phát triển Spark Structured Streaming Job (`src/processing/tradestream/stream_hot_path.py`)**:
        *   *Đọc Stream*: Kết nối tới các topics Kafka `crypto_trades` và `stock_trades`:
            ```python
            kafka_stream = (
                spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_brokers)
                .option("subscribe", "crypto_trades,stock_trades")
                .option("startingOffsets", "latest")
                .load()
            )
            ```
        *   *Xử lý dữ liệu*: Parse JSON từ message `value` theo schema chuẩn hóa (`symbol`, `price`, `quantity`, `trade_time`), sau đó chuyển `trade_time` (mili giây) thành Timestamp (`event_time`).
        *   *Sliding Window & Watermarking*: Áp dụng watermark 1 phút để loại bỏ/xử lý dữ liệu đến trễ và nhóm dữ liệu theo cửa sổ trượt 1 phút, trượt 10 giây:
            ```python
            windowed_df = (
                stream_df
                .withWatermark("event_time", "1 minute")
                .groupBy(F.col("symbol"), F.window(F.col("event_time"), "1 minute", "10 seconds"))
                .agg(
                    F.avg("price").alias("sma"),
                    F.when(F.sum("quantity") > 0, F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).otherwise(F.avg("price")).alias("vwap"),
                    F.count("price").alias("trade_count")
                )
            )
            ```
    *   [ ] **Bước 7.2: Thực hiện ghi (upsert) thời gian thực bằng `foreachBatch` vào TimescaleDB**:
        *   *Nhiệm vụ*: Vì Spark Structured Streaming không hỗ trợ upsert (ON CONFLICT) mặc định cho JDBC Sink, cần viết hàm ghi tùy chỉnh:
            ```python
            def write_to_timescale(batch_df, batch_id):
                # 1. Ghi đè DataFrame của batch vào một bảng staging tạm thời trên DB
                batch_df.write.format("jdbc").option("dbtable", "crypto_indicators_staging").mode("overwrite").save()
                
                # 2. Sử dụng JVM Connection để execute native UPSERT từ bảng staging sang bảng chính
                conn = batch_df.sparkSession._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
                stmt = conn.createStatement()
                stmt.execute("""
                    INSERT INTO crypto_indicators (symbol, window_start, window_end, sma, vwap, trade_count)
                    SELECT symbol, window_start, window_end, sma, vwap, trade_count
                    FROM crypto_indicators_staging
                    ON CONFLICT (symbol, window_start) DO UPDATE SET
                        window_end = EXCLUDED.window_end,
                        sma = EXCLUDED.sma,
                        vwap = EXCLUDED.vwap,
                        trade_count = EXCLUDED.trade_count
                """)
                stmt.execute("DROP TABLE IF EXISTS crypto_indicators_staging")
                conn.close()
            ```
        *   *Bắt đầu ghi stream*:
            ```python
            query = (
                windowed_indicators.writeStream
                .foreachBatch(write_to_timescale)
                .option("checkpointLocation", "s3a://lakehouse/checkpoints/hot_path")
                .trigger(processingTime="10 seconds")
                .start()
            )
            ```
    *   [ ] **Bước 7.3: Thiết lập Airflow DAG giám sát Streaming Job (`dags/tradestream/stream_hot_path_dag.py`)**:
        *   Sử dụng BashOperator kích hoạt chạy ngầm hoặc theo dõi trạng thái Spark Structured Streaming job thông qua API của Spark Master hoặc Spark REST API để đảm bảo luồng streaming chạy liên tục 24/7.

*   **Grafana Visualization (Hướng dẫn thực hiện)**:
    *   [ ] **Bước 7.4: Cấu hình TimescaleDB Datasource cho Biểu đồ Nến & Chỉ báo (Hot Path)**:
        *   Thiết lập truy vấn lấy dữ liệu từ bảng `crypto_indicators` với interval ngắn (ví dụ: quét 5s/lần) để hiển thị đường SMA/VWAP thời gian thực song song với biểu đồ giá.
    *   [ ] **Bước 7.5: Cấu hình Trino Datasource cho Phân tích Dài hạn (Cold Path)**:
        *   Tạo panel sử dụng Trino catalog kết nối Iceberg để vẽ biểu đồ so sánh khối lượng và xu hướng tích lũy theo tháng/năm.
    *   [ ] **Bước 7.6: Trực quan hóa ML Predictions (Forecast vs Actual)**:
        *   *Nhiệm vụ*: Vẽ biểu đồ so sánh xu hướng dự báo của mô hình với giá đóng cửa thực tế xảy ra.
        *   *Truy vấn SQL mẫu phục vụ panel*:
            ```sql
            SELECT 
                p.prediction_date AS time,
                p.symbol,
                p.predicted_direction,
                p.probability,
                d.close_price AS actual_close,
                -- Hậu kiểm: Hướng giá thực tế (1: Tăng, 0: Giảm so với ngày trước đó)
                CASE WHEN d.close_price > LAG(d.close_price, 1) OVER (PARTITION BY d.symbol ORDER BY d.fetch_date) THEN 1 ELSE 0 END AS actual_direction
            FROM daily_predictions p
            INNER JOIN daily_prices d ON p.symbol = d.symbol AND p.prediction_date = d.fetch_date
            WHERE p.symbol = 'BTC-USD' AND p.prediction_date >= NOW() - INTERVAL '30 days'
            ORDER BY time;
            ```
    *   [ ] **Bước 7.7: Cấu hình Alerting trên Grafana**:
        *   Thiết lập cảnh báo tự động gửi qua Telegram/Email nếu phát hiện model dự báo sai quá 3 ngày liên tiếp hoặc dữ liệu Hot Path bị trễ quá 2 phút không có bản ghi mới.

### Phase 8: Data Quality, Lineage & Alerting ⏳ [CHƯA THỰC HIỆN]
*   [ ] Tích hợp **Great Expectations** vào pipeline của Airflow để kiểm định 6 chiều kích thước của chất lượng dữ liệu (Data Quality).
*   [ ] **Data Quality Gates**: Thiết lập Great Expectations như một chốt chặn bắt buộc trước khi load dữ liệu vào Silver/Gold. Nếu check fail, pipeline lập tức dừng lại và roll back dữ liệu.
*   [ ] Thiết lập cơ chế gửi cảnh báo tự động qua **Telegram/Slack Bot** khi pipeline bị lỗi hoặc phát hiện dữ liệu bẩn.
*   [ ] Hardening hệ thống: Mã hóa credentials bằng Docker Secrets/Environment Variables bảo mật.
*   [ ] **Nâng cấp Enterprise (Chưa làm)**: Tích hợp **OpenLineage** và **Marquez** để tự động vẽ bản đồ nguồn gốc dịch chuyển dữ liệu (Data Lineage) giữa các task trong Spark và Airflow, hỗ trợ kiểm soát dữ liệu.

### Phase 9: CI/CD & Automated Testing ⏳ [CHƯA THỰC HIỆN]
*   [ ] Viết **Unit Tests** cho các hàm tính toán của PySpark sử dụng thư viện `pytest` và `chispa` (kiểm tra schema, logic SMA/EMA).
*   [ ] Thiết lập **GitHub Actions** tự động chạy linter (`Ruff` / `Black`) và chạy Unit Tests mỗi khi push code lên repo.
*   [ ] Cấu hình GitHub Actions tự động build Docker Image cho Spark, Airflow, Producer và đẩy lên Docker Hub / GitHub Packages.
*   [ ] Tự động hóa đồng bộ (sync) DAGs từ repository Git vào thư mục `dags` của Airflow sử dụng Git-sync hoặc CI/CD pipeline.

### Phase 10: [ĐÃ BỎ QUA] Infrastructure as Code (IaC) & Kubernetes Deployment ❌
*   *Phase này được lược bỏ để tiết kiệm RAM/CPU cho máy cá nhân và tập trung 100% vào việc nắm vững cốt lõi kỹ thuật xử lý dữ liệu Lakehouse.*
