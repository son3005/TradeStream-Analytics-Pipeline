# 📈 TradeStream Analytics Pipeline - Progress Tracker

## 📊 Tổng quan Lộ trình thực hiện
*   **Trạng thái hiện tại**: Đang thực hiện (Thiết lập Query Engine & Trino SQL Analytics)
*   **Tiến độ tổng thể**: 
    *   **Hoàn thành (Đúng hướng)**: Phase 0, Phase 1, Phase 2, Phase 3 (Lakehouse Storage), Phase 4 (Query Engine & Trino SQL Analytics).
    *   **Đã làm (Cần cập nhật lại)**: Phase 5 (Airflow), Phase 7 (Grafana) - *Hiện đang kết nối trực tiếp TimescaleDB, cần cập nhật kết nối qua Lakehouse ở các bước sau*.
    *   **Đang thực hiện**: Phase 5 (Airflow Orchestration & Modern Workflow).
    *   **Chưa thực hiện**: Phase 6 (ML Pipeline & MLOps), Phase 8, Phase 9.
    *   **Bỏ qua / Tối giản**: Phase 7 (Bỏ BI Tool phụ, tập trung dùng Grafana), Phase 10 (Bỏ qua IaC & K8s để tiết kiệm RAM/CPU và giữ dự án gọn nhẹ).

---

## 🗺️ Lộ trình chi tiết từng Phase (Đã tinh giản & tối ưu hóa)

### Phase 0: Foundation & Setup 🩸 [HOÀN THÀNH 🎉]
*   [x] Khởi tạo cấu trúc thư mục dự án (Infrastructure, Code, Docs).
*   [x] Thiết lập Docker Compose cơ bản cho dịch vụ nền tảng.
*   [x] Tạo môi trường Python ảo (venv) và cài đặt dependencies (`requirements.txt`).
*   [x] Tìm hiểu & lưu nhật ký kiến trúc: REST API vs WebSocket, Metaphor về Docker, và 4 nguyên tắc viết Dockerfile tối ưu.
*   [x] **Lý thuyết nền tảng**: Tìm hiểu hệ sinh thái **Hadoop (HDFS, YARN, MapReduce)** và triết lý thiết kế **Data Modeling trong Big Data** (Columnar format, Partitioning vs Bucketing, Phi chuẩn hóa dữ liệu).

### Phase 1: Kafka Ingestion & Producer Resilience 📨 [HOÀN THÀNH 🎉]
*   [x] Xây dựng Python Async Producer (`yahoo_batch_producer.py`) lấy giá thị trường.
*   [x] Tạo Kafka Topic `raw_daily_prices` trên Kafka Cluster Docker.
*   [x] Đẩy dữ liệu thô (Raw JSON) thành công vào Kafka.
*   [x] Tìm hiểu & lưu nhật ký: Cơ chế Partition & Offset, Replication, Zookeeper và giám sát Consumer Lag trên Kafka-UI.
*   [x] **Lý thuyết nâng cao**: Tìm hiểu bản chất **Change Data Capture (CDC)**, cơ chế Log-based CDC của **Debezium** và quy trình tích hợp với Kafka.
*   [x] **Tuning & Management**: Cấu hình tối ưu Producer (`acks=all` để bảo toàn dữ liệu, `linger.ms=5` và `batch.size=16384` kết hợp nén dữ liệu `snappy` để giảm tải I/O mạng).
*   [x] **Tuning & Management**: Cấu hình Spark Consumer sử dụng Manual Commit (`kafka.enable.auto.commit=false`) kết hợp với `failOnDataLoss=false` để đảm bảo xử lý chính xác (At-least-once).
*   [x] **Resilience & Retry Mechanism**: Bổ sung cơ chế retry với exponential backoff (1s, 2s, 4s...) và rate-limiting tối đa 100 API request đồng thời trong Python Producer.
*   [x] **Nghiên cứu Schema Registry**: Tìm hiểu và lưu tài liệu nghiên cứu về **Schema Registry** (Avro/Protobuf) để quản lý Schema tiến hóa và bảo toàn dữ liệu nhị phân siêu nhẹ (Thay thế container thực tế bằng tài liệu nghiên cứu lý thuyết để tối ưu tài nguyên RAM/CPU cho máy cá nhân).

### Phase 2: Spark Processing & Medallion Architecture ⚡ [HOÀN THÀNH 🎉]
*   [x] Viết script PySpark Batch Processor (`spark_batch_processor.py`) đọc dữ liệu thô từ Kafka.
*   [x] Parse JSON động theo schema định nghĩa sẵn (Schema Enforcement).
*   [x] **Lý thuyết nâng cao**: Tìm hiểu sự khác biệt và cơ chế của **Spark RDD vs DataFrame & Dataset**, vai trò của **Catalyst Optimizer** và **Tungsten Engine**.
*   [x] **Refactor Logic tính toán (Window Functions)**: Loại bỏ hoàn toàn vòng lặp Python / Python UDFs không tối ưu. Viết lại logic tính toán kỹ thuật (Daily Return, Price Range) sử dụng **Spark SQL Window Functions** kết hợp phân vùng (`partitionBy("symbol").orderBy("fetch_date")`) để đảm bảo không bị tính toán lệch chéo dữ liệu giữa các mã tài sản (Đã triển khai trong `transform_bronze_to_silver.py`).
*   [x] **Thay đổi quy trình Dataflow (Medallion)**: Hủy bỏ việc Spark ghi JDBC trực tiếp xuống PostgreSQL/TimescaleDB ở bước thô. Chuyển sang ghi dữ liệu thô dạng JSON/Parquet lên MinIO (Bronze Layer) (Kafka ➔ Bronze ➔ Silver).
*   [x] **Xác lập chế độ chạy (Spark Batch Mode)**: Chạy Spark dưới dạng Batch Job được trigger bởi Airflow định kỳ để tiết kiệm tài nguyên RAM/CPU thay vì chạy Streaming 24/7.
*   [x] **Xử lý Schema Drift & DLQ**: Thiết lập cơ chế bắt lỗi khi cấu trúc dữ liệu đầu vào thay đổi đột ngột (Schema Drift) và đẩy các bản ghi lỗi vào Dead Letter Queue (DLQ) trên MinIO (`s3a://lakehouse/dlq/malformed_daily_prices`).
*   [x] **Tuning & Management**: Cấu hình `spark.sql.shuffle.partitions=4` (thay vì mặc định 200) ở tất cả các script xử lý của Spark để tối ưu hiệu năng tính toán trên lượng dữ liệu nhỏ, loại bỏ empty tasks.
*   [x] **Nâng cấp Enterprise**: Xử lý dữ liệu đến trễ hoặc dữ liệu bị xáo trộn thứ tự (Late & Out-of-order Data) sử dụng cơ chế Deduplication (`dropDuplicates`) kết hợp với lệnh SQL `MERGE INTO` (Upsert) của Iceberg.

### Phase 3: Lakehouse Storage (MinIO + Apache Iceberg) 🎉 [HOÀN THÀNH 🎉]
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

### Phase 4: Query Engine & Trino SQL Analytics 🎉 [HOÀN THÀNH 🎉]
*   [x] Thiết lập service **Trino Query Engine** kết nối tới Iceberg catalog của MinIO.
*   [x] Viết các truy vấn phân tích SQL nâng cao (Window Functions, CTEs, Aggregations) trên Trino.
*   [x] Thực hiện tối ưu hóa truy vấn bằng cách đọc Execution Plan (`EXPLAIN ANALYZE`).
*   [x] Tạo các Views phục vụ trực tiếp cho báo cáo và ML (Đã tạo view `v_daily_market_summary` ở tầng PostgreSQL/TimescaleDB).
*   [x] **Nâng cấp Enterprise**: Thực thi **Federated Query** (Truy vấn liên kết) - Thực hiện JOIN trực tiếp giữa bảng dữ liệu lịch sử khổng lồ trong Apache Iceberg (MinIO) và bảng cấu hình nhỏ trong PostgreSQL trên cùng một câu lệnh SQL.

### Phase 5: Airflow Orchestration & Modern Workflow 🎼 [CẦN CẬP NHẬT KHI LÊN LAKEHOUSE]
*   [x] Tích hợp Airflow DAG điều phối: Ingestion -> Spark Batch Job.
*   [ ] **Refactor & Modernize (Áp dụng tư duy Airflow 3 / Modern Airflow)**:
    *   [ ] Chuyển đổi toàn bộ DAG sang cấu trúc **TaskFlow API (`@dag`, `@task`)** giúp loại bỏ code boilerplate của Operator truyền thống.
    *   [ ] Cấu hình cơ chế **Asset-Based / Dataset Scheduling** (Lập lịch hướng sự kiện dữ liệu): DAG xử lý Spark sẽ chạy tự động ngay sau khi DAG Ingestion cập nhật xong dữ liệu thô (Asset) trên MinIO.
    *   [ ] Áp dụng **Dynamic Task Mapping** (`.expand()`) để song song hóa việc fetch và xử lý dữ liệu cho nhiều symbols linh hoạt tại runtime.
    *   [ ] Xây dựng luồng Lakehouse chuẩn: Ingestion (API -> Kafka) -> Bronze Layer (MinIO JSON) -> Silver Layer (Iceberg Star Schema) -> Gold Layer / Serving Layer (TimescaleDB).
*   [ ] **Chuyển đổi Executor**: Chuyển cấu hình Airflow từ SequentialExecutor sang **LocalExecutor** trên Docker để thực thi các task song song thực sự và kiểm thử Dynamic Task Mapping.
*   [ ] **Tự động hóa bảo trì**: Tích hợp bảo trì Iceberg tables (Compaction, Expire Snapshots) thành một DAG tự động chạy hàng tuần trong Airflow.
*   [ ] **Tuning & Management (Chưa làm)**: Chuyển toàn bộ mật khẩu, thông tin nhạy cảm vào *Airflow Connections & Variables* (thay vì code cứng).
*   [ ] **Tuning & Management (Chưa làm)**: Tích hợp cơ chế tự động chạy lại (Retries) và cơ chế báo lỗi thông qua Slack/Telegram Alert.
*   [ ] **Nâng cấp Enterprise (Chưa làm)**: Thay thế việc đồng bộ cứng Spark Job bằng **Deferrable Operators** (Triggerer không đồng bộ) để giải phóng tài nguyên worker của Airflow trong lúc chờ Spark chạy xong.

### Phase 6: Machine Learning Pipeline & MLOps ⏳ [CHƯA THỰC HIỆN]
*   [ ] Feature Engineering nâng cao (SMA, EMA, RSI, Bollinger Bands...) từ dữ liệu giá trong tầng Silver/Gold.
*   [ ] Viết script train model phân loại (XGBoost) dự đoán xu hướng giá và model hồi quy (LightGBM) dự đoán giá đóng cửa.
*   [ ] Tích hợp **MLflow** để theo dõi (track) thí nghiệm, lưu trữ phiên bản model.
*   [ ] Cấu hình Airflow gọi Model để suy diễn (Inference) và lưu kết quả dự đoán vào DB phục vụ dashboard.
*   [ ] **Nâng cấp Enterprise (Chưa làm)**: Triển khai **Model Serving API** (FastAPI hoặc MLflow Serving Server) để phục vụ dự đoán thời gian thực qua REST API thay vì chỉ suy diễn offline bằng Airflow.

### Phase 7: Grafana Dashboard 📊 [CẦN CẬP NHẬT KHI LÊN LAKEHOUSE]
*   [x] Kết nối Grafana và vẽ thành công các biểu đồ biến động giá từ TimescaleDB.
*   [ ] **Cần Refactor**: Cấu hình thêm Datasource **Trino** vào Grafana để vẽ biểu đồ phân tích dài hạn trực tiếp từ hồ Iceberg.
*   [ ] **Tuning & Management (Chưa làm)**: Cấu hình Alerting trực tiếp trên Grafana (cảnh báo khi dữ liệu bị trễ hoặc giá biến động đột biến qua email/Slack).

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
