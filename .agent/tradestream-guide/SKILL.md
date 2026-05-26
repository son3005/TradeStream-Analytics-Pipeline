---
name: tradestream-guide
description: "Hướng dẫn conventions, architecture decisions, và project standards cho dự án TradeStream Analytics Pipeline. Đọc trước khi code bất kỳ component nào."
---

# TradeStream Project Guide

## Tổng quan dự án

**TradeStream Analytics Pipeline** là hệ thống streaming end-to-end xử lý dữ liệu Crypto + Stock real-time trên kiến trúc Lakehouse.

## Tech Stack

| Layer | Công nghệ | Mục đích |
|-------|-----------|----------|
| Data Sources | Binance WebSocket, Yahoo Finance API | Crypto 24/7, Stock market hours |
| Ingestion | Apache Kafka | Message broker, decoupling |
| Processing | Apache Spark Structured Streaming | Real-time transformations |
| Storage | Apache Iceberg + MinIO | Lakehouse (S3-compatible) |
| Query | Trino | Distributed SQL engine |
| Orchestration | Apache Airflow | Pipeline scheduling |
| ML | Scikit-learn, XGBoost, MLflow | Prediction, anomaly detection |
| Visualization | Grafana | Real-time dashboards |

## Cấu trúc thư mục

```
tradestream/
├── .agent/                     # Hướng dẫn quy trình & Agent skills
├── config/                     # Chứa các cấu hình nghiệp vụ JSON/YAML
│   └── symbols.json            # Tracking symbols
├── dags/                       # Airflow DAGs
│   ├── tradestream/            # DAGs vận hành chính thức của TradeStream
│   │   ├── daily_batch.py
│   │   ├── data_quality.py
│   │   └── backfill.py
│   └── examples/               # DAGs học tập, kiểm thử nhanh
│       └── hello_dag.py
├── dashboards/                 # Grafana dashboard JSON exports
├── docs/                       # Tài liệu tiếng Việt
│   ├── learning-log.md
│   ├── progress.md
│   ├── architecture.md
│   └── decisions.md
├── infrastructure/             # Cấu hình container, database, spark, trino, airflow
│   ├── docker-compose.yml
│   ├── db/
│   ├── spark/
│   ├── trino/
│   └── airflow/
├── src/                        # Mã nguồn chính của dự án (Production Code)
│   ├── producers/              # Trình sản xuất dữ liệu real-time
│   │   ├── crypto_producer.py
│   │   └── stock_producer.py
│   ├── processing/             # Job xử lý Spark chính thức
│   │   └── tradestream/        # Core Spark jobs cho luồng TradeStream
│   │       ├── ingest_raw_to_bronze.py
│   │       ├── transform_bronze_to_silver.py
│   │       └── sync_silver_to_postgres.py
│   ├── management/             # Các scripts thiết lập ban đầu và bảo trì định kỳ
│   │   ├── create_star_schema.py
│   │   ├── generate_dim_date.py
│   │   ├── maintain_lakehouse.py
│   │   └── dim_date.csv        # Dữ liệu tĩnh dim_date
│   └── utils/                  # Tiện ích dùng chung
│       └── spark_helper.py     # Khởi tạo Spark & DB credentials
├── tests/                      # Thư mục kiểm thử tự động
├── tools/                      # Các công cụ phụ trợ, thí nghiệm, code cũ legacy
│   ├── test_kafka_producer.py
│   ├── test_minio_connection.py
│   ├── verify_iceberg_features.py
│   └── yahoo_batch_producer.py
├── requirements.txt            # Python dependencies
└── .env                        # Environment variables (KHÔNG commit)
```

## Coding Conventions

### Python
- Python 3.11+
- Type hints bắt buộc cho mọi function (cả tham số đầu vào và giá trị trả về).
- Docstrings:
  - Sử dụng định dạng **Google Style Docstrings** chuẩn.
  - Viết **100% bằng Tiếng Việt** cho tất cả các hàm (bao gồm cả business logic và technical/infrastructure code). Tuyệt đối không viết nửa tiếng Anh nửa tiếng Việt (hỗn hợp) trong cùng một docstring hoặc giữa các file khác nhau nhằm thống nhất tài liệu hoá dự án.
- Pydantic models cho tất cả data schemas
- async/await cho I/O operations
- Logging thay vì print()

### Naming
- Files: `snake_case.py`
- Classes: `PascalCase`
- Functions: `snake_case` (Bắt đầu bằng các động từ hành động thể hiện rõ chức năng, ví dụ: `get_`, `fetch_`, `calculate_`, `process_`, `sync_`, `validate_`, `maintain_`).
- Variables:
  - Tên biến: `snake_case` (mang tính mô tả, tránh đặt tên viết tắt 1 chữ cái trừ vòng lặp ngắn).
  - Biến Boolean: Bắt đầu bằng tiền tố `is_`, `has_`, `should_`, `enable_` (ví dụ: `is_weekend`, `has_data`, `enable_iceberg`).
  - Danh sách/Từ điển: Đặt tên danh từ số nhiều (ví dụ: `symbols`, `tasks`) hoặc có hậu tố chỉ rõ kiểu cấu trúc (ví dụ: `symbol_map`, `db_config`).
- Constants: `UPPER_SNAKE_CASE`
- Kafka topics: `{domain}.{entity}` (ví dụ: `crypto.prices`, `stock.prices`)
- Iceberg tables: `{schema}.{table}` (ví dụ: `lakehouse.fact_trades`)

### Bố cục File Python (File Layout & Function Order)
Tất cả các file Python trong dự án phải tuân theo cấu trúc sắp xếp từ trên xuống dưới như sau:
1. **Imports**: Sắp xếp theo nhóm (Thư viện chuẩn Python ➔ Thư viện bên thứ ba ➔ Module nội bộ dự án).
2. **Module-level variables / Configurations**: Khai báo hằng số (`UPPER_SNAKE_CASE`) và đọc biến môi trường `.env`.
3. **Helper functions / Callback functions**: Các hàm bổ trợ cấp thấp (ví dụ: `delivery_report`, `get_tick_schema`).
4. **Main Entry Point**: Hàm thực thi chính của file (ví dụ: `def main() -> None:` hoặc `async def poll_stocks_loop() -> None:`).
5. **Execution Guard**: Khối lệnh chạy trực tiếp `if __name__ == "__main__":`.

### Quy tắc Tái sử dụng Code (DRY - Don't Repeat Yourself)
- **Không sao chép (Copy-paste)**: Tuyệt đối không copy các đoạn logic giống nhau qua lại giữa các file (ví dụ: parse URL db, setup logging, load credentials).
- **Trích xuất Helper**: Bất kỳ hàm hoặc khối logic nào xuất hiện từ **2 lần trở lên** ở các file khác nhau bắt buộc phải được tách ra thành helper function đặt trong thư mục [src/utils/](file:///E:/DuAn/TradeStream%20Analytics%20Pipeline/src/utils) (như `spark_helper.py`, `logger.py`, `config.py`) hoặc `src/models/` để import dùng chung.

### Tách biệt Cấu hình (Configuration Isolation)
- **Không ghi cứng (Hardcoding)**:
  - Cấm ghi cứng mật khẩu, token, đường dẫn database (`TIMESCALE_CONN`), endpoint lưu trữ (`MINIO_ENDPOINT`) vào code. Tất cả phải được truyền qua biến môi trường thông qua file `.env`.
  - Cấm ghi cứng danh sách mã chứng khoán/tiền số trong code. Các dữ liệu cấu hình nghiệp vụ này phải được lưu riêng tại [config/symbols.json](file:///E:/DuAn/TradeStream%20Analytics%20Pipeline/config/symbols.json) và được các script đọc động ở runtime.

### Quy chuẩn viết Docstrings (Function Description)
Mỗi function bắt buộc phải có docstring mô tả đầy đủ 4 phần:
1. **Description**: Mô tả ngắn gọn (1 dòng) và mô tả chi tiết (nếu cần) về nhiệm vụ của hàm.
2. **Args**: Danh sách tham số, kèm kiểu dữ liệu và ý nghĩa của từng tham số.
3. **Returns**: Kiểu dữ liệu trả về và mô tả ý nghĩa kết quả.
4. **Raises** (nếu có): Danh sách các exception có thể phát sinh và điều kiện xảy ra.

*Mẫu Docstring chuẩn:*
```python
def calculate_sma(prices: list[float], window_size: int) -> float:
    """
    Tính toán trung bình trượt giản đơn (SMA) cho một chuỗi giá.
    
    Args:
        prices (list[float]): Danh sách các mức giá lịch sử.
        window_size (int): Số lượng phiên (cửa sổ) dùng để tính trung bình.
        
    Returns:
        float: Giá trị trung bình trượt SMA tính được.
        
    Raises:
        ValueError: Nếu danh sách giá rỗng hoặc kích thước cửa sổ lớn hơn chiều dài mảng.
    """
    if not prices:
        raise ValueError("Danh sách giá không được rỗng")
    # ... logic xử lý ...
```
### Quy chuẩn Tổ chức Thư mục và File (Folder & File Organization)
Để đảm bảo mã nguồn dễ đọc hiểu, dễ bảo trì và mở rộng lâu dài, các file trong dự án phải được tổ chức tuân theo các quy tắc sau:
- **Phân tách rạch ròi theo chức năng**:
  - Mã nguồn chạy chính thức của hệ thống (Production Pipeline) chỉ được đặt trong `src/producers/` và `src/processing/{flow_name}/` (ví dụ: `src/processing/tradestream/`).
  - Các script dùng để thiết lập ban đầu (setup/init), sinh dữ liệu mẫu tĩnh, hoặc bảo trì hồ dữ liệu (compaction, expire snapshots) bắt buộc phải đặt trong `src/management/`.
  - Các script thí nghiệm tính năng (experiments), kiểm tra kết nối nhanh, hoặc mã nguồn cũ không còn chạy (legacy) phải đặt trong thư mục `tools/` ở gốc dự án.
- **Tổ chức thư mục dags/**:
  - Tất cả các DAG chính thức được gom vào thư mục con theo nghiệp vụ dưới `dags/{flow_name}/` (ví dụ: `dags/tradestream/`).
  - Các DAG ví dụ, demo học tập được gom vào thư mục `dags/examples/`.
- **Tránh lưu trữ File trống / Thư mục trống (Placeholder)**:
  - Tuyệt đối không giữ các file rỗng 0 bytes hoặc các thư mục trống làm placeholder trong dự án trừ các file `__init__.py` bắt buộc của Python. Nếu chưa có mã nguồn phát triển cụ thể, hãy xóa bỏ chúng để giữ cấu trúc dự án tinh gọn nhất.

### Docker
- Mỗi service có healthcheck
- Sử dụng profiles để quản lý RAM
- Volume mounts cho persistent data
- Network riêng cho internal communication

### Git
- Mỗi phase = 1 branch: `phase-{n}-{name}`
- Commit message: `[Phase N] Mô tả ngắn`
- Merge vào main khi phase hoàn thành
- Tag version: `v0.{phase}.0`

### Testing
- Unit tests cho business logic (indicators, transformations)
- Integration tests cho data flow (producer → consumer)
- Quick smoke tests cho Docker services

## Docker Compose Profiles

```yaml
# Chạy theo phase:
docker compose --profile core up -d                    # Kafka stack
docker compose --profile core --profile processing up -d   # + Spark
docker compose --profile core --profile processing --profile storage up -d  # + Lakehouse
```

| Profile | Services | RAM |
|---------|----------|-----|
| `core` | Zookeeper, Kafka, Kafka UI | ~2GB |
| `processing` | Spark Master, Spark Worker | ~3GB |
| `storage` | MinIO, Iceberg REST Catalog | ~1GB |
| `query` | Trino | ~1GB |
| `orchestration` | Airflow Webserver, Scheduler, PostgreSQL | ~2GB |
| `ml` | MLflow, PostgreSQL (ML metadata) | ~512MB |
| `viz` | Grafana | ~512MB |

## Data Sources

### Binance (Crypto)
- WebSocket stream: `wss://stream.binance.com:9443/ws/{symbol}@trade`
- Symbols mặc định: `btcusdt`, `ethusdt`, `bnbusdt`
- Miễn phí, 24/7, không cần API key cho public data

### Yahoo Finance (Stock)
- Library: `yfinance`
- Symbols mặc định: `AAPL`, `GOOGL`, `MSFT`, `TSLA`
- Polling interval: 1 phút (market hours)
- Miễn phí nhưng có rate limit

## Star Schema

### Fact Table: `fact_trades`
- Grain: mỗi giao dịch hoặc mỗi candle 1 phút
- Measures: price, volume, SMA, EMA, RSI, VWAP, Bollinger Bands

### Dimension Tables
- `dim_assets`: symbol, name, type (crypto/stock), sector
- `dim_exchanges`: Binance, Yahoo Finance, tên, loại
- `dim_time`: date, hour, day_of_week, is_weekend, market_session
