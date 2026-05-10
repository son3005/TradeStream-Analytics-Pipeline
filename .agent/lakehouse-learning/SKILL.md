---
name: lakehouse-learning
description: "Hướng dẫn học kiến trúc Lakehouse với Apache Iceberg + MinIO từ zero cho dự án TradeStream. Bao gồm concepts, Star Schema design, và hands-on setup."
---

# Lakehouse Architecture — TradeStream

## Mục đích

Hướng dẫn học và triển khai kiến trúc Lakehouse với Apache Iceberg (table format) + MinIO (S3-compatible storage) cho dự án TradeStream.

## Khi nào sử dụng

- Khi bắt đầu Phase 3 của TradeStream
- Khi thiết kế Star Schema
- Khi cần tối ưu storage/partitioning
- Khi gặp lỗi Iceberg operations

---

## Concepts cốt lõi

### Lakehouse là gì?

Lakehouse = **Data Lake** (lưu mọi loại data, giá rẻ) + **Data Warehouse** (query nhanh, schema rõ ràng).

```
Truyền thống:
  Raw Data → Data Lake (file thô) → ETL → Data Warehouse (SQL)

Lakehouse:
  Raw Data → Lakehouse (file thô + table format + SQL engine)
                ↑ Apache Iceberg giúp biến file thô thành "bảng SQL"
```

### Các thành phần

| Thành phần | Công nghệ | Vai trò |
|-----------|-----------|---------|
| **Storage** | MinIO (S3-compatible) | Lưu trữ file Parquet (data thật) |
| **Table Format** | Apache Iceberg | Quản lý metadata, schema, transactions |
| **Catalog** | Iceberg REST Catalog | Đăng ký và tìm kiếm tables |
| **Query Engine** | Trino (Phase 4) | Chạy SQL trên Iceberg tables |
| **Write Engine** | Spark | Ghi data vào Iceberg tables |

### Tại sao Iceberg?

| Tính năng | Giải thích | Ví dụ |
|-----------|-----------|-------|
| **Time Travel** | Quay lại phiên bản cũ của data | `SELECT * FROM trades VERSION AS OF 123` |
| **Schema Evolution** | Thêm/sửa cột không cần rebuild | Thêm cột `bollinger_upper` sau |
| **Partition Evolution** | Đổi cách partition không cần rewrite | Chuyển từ partition theo ngày → theo giờ |
| **ACID Transactions** | Đọc/ghi đồng thời an toàn | Spark ghi + Trino đọc cùng lúc |
| **Hidden Partitioning** | Tự động partition, user không cần biết | `WHERE trade_date = '2026-01-01'` tự filter |

---

## Docker Setup

```yaml
# docker-compose.yml (profile: storage)
services:
  minio:
    image: minio/minio:latest
    profiles: ["storage"]
    ports:
      - "9000:9000"     # API
      - "9001:9001"     # Console UI
    environment:
      MINIO_ROOT_USER: tradestream
      MINIO_ROOT_PASSWORD: tradestream123
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 5

  # Iceberg REST Catalog
  iceberg-rest:
    image: tabulario/iceberg-rest:latest
    profiles: ["storage"]
    depends_on:
      minio:
        condition: service_healthy
    ports:
      - "8181:8181"
    environment:
      CATALOG_WAREHOUSE: s3://tradestream-lakehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      CATALOG_S3_ENDPOINT: http://minio:9000
      AWS_ACCESS_KEY_ID: tradestream
      AWS_SECRET_ACCESS_KEY: tradestream123
      AWS_REGION: us-east-1

volumes:
  minio-data:
```

---

## Star Schema Design

### Fact Table: `fact_trades`

```sql
CREATE TABLE lakehouse.fact_trades (
    trade_id        BIGINT,
    event_time      TIMESTAMP,
    asset_id        INT,         -- FK → dim_assets
    exchange_id     INT,         -- FK → dim_exchanges
    time_id         INT,         -- FK → dim_time
    
    -- Measures (giá trị đo)
    price           DECIMAL(18, 8),
    volume          DECIMAL(18, 8),
    trade_count     INT,
    
    -- Technical Indicators
    sma_20          DECIMAL(18, 8),
    ema_12          DECIMAL(18, 8),
    ema_26          DECIMAL(18, 8),
    rsi_14          DECIMAL(18, 8),
    vwap            DECIMAL(18, 8),
    bollinger_upper DECIMAL(18, 8),
    bollinger_lower DECIMAL(18, 8),
    
    -- Metadata
    ingestion_time  TIMESTAMP,
    batch_id        STRING
)
USING iceberg
PARTITIONED BY (days(event_time), asset_id)
```

### Dimension: `dim_assets`

```sql
CREATE TABLE lakehouse.dim_assets (
    asset_id    INT,
    symbol      STRING,      -- 'BTCUSDT', 'AAPL'
    name        STRING,      -- 'Bitcoin', 'Apple Inc.'
    asset_type  STRING,      -- 'crypto', 'stock'
    sector      STRING,      -- 'Technology', 'Finance'
    market      STRING,      -- 'Binance', 'NASDAQ'
    is_active   BOOLEAN
)
USING iceberg
```

### Dimension: `dim_exchanges`

```sql
CREATE TABLE lakehouse.dim_exchanges (
    exchange_id INT,
    name        STRING,      -- 'Binance', 'Yahoo Finance'
    type        STRING,      -- 'crypto_exchange', 'stock_data_provider'
    timezone    STRING,      -- 'UTC', 'America/New_York'
    api_type    STRING       -- 'websocket', 'rest_polling'
)
USING iceberg
```

### Dimension: `dim_time`

```sql
CREATE TABLE lakehouse.dim_time (
    time_id         INT,
    event_date      DATE,
    event_hour      INT,
    event_minute    INT,
    day_of_week     STRING,      -- 'Monday', 'Friday'
    day_of_week_num INT,         -- 1-7
    is_weekend      BOOLEAN,
    week_of_year    INT,
    month           INT,
    quarter         INT,
    year            INT,
    market_session  STRING       -- 'pre_market', 'regular', 'after_hours', '24h'
)
USING iceberg
```

---

## Quick Demo: Iceberg Time Travel

```python
"""Quick Demo: Tạo Iceberg table + Time Travel"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Iceberg-QuickDemo") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "rest") \
    .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0") \
    .getOrCreate()

# Tạo table
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.demo.prices (
        symbol STRING, price DOUBLE, ts TIMESTAMP
    ) USING iceberg
""")

# Insert data
spark.sql("INSERT INTO lakehouse.demo.prices VALUES ('BTC', 67000, current_timestamp())")
# Snapshot 1 ☝️

spark.sql("INSERT INTO lakehouse.demo.prices VALUES ('BTC', 68000, current_timestamp())")
# Snapshot 2 ☝️

# Time Travel: Xem data ở snapshot cũ!
history = spark.sql("SELECT * FROM lakehouse.demo.prices.history")
history.show()

# Quay về snapshot đầu tiên
snapshot_id = history.collect()[0]["snapshot_id"]
spark.sql(f"SELECT * FROM lakehouse.demo.prices VERSION AS OF {snapshot_id}").show()

print("✅ Time Travel thành công! Bạn có thể quay về bất kỳ thời điểm nào.")
```

---

## Troubleshooting

| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| `NoSuchTableException` | Table chưa tạo hoặc catalog sai | Check catalog config, tạo table trước |
| `S3Exception: Access Denied` | MinIO credentials sai | Check `AWS_ACCESS_KEY_ID` và `AWS_SECRET_ACCESS_KEY` |
| `Cannot connect to MinIO` | MinIO chưa start | Check Docker container health |
| Dữ liệu ghi nhưng query trống | Partition filter sai | Check partition spec |

---

## Checklist Phase 3

```
□ MinIO chạy, Console UI tại http://localhost:9001
□ Iceberg REST Catalog chạy tại http://localhost:8181
□ Bucket 'tradestream-lakehouse' đã tạo trên MinIO
□ Quick Demo: tạo table + time travel thành công
□ Star Schema tables đã tạo (fact_trades, dim_*)
□ Spark Streaming ghi được vào Iceberg
□ Data có thể query bằng Spark SQL
□ Partitioning theo ngày hoạt động
□ Learning-log: Lakehouse concepts
```
