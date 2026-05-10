---
name: trino-learning
description: "Hướng dẫn học Trino (distributed SQL engine) từ zero cho dự án TradeStream. Query Iceberg Lakehouse bằng SQL quen thuộc."
---

# Trino Query Engine — TradeStream

## Mục đích

Hướng dẫn học và triển khai Trino để chạy SQL queries trên Apache Iceberg Lakehouse trong dự án TradeStream.

## Khi nào sử dụng

- Khi bắt đầu Phase 4 của TradeStream
- Khi cần viết analytical queries
- Khi tối ưu query performance
- Khi tạo views cho dashboard

---

## Concepts cốt lõi

### Trino là gì?
Trino (trước đây gọi là PrestoSQL) là **distributed SQL query engine** — chạy SQL trên nhiều nguồn dữ liệu khác nhau mà không cần di chuyển data.

```
Trino có thể query đồng thời:
  → Iceberg tables (Lakehouse)
  → PostgreSQL (traditional DB)
  → Kafka topics (streaming data)
  → Tất cả trong 1 câu SQL!
```

### Thuật ngữ

| Thuật ngữ | Giải thích | Ví dụ |
|-----------|-----------|-------|
| **Catalog** | Nguồn dữ liệu | `lakehouse` (Iceberg), `kafka` |
| **Schema** | Nhóm tables | `lakehouse.trading` |
| **Connector** | Plugin kết nối data source | Iceberg connector, Kafka connector |
| **Coordinator** | Node điều phối queries | 1 coordinator cho dev |
| **Worker** | Node thực thi queries | Coordinator kiêm worker cho dev |

---

## Docker Setup

```yaml
# docker-compose.yml (profile: query)
services:
  trino:
    image: trinodb/trino:latest
    profiles: ["query"]
    ports:
      - "8082:8080"     # Trino UI
    volumes:
      - ./config/trino/catalog:/etc/trino/catalog
    environment:
      - JAVA_TOOL_OPTIONS=-Xmx1G
```

### Catalog config: Iceberg

```properties
# config/trino/catalog/lakehouse.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
iceberg.file-system-type=NATIVE_S3
s3.endpoint=http://minio:9000
s3.aws-access-key=tradestream
s3.aws-secret-key=tradestream123
s3.path-style-access=true
s3.region=us-east-1
```

---

## Quick Demo

```sql
-- Kết nối Trino CLI hoặc DBeaver
-- 1. Liệt kê catalogs
SHOW CATALOGS;

-- 2. Xem schemas
SHOW SCHEMAS FROM lakehouse;

-- 3. Xem tables
SHOW TABLES FROM lakehouse.trading;

-- 4. Query đầu tiên!
SELECT symbol, price, volume, event_time
FROM lakehouse.trading.fact_trades
ORDER BY event_time DESC
LIMIT 10;
```

---

## Analytical Queries cho Trading

### Top movers (thay đổi giá nhiều nhất)
```sql
SELECT 
    a.symbol,
    a.asset_type,
    MIN(f.price) AS low_price,
    MAX(f.price) AS high_price,
    MAX(f.price) - MIN(f.price) AS price_range,
    ROUND((MAX(f.price) - MIN(f.price)) / MIN(f.price) * 100, 2) AS change_pct
FROM lakehouse.trading.fact_trades f
JOIN lakehouse.trading.dim_assets a ON f.asset_id = a.asset_id
WHERE f.event_time >= current_timestamp - interval '24' hour
GROUP BY a.symbol, a.asset_type
ORDER BY change_pct DESC;
```

### Volume analysis
```sql
SELECT 
    t.event_hour,
    a.asset_type,
    SUM(f.volume) AS total_volume,
    AVG(f.price) AS avg_price,
    COUNT(*) AS trade_count
FROM lakehouse.trading.fact_trades f
JOIN lakehouse.trading.dim_assets a ON f.asset_id = a.asset_id
JOIN lakehouse.trading.dim_time t ON f.time_id = t.time_id
GROUP BY t.event_hour, a.asset_type
ORDER BY t.event_hour;
```

### RSI Overbought/Oversold alerts
```sql
SELECT symbol, price, rsi_14, event_time,
    CASE 
        WHEN rsi_14 > 70 THEN '🔴 QUÁ MUA'
        WHEN rsi_14 < 30 THEN '🟢 QUÁ BÁN'
        ELSE '⚪ BÌNH THƯỜNG'
    END AS rsi_signal
FROM lakehouse.trading.fact_trades
WHERE rsi_14 IS NOT NULL
    AND (rsi_14 > 70 OR rsi_14 < 30)
ORDER BY event_time DESC
LIMIT 20;
```

### Views cho Grafana Dashboard
```sql
-- View: Tóm tắt thị trường real-time
CREATE OR REPLACE VIEW lakehouse.trading.v_market_summary AS
SELECT 
    a.symbol,
    a.asset_type,
    f.price AS current_price,
    f.sma_20,
    f.ema_12,
    f.rsi_14,
    f.vwap,
    f.volume,
    f.event_time
FROM lakehouse.trading.fact_trades f
JOIN lakehouse.trading.dim_assets a ON f.asset_id = a.asset_id
WHERE f.event_time >= current_timestamp - interval '1' hour;
```

---

## Troubleshooting

| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| `Catalog not found` | File .properties chưa mount đúng | Check volume mount path |
| `Cannot connect to Iceberg REST` | Catalog service chưa start | Start storage profile trước |
| Query chậm | Không filter theo partition | Thêm WHERE trên partition column |
| `Access Denied to S3` | MinIO credentials sai | Check catalog properties file |

---

## Checklist Phase 4

```
□ Trino chạy, UI tại http://localhost:8082
□ Catalog lakehouse kết nối thành công
□ SHOW TABLES hiển thị đúng tables
□ Quick Demo: SELECT query hoạt động
□ Analytical queries chạy đúng kết quả
□ Views cho dashboard đã tạo
□ Query performance chấp nhận được
□ Learning-log: Trino concepts
```
