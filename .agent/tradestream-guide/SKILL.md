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
├── .agent/                     # Antigravity skills cho dự án
├── docker-compose.yml          # Tất cả services (dùng profiles)
├── .env                        # Environment variables (KHÔNG commit)
├── .env.example                # Template cho .env
├── requirements.txt            # Python dependencies
├── src/
│   ├── __init__.py
│   ├── producers/              # Kafka producers
│   │   ├── __init__.py
│   │   ├── crypto_producer.py  # Binance WebSocket → Kafka
│   │   └── stock_producer.py   # Yahoo Finance → Kafka
│   ├── consumers/              # Kafka consumers
│   │   ├── __init__.py
│   │   └── simple_consumer.py  # Debug consumer
│   ├── processing/             # Spark jobs
│   │   ├── __init__.py
│   │   ├── stream_processor.py # Main streaming job
│   │   └── indicators.py       # SMA, EMA, RSI, VWAP...
│   ├── models/                 # Data models
│   │   ├── __init__.py
│   │   ├── trade.py            # Trade event schema
│   │   └── indicator.py        # Indicator schema
│   ├── storage/                # Lakehouse operations
│   │   ├── __init__.py
│   │   ├── iceberg_writer.py
│   │   └── schema.py           # Star Schema definitions
│   └── utils/                  # Helpers
│       ├── __init__.py
│       ├── config.py           # Configuration loader
│       └── logger.py           # Logging setup
├── config/
│   ├── kafka/                  # Kafka configs
│   ├── spark/                  # Spark configs
│   ├── trino/                  # Trino catalog configs
│   └── airflow/                # Airflow configs
├── notebooks/                  # Jupyter notebooks cho exploration
├── dashboards/                 # Grafana dashboard JSON exports
├── dags/                       # Airflow DAGs
│   ├── daily_batch.py
│   ├── data_quality.py
│   └── backfill.py
├── ml/
│   ├── features/               # Feature engineering
│   ├── models/                 # Trained models
│   ├── training/               # Training scripts
│   └── serving/                # Model serving
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
└── docs/                       # Documentation tiếng Việt
    ├── learning-log.md          # Nhật ký học tập ADHD
    ├── progress.md              # Tiến trình phases
    ├── architecture.md          # Kiến trúc chi tiết
    └── decisions.md             # Decision log
```

## Coding Conventions

### Python
- Python 3.11+
- Type hints bắt buộc cho mọi function
- Docstrings tiếng Việt cho business logic, tiếng Anh cho technical docs
- Pydantic models cho tất cả data schemas
- async/await cho I/O operations
- Logging thay vì print()

### Naming
- Files: `snake_case.py`
- Classes: `PascalCase`
- Functions: `snake_case`
- Constants: `UPPER_SNAKE_CASE`
- Kafka topics: `{domain}.{entity}` (ví dụ: `crypto.prices`, `stock.prices`)
- Iceberg tables: `{schema}.{table}` (ví dụ: `lakehouse.fact_trades`)

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
