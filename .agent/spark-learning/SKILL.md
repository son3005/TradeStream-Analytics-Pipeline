---
name: spark-learning
description: "Hướng dẫn học Apache Spark Structured Streaming từ zero cho dự án TradeStream. Bao gồm concepts, stream processing patterns, và technical indicator calculations."
---

# Spark Structured Streaming — TradeStream

## Mục đích

Hướng dẫn học và triển khai Spark Structured Streaming để xử lý dữ liệu trading real-time, tính toán chỉ số kỹ thuật (SMA, EMA, RSI, VWAP).

## Khi nào sử dụng

- Khi bắt đầu Phase 2 của TradeStream
- Khi cần debug Spark jobs
- Khi cần tối ưu stream processing
- Khi thêm chỉ số kỹ thuật mới

---

## Concepts cốt lõi

### Spark là gì?
Apache Spark là **distributed computing engine** — xử lý dữ liệu lớn song song trên nhiều máy. Structured Streaming là module xử lý **dòng dữ liệu liên tục** (stream).

### Kiến trúc Spark

```
Driver Program (điều phối)
    ├── Spark Master (quản lý cluster)
    └── Workers (thực thi tasks)
        ├── Executor 1 (xử lý partition 1)
        └── Executor 2 (xử lý partition 2)
```

### Thuật ngữ

| Thuật ngữ | Giải thích | Ví dụ TradeStream |
|-----------|-----------|-------------------|
| **DataFrame** | Bảng dữ liệu phân tán | Bảng chứa trades từ Kafka |
| **Structured Streaming** | Xử lý stream như bảng vô hạn | Mỗi trade mới = 1 row mới |
| **Window** | Khung thời gian gom nhóm | Window 5 phút để tính SMA |
| **Watermark** | Cho phép dữ liệu đến muộn | Chấp nhận data trễ 10 giây |
| **Trigger** | Tần suất xử lý | Mỗi 10 giây xử lý 1 batch |
| **Checkpoint** | Lưu tiến trình để recovery | Spark restart không mất data |
| **Sink** | Nơi output ghi vào | Console, Kafka topic, Iceberg |

---

## Docker Setup

```yaml
# docker-compose.yml (profile: processing)
services:
  spark-master:
    image: bitnami/spark:3.5
    profiles: ["processing"]
    environment:
      SPARK_MODE: master
      SPARK_MASTER_HOST: spark-master
    ports:
      - "8081:8080"   # Spark UI
      - "7077:7077"   # Master port
    volumes:
      - ./src:/opt/spark-apps
      - ./data:/opt/spark-data

  spark-worker:
    image: bitnami/spark:3.5
    profiles: ["processing"]
    depends_on:
      - spark-master
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 2G
      SPARK_WORKER_CORES: 2
    volumes:
      - ./src:/opt/spark-apps
      - ./data:/opt/spark-data
```

---

## Quick Demo: Spark đọc CSV

```python
"""Quick Demo: Đọc CSV giá BTC và tính SMA"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("TradeStream-QuickDemo") \
    .master("local[*]") \
    .getOrCreate()

# Đọc CSV
df = spark.read.csv("data/btc_prices.csv", header=True, inferSchema=True)

# Tính SMA 20 periods
window_spec = Window.orderBy("timestamp").rowsBetween(-19, 0)
df_with_sma = df.withColumn("sma_20", F.avg("price").over(window_spec))

df_with_sma.show(10)
print("✅ SMA tính thành công!")
```

---

## Deep Build: Spark đọc từ Kafka

```python
"""Spark Structured Streaming đọc từ Kafka topic"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("TradeStream-Processor") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Schema cho Kafka messages
trade_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("timestamp", LongType()),
    StructField("exchange", StringType()),
    StructField("asset_type", StringType())
])

# Đọc từ Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "crypto.prices,stock.prices") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
trades = raw_stream \
    .select(F.from_json(F.col("value").cast("string"), trade_schema).alias("trade")) \
    .select("trade.*") \
    .withColumn("event_time", F.from_unixtime(F.col("timestamp") / 1000).cast("timestamp"))

# Tính SMA trong window 5 phút
windowed = trades \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        F.window("event_time", "5 minutes", "1 minute"),
        "symbol"
    ) \
    .agg(
        F.avg("price").alias("sma_5m"),
        F.sum("volume").alias("total_volume"),
        F.count("*").alias("trade_count"),
        F.max("price").alias("high"),
        F.min("price").alias("low")
    )

# Output ra console (debug)
query = windowed.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/tmp/checkpoint/trades") \
    .start()

query.awaitTermination()
```

---

## Chỉ số kỹ thuật (Technical Indicators)

### SMA (Simple Moving Average)
```python
def calculate_sma(df, column="price", periods=20):
    """Trung bình giá đơn giản trong N periods"""
    window = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-(periods-1), 0)
    return df.withColumn(f"sma_{periods}", F.avg(column).over(window))
```

### EMA (Exponential Moving Average)
```python
# EMA cần tính iterative — dùng Pandas UDF
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def calculate_ema(prices: pd.Series) -> pd.Series:
    """EMA 12 periods — gần đây có trọng số cao hơn"""
    return prices.ewm(span=12, adjust=False).mean()
```

### RSI (Relative Strength Index)
```python
@pandas_udf("double")
def calculate_rsi(prices: pd.Series) -> pd.Series:
    """RSI 14 periods — đo mức quá mua/quá bán (0-100)"""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))
```

### VWAP (Volume Weighted Average Price)
```python
def calculate_vwap(df):
    """Giá trung bình có trọng số theo volume"""
    window = Window.partitionBy("symbol", F.to_date("event_time")).orderBy("event_time").rowsBetween(Window.unboundedPreceding, 0)
    return df.withColumn("vwap",
        (F.sum(F.col("price") * F.col("volume")).over(window)) /
        (F.sum("volume").over(window))
    )
```

---

## Troubleshooting

| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| `OutOfMemoryError` | Worker RAM không đủ | Tăng `SPARK_WORKER_MEMORY` hoặc giảm data |
| `KafkaSourceException` | Kafka chưa ready | Đợi Kafka healthcheck pass |
| Dữ liệu trống | Watermark quá chặt | Tăng watermark duration |
| Spark UI không truy cập | Port conflict | Check port 8081 |

---

## Checklist Phase 2

```
□ Spark Master + Worker chạy trên Docker
□ Spark UI accessible tại http://localhost:8081
□ Quick Demo: đọc CSV + tính SMA thành công
□ Spark đọc được từ Kafka topics
□ Tính SMA, EMA hoạt động
□ Tính RSI, VWAP hoạt động
□ Output stream chạy liên tục
□ Learning-log: Spark concepts
```
