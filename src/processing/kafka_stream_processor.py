"""
=============================================================
PHASE 2 — DEEP BUILD: Spark Structured Streaming từ Kafka
=============================================================
Nhiệm vụ:
  - Đọc stream LIVE từ Kafka topics: crypto_trades, stock_trades
  - Tính các chỉ số kỹ thuật trong cửa sổ thời gian (time window):
      * SMA  (Simple Moving Average)    - Trung bình giá
      * VWAP (Volume Weighted Avg Price) - Trung bình có trọng số theo KL
  - In kết quả ra console mỗi 10 giây

Cách chạy:
  1. Đảm bảo Kafka đang chạy: docker compose --profile core up -d
  2. Chạy producer (terminal khác): python src/producers/crypto_producer.py
  3. Chạy script này: python src/processing/kafka_stream_processor.py
=============================================================
"""
import sys
import os

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Fix lỗi HADOOP_HOME trên Windows — Spark cần winutils.exe + hadoop.dll
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_hadoop_home  = os.path.join(_project_root, "tools", "hadoop")
_hadoop_bin   = os.path.join(_hadoop_home, "bin")
os.environ["HADOOP_HOME"]           = _hadoop_home
os.environ["PYSPARK_PYTHON"]        = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
# Thêm bin vào PATH để JVM tìm được hadoop.dll
if _hadoop_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, avg, sum as spark_sum,
    window, round as spark_round, count,
    to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType
)
import psycopg2
from psycopg2.extras import execute_values


# ============================
# CẤU HÌNH
# ============================
KAFKA_BROKER = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
CRYPTO_TOPIC = "crypto_trades"
STOCK_TOPIC  = "stock_trades"

# Cấu hình TimescaleDB
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "tradestream"
DB_USER = "postgres"
DB_PASS = "postgres"

# Cửa sổ thời gian để tính chỉ số (5 phút)
WINDOW_DURATION  = "5 minutes"
SLIDE_DURATION   = "1 minute"   # Cập nhật mỗi 1 phút

print("=" * 60)
print("  TRADESTREAM — Spark Kafka Stream Processor")
print("=" * 60)
print(f"  Kafka Broker : {KAFKA_BROKER}")
print(f"  Topics       : {CRYPTO_TOPIC}, {STOCK_TOPIC}")
print(f"  Window       : {WINDOW_DURATION} (slide: {SLIDE_DURATION})")
print("=" * 60)


# ============================
# BƯỚC 1: Khởi động Spark
# ============================
# packages: spark-sql-kafka là JAR connector để Spark "nói chuyện" với Kafka
# Khi khởi động, Spark sẽ tự tải JAR này về nếu chưa có
print("\n[1/4] Khởi động Spark Session...")

spark = (
    SparkSession.builder
    .appName("TradeStream-Kafka-Processor")
    .master("local[2]")   # 2 thread: 1 nhận data, 1 xử lý
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3")
    .config("spark.sql.shuffle.partitions", "2") # Số phân vùng để xử lý dữ liệu
    .config("spark.driver.memory", "1g") # RAM cấp cho Spark
    .config("spark.sql.streaming.checkpointLocation", "data/checkpoints") # Spark lưu trạng thái xử lý vào đây để khi restart ko mất dữ liệu 
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("  -> Spark ready!")


# ============================
# BƯỚC 2: Định nghĩa Schema
# ============================
# Kafka gửi data dưới dạng bytes, ta cần "bản vẽ" để parse JSON
print("[2/4] Định nghĩa Schema dữ liệu...")

# Schema cho Crypto (Binance WebSocket)
crypto_schema = StructType([
    StructField("symbol",     StringType(), True),
    StructField("price",      DoubleType(), True),
    StructField("quantity",   DoubleType(), True),
    StructField("trade_time", LongType(),   True),   # Unix timestamp ms
])

# Schema cho Stock (Yahoo Finance)
stock_schema = StructType([
    StructField("symbol",     StringType(), True),
    StructField("price",      DoubleType(), True),
    StructField("trade_time", LongType(),   True),
])
print("  -> Schema ready!")


# ============================
# BƯỚC 3: Đọc từ Kafka (Structured Streaming)
# ============================
print("[3/4] Kết nối Kafka...")

# Đọc raw bytes từ Kafka
# "subscribe": tên topic muốn lắng nghe
# "startingOffsets": "latest" = chỉ đọc tin nhắn MỚI (bỏ qua tin cũ)
def read_from_kafka(topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "latest") # Chỉ đọc tin mới nhất
        .option("failOnDataLoss", "false") # Cho phép bỏ qua nếu lỗi
        .load()
    )

# Parse bytes → JSON → DataFrame có các cột rõ ràng
def parse_kafka_stream(raw_df, schema):
    return (
        raw_df
        .select(
            # Kafka value là bytes, ép sang string rồi parse JSON
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_ts")  # Thời điểm Kafka nhận tin
        )
        .select("data.*", "kafka_ts")  # Bung các fields của JSON ra
        .withColumn(
            "event_time",
            # Chuyển Unix ms → Spark Timestamp để dùng Window function
            to_timestamp(col("trade_time") / 1000)
        )
    )

crypto_raw = read_from_kafka(CRYPTO_TOPIC)
stock_raw  = read_from_kafka(STOCK_TOPIC)

crypto_df = parse_kafka_stream(crypto_raw, crypto_schema)
stock_df  = parse_kafka_stream(stock_raw,  stock_schema)

print("  -> Kafka streams connected!")


# ============================
# BƯỚC 4: Tính chỉ số kỹ thuật
# ============================
print("[4/4] Thiết lập tính toán chỉ số...")

def compute_indicators(df):
    """
    Tính SMA và VWAP trong cửa sổ thời gian trượt.
    
    - SMA  = avg(price) trong window
    - VWAP = sum(price * quantity) / sum(quantity)
    """
    quantity_col = col("quantity") if "quantity" in df.columns else col("price") * 0 + 1
    
    return (
        df
        .withWatermark("event_time", "2 minutes")  # Bỏ qua dữ liệu trễ > 2 phút
        .groupBy(
            window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
            col("symbol")
        )
        .agg(
            spark_round(avg(col("price")), 2).alias("sma"),
            spark_round(
                spark_sum(col("price") * quantity_col) / spark_sum(quantity_col),
                2
            ).alias("vwap"),
            spark_round(avg(col("price")), 2).alias("avg_price"),
            count("*").alias("trade_count"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sma"),
            col("vwap"),
            col("trade_count"),
        )
    )

crypto_indicators = compute_indicators(crypto_df)
stock_indicators  = compute_indicators(stock_df)
print("  -> Indicators configured!")
def write_to_postgres(df, epoch_id):
    pandas_df = df.toPandas()
    if pandas_df.empty:
        return
    
    print(f"\n--- [TimescaleDB] Ghi Batch {epoch_id} ({len(pandas_df)} dòng) ---")
    print(pandas_df.to_string(index=False))
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cursor = conn.cursor()
        
        upsert_query = """
            INSERT INTO crypto_indicators (symbol, window_start, window_end, sma, vwap, trade_count)
            VALUES %s
            ON CONFLICT (symbol, window_start) 
            DO UPDATE SET 
                window_end = EXCLUDED.window_end,
                sma = EXCLUDED.sma,
                vwap = EXCLUDED.vwap,
                trade_count = EXCLUDED.trade_count;
        """
        
        values = [
            (
                row.symbol, 
                row.window_start, 
                row.window_end, 
                row.sma, 
                row.vwap, 
                row.trade_count
            ) for row in pandas_df.itertuples(index=False)
        ]
        
        execute_values(cursor, upsert_query, values)
        conn.commit()
        cursor.close()
        conn.close()
        print("-> Đã lưu vào Database thành công!")
    except Exception as e:
        print(f"-> [Lỗi Database] Không thể ghi dữ liệu: {e}")

# ============================
# BƯỚC 5: Xuất kết quả ra TimescaleDB & Console
# ============================
print("\n" + "=" * 60)
print("  STREAMING STARTED — Dữ liệu sẽ cập nhật mỗi 10 giây")
print("  Nhấn Ctrl+C để dừng")
print("=" * 60 + "\n")

# outputMode="complete": in lại toàn bộ kết quả mỗi lần cập nhật
# outputMode="update":   chỉ in những dòng mới/thay đổi
query_crypto = (
    crypto_indicators.writeStream
    .outputMode("update")
    .foreachBatch(write_to_postgres)
    .trigger(processingTime="10 seconds")
    .queryName("crypto-indicators-db")
    .start()
)

query_stock = (
    stock_indicators.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("numRows", "10")
    .trigger(processingTime="10 seconds")
    .queryName("stock-indicators")
    .start()
)

# Giữ chương trình chạy cho đến khi Ctrl+C
try:
    query_crypto.awaitTermination()
    query_stock.awaitTermination()
except KeyboardInterrupt:
    print("\n[!] Đang dừng stream xử lý...")
    query_crypto.stop()
    query_stock.stop()
    spark.stop()
    print("[OK] Đã dừng an toàn.")
