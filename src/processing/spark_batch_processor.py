import os
import sys
from urllib.parse import urlparse

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, expr
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, ArrayType
)

# ============================================================
# CẤU HÌNH
# ============================================================

# Parse connection string: postgresql://user:pass@host:port/dbname
_TIMESCALE_CONN = os.environ.get(
    "TIMESCALE_CONN",
    "postgresql://postgres:postgres@timescaledb:5432/tradestream"
)
_parsed = urlparse(_TIMESCALE_CONN)
DB_USER = _parsed.username or "postgres"
DB_PASS = _parsed.password or "postgres"
DB_HOST = _parsed.hostname or "timescaledb"
DB_PORT = _parsed.port or 5432
DB_NAME = (_parsed.path or "/tradestream").lstrip("/")

# JDBC URL không chứa credentials (JDBC driver cần nhận riêng qua .option)
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = "raw_daily_prices"


# ============================================================
# KHỞI TẠO SPARK SESSION
# ============================================================

def create_spark_session():
    """
    Tạo SparkSession kết nối tới Spark Master cluster.
    - spark-sql-kafka: để đọc dữ liệu từ Kafka
    - postgresql JDBC driver: để ghi dữ liệu vào TimescaleDB
    Cả hai đều được tải qua Maven (không cần pip install).
    """
    return (
        SparkSession.builder
        .appName("DailyBatchSparkProcessor")
        .master("spark://spark-master:7077")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "org.postgresql:postgresql:42.6.0"
        )
        .getOrCreate()
    )


# ============================================================
# ĐỊNH NGHĨA SCHEMA JSON TỪ KAFKA
# ============================================================

def get_schema():
    """
    Cấu trúc JSON mà Python Producer đẩy vào Kafka.
    Phải khớp chính xác với payload trong yahoo_batch_producer.py.
    """
    quote_schema = StructType([
        StructField("open",   ArrayType(DoubleType()), True),
        StructField("high",   ArrayType(DoubleType()), True),
        StructField("low",    ArrayType(DoubleType()), True),
        StructField("close",  ArrayType(DoubleType()), True),
        StructField("volume", ArrayType(LongType()),   True),
    ])

    indicators_schema = StructType([
        StructField("quote", ArrayType(quote_schema), True)
    ])

    raw_data_schema = StructType([
        StructField("meta", StructType([
            StructField("currency", StringType(), True)
        ]), True),
        StructField("indicators", indicators_schema, True),
    ])

    metadata_schema = StructType([
        StructField("symbol",          StringType(), True),
        StructField("name",            StringType(), True),
        StructField("asset_type",      StringType(), True),
        StructField("fetch_timestamp", StringType(), True),
    ])

    return StructType([
        StructField("metadata", metadata_schema, True),
        StructField("raw_data", raw_data_schema,  True),
    ])


# ============================================================
# XỬ LÝ TỪNG MICRO-BATCH
# ============================================================

def process_batch(df, epoch_id):
    """
    Gọi bởi writeStream.foreachBatch().
    - Tính Daily Return và Price Range.
    - Ghi kết quả vào TimescaleDB qua Spark JDBC (không cần psycopg2).

    Tại sao dùng Spark JDBC thay vì psycopg2?
    -> JDBC driver PostgreSQL đã có sẵn qua Maven package khai báo
       trong create_spark_session(), không cần pip install gì thêm.
    -> Tránh lỗi quyền khi cài thư viện trong Spark container.
    """
    count = df.count()
    print(f"[Batch {epoch_id}] Dang xu ly {count} ban ghi...")
    if count == 0:
        return

    # --- Tính toán chỉ số ---
    processed_df = (
        df
        .withColumn("daily_return",
            when(col("open_price") > 0,
                ((col("close_price") - col("open_price")) / col("open_price")) * 100
            ).otherwise(0)
        )
        .withColumn("price_range",
            when(col("low_price") > 0,
                ((col("high_price") - col("low_price")) / col("low_price")) * 100
            ).otherwise(0)
        )
    )

    processed_df.select("symbol", "close_price", "daily_return", "price_range").show(truncate=False)

    cols_to_write = [
        "symbol", "fetch_date", "open_price", "high_price",
        "low_price", "close_price", "volume", "currency", "asset_type"
    ]

    # --- UPSERT: Delete-then-Insert ---
    # DriverManager không thấy JAR từ --jars (dùng system classloader riêng).
    # Fix: Dùng org.postgresql.Driver().connect() trực tiếp - cùng classloader với Spark.
    try:
        # Bước 1: Kết nối DB qua org.postgresql.Driver (py4j) và DELETE records hôm nay
        jvm = df.sparkSession.sparkContext._jvm
        props = jvm.java.util.Properties()
        props.setProperty("user", DB_USER)
        props.setProperty("password", DB_PASS)
        driver = jvm.org.postgresql.Driver()
        conn = driver.connect(JDBC_URL, props)
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        deleted = stmt.executeUpdate("DELETE FROM daily_prices WHERE fetch_date = CURRENT_DATE")
        conn.commit()
        stmt.close()
        conn.close()
        print(f"[Batch {epoch_id}] Da xoa {deleted} ban ghi cu (CURRENT_DATE).")

        # Bước 2: INSERT fresh data
        (
            processed_df.select(cols_to_write)
            .write
            .format("jdbc")
            .option("url",      JDBC_URL)
            .option("dbtable",  "daily_prices")
            .option("user",     DB_USER)
            .option("password", DB_PASS)
            .option("driver",   "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        print(f"[Batch {epoch_id}] Da ghi thanh cong {count} ban ghi vao TimescaleDB.")
    except Exception as e:
        print(f"[Batch {epoch_id}] Loi ghi Database: {e}")




# ============================================================
# HÀM CHÍNH
# ============================================================

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Bat dau doc du lieu tu Kafka ({KAFKA_BROKER}, topic: {KAFKA_TOPIC})...")

    # 1. Đọc luồng từ Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # 2. Parse JSON (value bytes -> struct)
    json_df = (
        raw_df
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), get_schema()).alias("data"))
        .select("data.*")
    )

    # 3. Flatten các trường lồng nhau, lấy phần tử đầu tiên của mảng
    flat_df = (
        json_df.select(
            col("metadata.symbol").alias("symbol"),
            col("metadata.asset_type").alias("asset_type"),
            col("raw_data.meta.currency").alias("currency"),
            expr("raw_data.indicators.quote[0].open[0]").alias("open_price"),
            expr("raw_data.indicators.quote[0].high[0]").alias("high_price"),
            expr("raw_data.indicators.quote[0].low[0]").alias("low_price"),
            expr("raw_data.indicators.quote[0].close[0]").alias("close_price"),
            expr("raw_data.indicators.quote[0].volume[0]").alias("volume"),
        )
        .withColumn("fetch_date", expr("to_date(current_timestamp())"))
        # Thay thế null bằng 0 để tránh lỗi khi ghi DB
        .na.fill(value=0, subset=["open_price", "high_price", "low_price", "close_price", "volume"])
    )

    # 4. Kích hoạt Streaming với trigger AvailableNow
    # -> Xử lý hết message hiện có trong Kafka rồi TỰ DỪNG (phù hợp cho Batch)
    query = (
        flat_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", "/tmp/spark_checkpoints/daily_prices")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print("Hoan thanh Spark Batch Job!")


if __name__ == "__main__":
    main()
