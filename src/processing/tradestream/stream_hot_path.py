import os
import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from src.utils.spark_helper import get_db_credentials, get_spark_session

# Kich hoat che do ma hoa ky tu UTF-8 cho Windows de logs hien thi tieng Viet chinh xac
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Doc cac cau hinh tu bien moi truong
MINIO_BUCKET: str = os.environ.get("MINIO_LAKEHOUSE_BUCKET", "lakehouse")
KAFKA_BROKER: str = os.environ.get("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPICS: str = "stock_trades,crypto_trades"

def get_tick_schema() -> StructType:
    """
    Dinh nghia cau truc schema cua Spark DataFrame de parse payload JSON tu Kafka.

    Returns:
        StructType: Schema mo ta truong du lieu cua moi tick giao dich.
    """
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("trade_time", LongType(), True)
    ])

def write_to_timescale(batch_df: DataFrame, batch_id: int) -> None:
    """
    Ghi micro-batch hien tai vao database TimescaleDB bang cach su dung
    bang Staging tam thoi va thuc hien native UPSERT (ON CONFLICT DO UPDATE).

    Args:
        batch_df (DataFrame): DataFrame chua ket qua window aggregation cua micro-batch.
        batch_id (int): ID duy nhat cua micro-batch do Spark cung cap.

    Returns:
        None

    Raises:
        Exception: Neu loi mo ket noi JDBC hoac thuc thi native SQL UPSERT.
    """
    # Neu batch_df rong, khong can lam gi
    if batch_df.count() == 0:
        return

    # Lay database credentials va JDBC URL
    db_config = get_db_credentials()
    JDBC_URL = db_config["jdbc_url"]
    DB_USER = db_config["user"]
    DB_PASS = db_config["password"]

    # Dat ten bang staging duy nhat cho micro-batch hien tai de tranh xung dot
    staging_table = f"crypto_indicators_staging_{batch_id}"
    target_table = "crypto_indicators"

    # Giai phang cot struct window lay window_start va window_end
    flat_df = (
        batch_df
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("sma"),
            F.col("vwap"),
            F.col("trade_count")
        )
    )

    try:
        # 1. Ghi du lieu phẳng vao bang staging tam thoi tren Postgres
        (
            flat_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", staging_table)
            .option("user", DB_USER)
            .option("password", DB_PASS)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )

        # 2. Dung JVM connection truc tiep tu Spark Driver de thuc hien native UPSERT
        jvm = batch_df.sparkSession._jvm
        conn = jvm.java.sql.DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASS)

        try:
            stmt = conn.createStatement()

            # Thuc thi cau lenh INSERT ... ON CONFLICT cua Postgres
            # Neu symbol va window_start da ton tai, update sma, vwap va trade_count moi nhat
            upsert_query = f"""
                INSERT INTO {target_table} (symbol, window_start, window_end, sma, vwap, trade_count)
                SELECT symbol, window_start, window_end, sma, vwap, trade_count
                FROM {staging_table}
                ON CONFLICT (symbol, window_start) DO UPDATE SET
                    window_end = EXCLUDED.window_end,
                    sma = EXCLUDED.sma,
                    vwap = EXCLUDED.vwap,
                    trade_count = EXCLUDED.trade_count
            """
            stmt.execute(upsert_query)

            # Xoa bang staging tam thoi
            stmt.execute(f"DROP TABLE IF EXISTS {staging_table}")
            stmt.close()
        finally:
            conn.close()

    except Exception as e:
        print(f"[X] Gặp lỗi khi ghi micro-batch {batch_id} vao TimescaleDB: {e}")

def main() -> None:
    """
    Job Structured Streaming chinh: doc tu Kafka, parse JSON, ap dung Watermark
    va Sliding Window, thuc hien tinh toan SMA & VWAP, roi goi sink foreachBatch.

    Returns:
        None

    Raises:
        Exception: Neu gap loi bat dau luong streaming hoac cau hinh checkpoint.
    """
    # Khởi tạo Spark Session ho tro readStream tu Kafka (enable_iceberg=False vi ta ghi JDBC thang vao TimescaleDB)
    spark = get_spark_session("RealTimeHotPathIndicatorStreaming", enable_iceberg=False)
    spark.sparkContext.setLogLevel("WARN")

    print(f"[*] Starting Real-time Hot Path Streaming Job for topics: {KAFKA_TOPICS}")

    try:
        # 1. Doc luong tu Kafka
        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", KAFKA_TOPICS)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # 2. Parse payload tin nhan JSON tu Kafka va chuyen trade_time sang Timestamp
        parsed_stream = (
            kafka_stream
            .withColumn("value_str", F.col("value").cast(StringType()))
            .withColumn("data", F.from_json(F.col("value_str"), get_tick_schema()))
            .select("data.*")
            .filter(F.col("symbol").isNotNull())
            .withColumn("event_time", F.from_unixtime(F.col("trade_time") / 1000).cast("timestamp"))
        )

        # 3. Thiet lap Watermark 1 phut va Sliding Window 1 phut, trượt 10 giay
        windowed_stream = (
            parsed_stream
            .withWatermark("event_time", "1 minute")
            .groupBy(
                F.col("symbol"),
                F.window(F.col("event_time"), "1 minute", "10 seconds")
            )
            .agg(
                F.avg("price").alias("sma"),
                # Tinh VWAP: sum(price * quantity) / sum(quantity)
                F.when(
                    F.sum("quantity") > 0,
                    F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")
                ).otherwise(F.avg("price")).alias("vwap"),
                F.count("price").alias("trade_count")
            )
        )

        # Duong dan checkpoint luu tru tren MinIO
        checkpoint_path = f"s3a://{MINIO_BUCKET}/checkpoints/hot_path"

        # 4. Ghi luong lien tuc qua foreachBatch vao TimescaleDB
        print(f"[*] Writing real-time indicators to TimescaleDB. Checkpoint: {checkpoint_path}")
        query = (
            windowed_stream.writeStream
            .foreachBatch(write_to_timescale)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="10 seconds")  # Trigger moi 10 giay giong voi buoc truot
            .start()
        )

        query.awaitTermination()

    except Exception as e:
        print(f"[X] Gặp loi trong luồng Spark Structured Streaming: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
