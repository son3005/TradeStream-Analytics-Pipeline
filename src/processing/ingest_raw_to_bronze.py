import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Đọc cấu hình từ file .env
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_PASS = os.environ.get("MINIO_ROOT_PASSWORD", "minioadminpassword")
MINIO_BUCKET = os.environ.get("MINIO_LAKEHOUSE_BUCKET", "lakehouse")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = "raw_daily_prices"
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
SPARK_PACKAGES = os.environ.get(
    "SPARK_PACKAGES",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

def main():
    # 1. Khởi tạo Spark Session hỗ trợ đọc ghi S3 (MinIO)
    spark = (
        SparkSession.builder
        .appName("RawToBronzeIngestion")
        .master(SPARK_MASTER)
        # Khai báo các gói JAR cần thiết cho Kafka và Hadoop AWS S3
        .config("spark.jars.packages", SPARK_PACKAGES)
        # Cấu hình kết nối MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("[*] Starting Kafka to Bronze Ingestion Batch...")

    try:
        # 2. Đọc luồng dữ liệu từ Kafka
        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )

        # 3. Lấy các trường dữ liệu thô dạng chuỗi (String) từ Kafka
        # Lưu ý: Kafka trả về key và value ở dạng nhị phân (binary), cần ép kiểu sang STRING
        raw_json_df = kafka_df.selectExpr(
            "CAST(key AS STRING) as key",
            "CAST(value AS STRING) as value",
            "partition",
            "offset",
            "timestamp"
        )

        # Đường dẫn thư mục Bronze trên MinIO
        bronze_path = f"s3a://{MINIO_BUCKET}/bronze/raw_daily_prices"
        checkpoint_path = f"s3a://{MINIO_BUCKET}/checkpoints/raw_to_bronze"

        # 4. Ghi luồng dữ liệu dạng JSON thô lên MinIO
        print(f"[*] Writing raw JSON streams to Bronze: {bronze_path}")
        query = (
            raw_json_df.writeStream
            .format("json") # Định dạng lưu trữ (json/parquet/csv)
            .option("path", bronze_path)
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True) # Xử lý hết tin nhắn rồi tự động tắt
            .start()
        )

        query.awaitTermination()
        print("[SUCCESS] Bronze Ingestion completed successfully!")

    except Exception as e:
        print(f"[X] Error in Bronze Ingestion: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
