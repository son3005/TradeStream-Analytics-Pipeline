import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.utils.spark_helper import get_spark_session

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Đọc cấu hình từ file .env
MINIO_BUCKET = os.environ.get("MINIO_LAKEHOUSE_BUCKET", "lakehouse")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
# Subscribe cùng lúc cả stock_trades và crypto_trades
KAFKA_TOPICS = "stock_trades,crypto_trades"

def main() -> None:
    """Tiêu thụ các giao dịch JSON thô từ Kafka và lưu trữ chúng vào tầng Bronze của MinIO.

    Đăng ký đồng thời các topic cổ phiếu và tiền số, đọc dữ liệu theo các vi lô (micro-batches)
    sử dụng trigger availableNow, và ghi các chuỗi thô vào thư mục Bronze để đảm bảo tính lâu bền.

    Returns:
        None

    Raises:
        Exception: Nếu kết nối tới Kafka broker hoặc thùng chứa (bucket) MinIO thất bại.
    """
    # 1. Khởi tạo Spark Session hỗ trợ đọc ghi S3 (MinIO)
    spark = get_spark_session("RawToBronzeIngestion", enable_iceberg=False)

    spark.sparkContext.setLogLevel("WARN")
    print(f"[*] Starting Kafka to Bronze Ingestion Batch for topics: {KAFKA_TOPICS}")

    try:
        # 2. Đọc luồng dữ liệu từ Kafka
        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", KAFKA_TOPICS)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")            # Không dừng pipeline nếu gặp offset lỗi/bị xóa
            .load()
        )

        # 3. Lấy các trường dữ liệu thô dạng chuỗi (String) từ Kafka
        raw_json_df = kafka_df.selectExpr(
            "CAST(key AS STRING) as key",
            "CAST(value AS STRING) as value",
            "partition",
            "offset",
            "timestamp"
        )

        # Đường dẫn thư mục Bronze trên MinIO
        bronze_path = f"s3a://{MINIO_BUCKET}/bronze/raw_trades"
        checkpoint_path = f"s3a://{MINIO_BUCKET}/checkpoints/raw_to_bronze"

        # 4. Ghi luồng dữ liệu dạng JSON thô lên MinIO
        print(f"[*] Writing raw JSON streams to Bronze: {bronze_path}")
        query = (
            raw_json_df.writeStream
            .format("json") # Định dạng lưu trữ
            .option("path", bronze_path)
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True) # Xử lý hết tin nhắn rồi tự động tắt (Micro-batch)
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
