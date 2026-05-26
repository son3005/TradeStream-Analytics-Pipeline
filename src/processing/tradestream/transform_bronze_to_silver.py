import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.window import Window
from src.utils.spark_helper import get_spark_session

# Đảm bảo mã hóa ký tự UTF-8 được kích hoạt trên Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Cấu hình MinIO
MINIO_BUCKET: str = os.environ.get("MINIO_LAKEHOUSE_BUCKET", "lakehouse")

def get_tick_schema() -> StructType:
    """Định nghĩa cấu trúc schema PySpark để phân tích các giao dịch thô dạng JSON.

    Returns:
        StructType: Đối tượng mô tả schema của PySpark.
    """
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("trade_time", LongType(), True)
    ])

def main() -> None:
    """Biến đổi dữ liệu thô từ tầng Bronze sang nến ngày tầng Silver (Apache Iceberg).

    Đọc dữ liệu ticks thô dạng JSON trong Bronze Layer, lọc các dòng dữ liệu không hợp lệ,
    gom nhóm dữ liệu ticks theo ngày để tính các chỉ số nến ngày OHLCV (Open, High, Low, 
    Close, Volume), tính chỉ báo kỹ thuật ngày (Daily Return, Price Range) bằng Window 
    Functions, JOIN với dim_date và lưu vào bảng fact_daily_prices bằng cơ chế MERGE INTO.

    Returns:
        None

    Raises:
        Exception: Nếu lỗi đọc/ghi dữ liệu hoặc tính toán chỉ báo chuỗi thời gian.
    """
    # =====================================================================
    # 3. KHỞI TẠO SPARK SESSION VỚI CẤU HÌNH APACHE ICEBERG CATALOG
    # =====================================================================
    spark = get_spark_session("BronzeToSilverTransformation")

    spark.sparkContext.setLogLevel("WARN")
    print("[*] Khởi động Spark Job: Chuyển đổi dữ liệu thô (Tick) từ Bronze sang Silver...")

    try:
        # =====================================================================
        # 4. ĐỌC DỮ LIỆU TỪ HỒ CHỨA THÔ (Bronze Layer - JSON)
        # =====================================================================
        bronze_path = f"s3a://{MINIO_BUCKET}/bronze/raw_trades"
        print(f"[*] Bước 1: Đọc tệp JSON thô từ Bronze Layer: {bronze_path}")
        
        raw_df = spark.read.json(bronze_path)
        
        # Nếu thư mục Bronze trống trơn, dừng job
        if "value" not in raw_df.columns:
            print("[NOTE] Tầng Bronze chưa có dữ liệu giao dịch nào. Đang thoát job...")
            return

        # =====================================================================
        # 5. PARSE VÀ CHUẨN HÓA DỮ LIỆU TỪ KAFKA PAYLOAD
        # =====================================================================
        parsed_df = (
            raw_df
            .withColumn("data", F.from_json(F.col("value"), get_tick_schema()))
            .select("data.*")
            .filter(col("symbol").isNotNull())
        )

        # =====================================================================
        # 6. GOM NHÓM VÀ TÍNH TOÁN DAILY OHLCV BẰNG WINDOW FUNCTIONS
        # =====================================================================
        print("[*] Bước 2: Gom nhóm Ticks thành dữ liệu nến ngày (Daily OHLCV)...")
        
        # Chuyển đổi trade_time (ms) sang Định dạng ngày DateType
        df_with_date = parsed_df.withColumn(
            "fetch_date", 
            F.to_date(F.from_unixtime(F.col("trade_time") / 1000))
        )
        
        # Định nghĩa Window để lấy giá Open và Close chính xác theo thời gian giao dịch
        window_asc = Window.partitionBy("symbol", "fetch_date").orderBy("trade_time")
        window_desc = Window.partitionBy("symbol", "fetch_date").orderBy(F.col("trade_time").desc())
        
        # Thêm các cột Open/Close tạm thời cho mỗi giao dịch dựa trên window
        df_ohlcv_temp = (
            df_with_date
            .withColumn("first_price", F.first("price").over(window_asc))
            .withColumn("last_price", F.first("price").over(window_desc))
        )
        
        # Gom nhóm theo mã tài sản và ngày để tính toán các chỉ số OHLCV thực tế
        dedup_df = (
            df_ohlcv_temp
            .groupBy("symbol", "fetch_date")
            .agg(
                F.first("first_price").alias("open_price"),
                F.max("price").alias("high_price"),
                F.min("price").alias("low_price"),
                F.first("last_price").alias("close_price"),
                F.coalesce(F.sum("quantity").cast(LongType()), F.lit(0)).alias("volume")
            )
        )

        # =====================================================================
        # 7. TÍNH TOÁN CHỈ BÁO KỸ THUẬT (Daily Return & Price Range)
        # =====================================================================
        print("[*] Bước 3: Áp dụng Window Functions để tính Daily Return và Price Range...")
        window_spec = Window.partitionBy("symbol").orderBy("fetch_date")

        processed_df = (
            dedup_df
            .withColumn("prev_close", F.lag("close_price", 1).over(window_spec))
            .withColumn(
                "daily_return",
                F.when(F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
                       ((F.col("close_price") - F.col("prev_close")) / F.col("prev_close")) * 100
                ).otherwise(0.0)
            )
            .withColumn(
                "price_range",
                F.when(F.col("low_price") > 0,
                       ((F.col("high_price") - F.col("low_price")) / F.col("low_price")) * 100
                ).otherwise(0.0)
            )
            .na.fill(value=0.0, subset=["open_price", "high_price", "low_price", "close_price", "daily_return", "price_range"])
            .na.fill(value=0, subset=["volume"])
        )

        # =====================================================================
        # 8. LIÊN KẾT VỚI BẢNG DIMENSION DATE (STAR SCHEMA)
        # =====================================================================
        print("[*] Bước 4: JOIN với bảng chiều thời gian dim_date...")
        dim_date_df = spark.read.table("lakehouse.trading.dim_date")

        final_df = (
            processed_df.join(
                dim_date_df,
                F.to_date(dim_date_df.full_date) == processed_df.fetch_date,
                "inner"
            )
            .select(
                processed_df.symbol,
                dim_date_df.date_key,
                processed_df.open_price,
                processed_df.high_price,
                processed_df.low_price,
                processed_df.close_price,
                processed_df.volume,
                processed_df.daily_return,
                processed_df.price_range
            )
        )

        # =====================================================================
        # 9. GHI UPSERT VÀO BẢNG FACT ICEBERG
        # =====================================================================
        print("[*] Bước 5: Thực hiện MERGE INTO (Upsert) dữ liệu vào bảng fact_daily_prices...")
        final_df.createOrReplaceTempView("temp_silver_prices")

        spark.sql("""
            MERGE INTO lakehouse.trading.fact_daily_prices target
            USING temp_silver_prices source
            ON target.symbol = source.symbol AND target.date_key = source.date_key
            WHEN MATCHED THEN UPDATE SET 
                open_price = source.open_price,
                high_price = source.high_price,
                low_price = source.low_price,
                close_price = source.close_price,
                volume = source.volume,
                daily_return = source.daily_return,
                price_range = source.price_range
            WHEN NOT MATCHED THEN INSERT (
                symbol, date_key, open_price, high_price, low_price, close_price, volume, daily_return, price_range
            ) VALUES (
                source.symbol, source.date_key, source.open_price, source.high_price, source.low_price, source.close_price, source.volume, source.daily_return, source.price_range
            )
        """)

        silver_count = spark.sql("SELECT COUNT(*) FROM lakehouse.trading.fact_daily_prices").collect()[0][0]
        print(f"[SUCCESS] Chuyển đổi dữ liệu sang Silver thành công! Số lượng bản ghi hiện tại trong fact_daily_prices: {silver_count}")

    except Exception as e:
        print(f"[X] Gặp lỗi trong quá trình xử lý chuyển đổi Bronze sang Silver: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    from pyspark.sql.functions import col
    main()
