import os
import sys
from urllib.parse import urlparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from pyspark.sql.window import Window

# Đảm bảo mã hóa ký tự UTF-8 được kích hoạt trên hệ điều hành Windows để hiển thị tiếng Việt không bị lỗi font
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# =====================================================================
# 1. ĐỌC CẤU HÌNH KẾT NỐI TIMESCALEDB (Lưu trữ Metadata Catalog cho Iceberg)
# =====================================================================
# Lấy chuỗi kết nối TimescaleDB từ biến môi trường .env (mặc định trỏ về container local)
_TIMESCALE_CONN = os.environ.get(
    "TIMESCALE_CONN",
    "postgresql://postgres:postgres@timescaledb:5432/tradestream"
)

# Phân tích cú pháp chuỗi kết nối để lấy ra User, Password, Host, Port, Database Name
# Xử lý cả hai trường hợp: chuỗi định dạng PostgreSQL URI (postgresql://...) hoặc định dạng libpq (key=value)
if "postgresql://" not in _TIMESCALE_CONN:
    # Trường hợp định dạng libpq: "host=timescaledb port=5432 dbname=tradestream..."
    pairs = dict(item.split("=") for item in _TIMESCALE_CONN.split() if "=" in item)
    DB_USER = pairs.get("user", "postgres")
    DB_PASS = pairs.get("password", "postgres")
    DB_HOST = pairs.get("host", "timescaledb")
    DB_PORT = int(pairs.get("port", 5432))
    DB_NAME = pairs.get("dbname", "tradestream")
else:
    # Trường hợp định dạng URI: "postgresql://user:pass@host:port/dbname"
    _parsed = urlparse(_TIMESCALE_CONN)
    DB_USER = _parsed.username or "postgres"
    DB_PASS = _parsed.password or "postgres"
    DB_HOST = _parsed.hostname or "timescaledb"
    DB_PORT = _parsed.port or 5432
    DB_NAME = (_parsed.path or "/tradestream").lstrip("/")

# Tạo chuỗi JDBC URL tiêu chuẩn mà Spark sử dụng để kết nối tới PostgreSQL
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# =====================================================================
# 2. CẤU HÌNH MINIO (HỒ CHỨA DỮ LIỆU) & SPARK PACKAGES
# =====================================================================
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_PASS = os.environ.get("MINIO_ROOT_PASSWORD", "minioadminpassword")
MINIO_BUCKET = os.environ.get("MINIO_LAKEHOUSE_BUCKET", "lakehouse")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")

# Các JAR packages cần thiết cho Spark (Kafka connector, Iceberg runtime, Postgres JDBC Driver, AWS S3 SDK)
SPARK_PACKAGES = os.environ.get(
    "SPARK_PACKAGES",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

def get_bronze_schema():
    """
    Định nghĩa cấu trúc dữ liệu (Schema) của dữ liệu thô từ Yahoo Finance.
    Vì dữ liệu lưu ở Bronze là JSON thô, Spark cần schema này để ép kiểu dữ liệu chính xác (Schema Enforcement).
    Cấu trúc JSON này chứa:
      - metadata: symbol, name, loại tài sản...
      - raw_data: danh sách mảng timestamp và các mảng giá tương ứng (open, high, low, close, volume).
    """
    # Mảng giá trị số thực đại diện cho các mức giá trong chuỗi thời gian
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

    # Cấu trúc thời gian và chỉ số chính
    raw_data_schema = StructType([
        StructField("meta", StructType([
            StructField("currency", StringType(), True)
        ]), True),
        StructField("timestamp", ArrayType(LongType()), True),
        StructField("indicators", indicators_schema, True),
    ])

    # Metadata đi kèm bản ghi
    metadata_schema = StructType([
        StructField("symbol",          StringType(), True),
        StructField("name",            StringType(), True),
        StructField("asset_type",      StringType(), True),
        StructField("fetch_timestamp", StringType(), True),
    ])

    # Schema tổng thể của 1 bản ghi thô (Bronze record)
    return StructType([
        StructField("metadata", metadata_schema, True),
        StructField("raw_data", raw_data_schema,  True),
    ])

def main():
    # =====================================================================
    # 3. KHỞI TẠO SPARK SESSION VỚI CẤU HÌNH APACHE ICEBERG CATALOG
    # =====================================================================
    spark = (
        SparkSession.builder
        .appName("BronzeToSilverTransformation")
        .master(SPARK_MASTER)
        .config("spark.jars.packages", SPARK_PACKAGES)
        
        # Kích hoạt các phần mở rộng SQL của Apache Iceberg (phục vụ MERGE INTO, UPDATE, DELETE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        # Khai báo Catalog có tên là 'lakehouse' sử dụng lớp Catalog JDBC của Iceberg
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "jdbc")
        .config("spark.sql.catalog.lakehouse.uri", JDBC_URL)
        .config("spark.sql.catalog.lakehouse.jdbc.user", DB_USER)
        .config("spark.sql.catalog.lakehouse.jdbc.password", DB_PASS)
        
        # Đường dẫn thư mục kho (Warehouse) trên MinIO nơi lưu trữ các file Parquet thực tế
        .config("spark.sql.catalog.lakehouse.warehouse", f"s3a://{MINIO_BUCKET}/warehouse")
        
        # Sử dụng Hadoop FileIO để Iceberg ghi trực tiếp xuống Object Storage (MinIO)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        
        # Cấu hình các thông số kết nối S3A Client tương thích với API của MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true") # Bắt buộc phải có khi chạy MinIO tự host
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    # Đặt mức ghi log là WARN để ẩn các tin nhắn INFO thừa thãi, giúp console sạch sẽ hơn
    spark.sparkContext.setLogLevel("WARN")
    print("[*] Khởi động Spark Job: Chuyển đổi dữ liệu từ Bronze sang Silver...")

    try:
        # =====================================================================
        # 4. ĐỌC DỮ LIỆU TỪ HỒ CHỨA THÔ (Bronze Layer - Định dạng JSON)
        # =====================================================================
        bronze_path = f"s3a://{MINIO_BUCKET}/bronze/raw_daily_prices"
        print(f"[*] Bước 1: Đọc tệp JSON thô từ Bronze Layer: {bronze_path}")
        
        raw_df = spark.read.json(bronze_path)
        
        # Nếu thư mục Bronze trống trơn (chưa có producer nào đẩy dữ liệu vào), tắt job
        if "value" not in raw_df.columns:
            print("[NOTE] Tầng Bronze chưa có dữ liệu nào. Đang thoát job...")
            return

        # =====================================================================
        # 5. PARSE DỮ LIỆU JSON ĐỘNG THEO SCHEMA ĐÃ ĐỊNH NGHĨA
        # =====================================================================
        # Cột 'value' trong Kafka chứa chuỗi JSON thô, ép kiểu nó sang schema có cấu trúc
        parsed_df = raw_df.withColumn("data", F.from_json(F.col("value"), get_bronze_schema())).select("data.*")

        # =====================================================================
        # 6. DÀN PHẲNG (FLATTEN) CÁC MẢNG CHUỖI THỜI GIAN
        # =====================================================================
        # Yahoo Finance trả về 1 dòng duy nhất chứa các mảng song song: timestamp[], open[], close[]...
        # Ta cần "nén" chúng lại theo từng cặp tương ứng bằng arrays_zip, rồi "nổ" (explode) ra thành nhiều dòng
        print("[*] Bước 2: Tiến hành dàn phẳng (flatten & explode) các mảng dữ liệu thời gian...")
        
        # Trích xuất các mảng cần zip từ cấu trúc dữ liệu phức tạp
        flat_arrays_df = parsed_df.select(
            "metadata",
            F.col("raw_data.timestamp").alias("ts"),
            F.expr("raw_data.indicators.quote[0].open").alias("op"),
            F.expr("raw_data.indicators.quote[0].high").alias("hi"),
            F.expr("raw_data.indicators.quote[0].low").alias("lo"),
            F.expr("raw_data.indicators.quote[0].close").alias("cl"),
            F.expr("raw_data.indicators.quote[0].volume").alias("vol")
        )

        # arrays_zip sẽ ghép nối các mảng theo chỉ mục: (ts[0], op[0]...) thành zipped[0]
        zipped_df = flat_arrays_df.withColumn(
            "zipped",
            F.arrays_zip("ts", "op", "hi", "lo", "cl", "vol")
        )

        # explode sẽ biến mỗi phần tử trong mảng zipped thành 1 hàng (row) riêng biệt trong bảng dữ liệu
        exploded_df = zipped_df.withColumn("tmp", F.explode("zipped")).select(
            F.col("metadata.symbol").alias("symbol"),
            F.to_date(F.from_unixtime(F.col("tmp.ts"))).alias("fetch_date"), # Đổi từ Epoch Unix timestamp sang định dạng ngày DateType
            F.col("tmp.op").alias("open_price"),
            F.col("tmp.hi").alias("high_price"),
            F.col("tmp.lo").alias("low_price"),
            F.col("tmp.cl").alias("close_price"),
            F.col("tmp.vol").alias("volume")
        )

        # Loại bỏ các bản ghi trùng lặp trên cùng một cặp mã tài sản và ngày (tránh trùng dữ liệu)
        dedup_df = exploded_df.dropDuplicates(["symbol", "fetch_date"])

        # =====================================================================
        # 7. TÍNH TOÁN CÁC CHỈ BÁO KỸ THUẬT BẰNG SPARK WINDOW FUNCTIONS
        # =====================================================================
        # Để tính Daily Return (Giá đóng cửa hôm nay so với hôm qua), ta sử dụng Window Function.
        # Phân vùng dữ liệu theo từng symbol cụ thể và sắp xếp theo ngày tăng dần.
        print("[*] Bước 3: Áp dụng Window Functions để tính Daily Return và Price Range...")
        window_spec = Window.partitionBy("symbol").orderBy("fetch_date")

        processed_df = (
            dedup_df
            # Lấy giá trị đóng cửa của ngày hôm trước (LAG 1 ngày) dựa trên cửa sổ Window đã khai báo
            .withColumn("prev_close", F.lag("close_price", 1).over(window_spec))
            # Tính Daily Return: ((Close_hôm_nay - Close_hôm_qua) / Close_hôm_qua) * 100
            .withColumn(
                "daily_return",
                F.when(F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
                       ((F.col("close_price") - F.col("prev_close")) / F.col("prev_close")) * 100
                ).otherwise(0.0)
            )
            # Tính Price Range (Biến động biên độ giá trong ngày): ((High - Low) / Low) * 100
            .withColumn(
                "price_range",
                F.when(F.col("low_price") > 0,
                       ((F.col("high_price") - F.col("low_price")) / F.col("low_price")) * 100
                ).otherwise(0.0)
            )
            # Điền các giá trị mặc định 0.0 nếu gặp dữ liệu null để tránh lỗi tính toán hoặc hiển thị
            .na.fill(value=0.0, subset=["open_price", "high_price", "low_price", "close_price", "daily_return", "price_range"])
            .na.fill(value=0, subset=["volume"])
        )

        # =====================================================================
        # 8. MÔ HÌNH HÓA STAR SCHEMA: LIÊN KẾT VỚI BẢNG DIMENSION DATE
        # =====================================================================
        # Đọc bảng dim_date tĩnh từ Apache Iceberg để lấy khoá ngoại 'date_key' (ví dụ: ngày 2026-05-18 -> key 20260518)
        print("[*] Bước 4: JOIN dữ liệu giao dịch với bảng chiều thời gian dim_date...")
        dim_date_df = spark.read.table("lakehouse.trading.dim_date")

        # INNER JOIN dựa trên ngày thực tế (full_date của dim_date tương ứng với fetch_date của giao dịch)
        final_df = (
            processed_df.join(
                dim_date_df,
                F.to_date(dim_date_df.full_date) == processed_df.fetch_date,
                "inner"
            )
            # Chọn lọc các trường cụ thể để đẩy vào bảng Fact
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
        # 9. GHI DỮ LIỆU AN TOÀN VÀO BẢNG FACT ICEBERG SỬ DỤNG SQL MERGE INTO
        # =====================================================================
        # Chuyển đổi DataFrame thành một bảng tạm thời trong bộ nhớ Spark để viết truy vấn SQL
        print("[*] Bước 5: Thực hiện MERGE INTO (Upsert) dữ liệu vào bảng fact_daily_prices...")
        final_df.createOrReplaceTempView("temp_silver_prices")

        # Sử dụng lệnh MERGE INTO (chỉ hỗ trợ trên các định dạng nâng cao như Iceberg/Delta)
        # So khớp dựa trên Symbol và Date_Key:
        #  - Nếu đã tồn tại dòng dữ liệu đó: thực hiện UPDATE để cập nhật các chỉ số mới nhất (đảm bảo tính Idempotency)
        #  - Nếu chưa tồn tại dòng dữ liệu đó: thực hiện INSERT để chèn bản ghi mới
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

        # Kiểm chứng số lượng dòng dữ liệu thực tế đã được ghi vào bảng Silver Iceberg
        silver_count = spark.sql("SELECT COUNT(*) FROM lakehouse.trading.fact_daily_prices").collect()[0][0]
        print(f"[SUCCESS] Chuyển đổi dữ liệu sang Silver thành công! Số lượng bản ghi hiện tại trong fact_daily_prices: {silver_count}")

    except Exception as e:
        print(f"[X] Gặp lỗi trong quá trình xử lý chuyển đổi Bronze sang Silver: {e}")
    finally:
        # Luôn luôn đóng SparkSession khi kết thúc để giải phóng tài nguyên CPU/RAM hệ thống
        spark.stop()

if __name__ == "__main__":
    main()

