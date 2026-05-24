import os
import sys
from urllib.parse import urlparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Kích hoạt chế độ mã hóa ký tự UTF-8 cho Windows để hiển thị log tiếng Việt chính xác
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# =====================================================================
# 1. ĐỌC CẤU HÌNH KẾT NỐI DB (Cho cả Iceberg Metadata và Target Postgres)
# =====================================================================
_TIMESCALE_CONN = os.environ.get(
    "TIMESCALE_CONN",
    "postgresql://postgres:postgres@timescaledb:5432/tradestream"
)

# Parse thông số kết nối cơ sở dữ liệu
if "postgresql://" not in _TIMESCALE_CONN:
    pairs = dict(item.split("=") for item in _TIMESCALE_CONN.split() if "=" in item)
    DB_USER = pairs.get("user", "postgres")
    DB_PASS = pairs.get("password", "postgres")
    DB_HOST = pairs.get("host", "timescaledb")
    DB_PORT = int(pairs.get("port", 5432))
    DB_NAME = pairs.get("dbname", "tradestream")
else:
    _parsed = urlparse(_TIMESCALE_CONN)
    DB_USER = _parsed.username or "postgres"
    DB_PASS = _parsed.password or "postgres"
    DB_HOST = _parsed.hostname or "timescaledb"
    DB_PORT = _parsed.port or 5432
    DB_NAME = (_parsed.path or "/tradestream").lstrip("/")

# Chuỗi JDBC URL truyền cho Spark JDBC Connector
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# =====================================================================
# 2. CẤU HÌNH KẾT NỐI MINIO S3 & SPARK SESSION
# =====================================================================
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_PASS = os.environ.get("MINIO_ROOT_PASSWORD", "minioadminpassword")
MINIO_BUCKET = os.environ.get("MINIO_LAKEHOUSE_BUCKET", "lakehouse")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
SPARK_PACKAGES = os.environ.get(
    "SPARK_PACKAGES",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

def main():
    # =====================================================================
    # 3. KHỞI TẠO SPARK SESSION VỚI CATALOG LAKEHOUSE ICEBERG
    # =====================================================================
    spark = (
        SparkSession.builder
        .appName("SilverToPostgresSync")
        .master(SPARK_MASTER)
        .config("spark.jars.packages", SPARK_PACKAGES)
        # Tối ưu hiệu năng shuffle cho dữ liệu nhỏ
        .config("spark.sql.shuffle.partitions", "4")
        # Thiết lập các cấu hình cần thiết để kết nối và tương tác với các bảng Apache Iceberg
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "jdbc")
        .config("spark.sql.catalog.lakehouse.uri", JDBC_URL)
        .config("spark.sql.catalog.lakehouse.jdbc.user", DB_USER)
        .config("spark.sql.catalog.lakehouse.jdbc.password", DB_PASS)
        .config("spark.sql.catalog.lakehouse.warehouse", f"s3a://{MINIO_BUCKET}/warehouse")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        # Kết nối Storage MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("[*] Khởi động Spark Job: Đồng bộ dữ liệu từ tầng Silver (Iceberg) sang PostgreSQL...")

    try:
        # =====================================================================
        # 4. ĐỌC DỮ LIỆU TỪ 3 BẢNG ICEBERG Ở TẦNG SILVER
        # =====================================================================
        print("[*] Bước 1: Đọc các bảng Apache Iceberg trong Lakehouse...")
        fact_df = spark.read.table("lakehouse.trading.fact_daily_prices")
        date_df = spark.read.table("lakehouse.trading.dim_date")
        asset_df = spark.read.table("lakehouse.trading.dim_assets")

        # Kiểm tra xem tầng Silver đã có dữ liệu hay chưa
        if fact_df.count() == 0:
            print("[NOTE] Bảng fact_daily_prices hiện không có bản ghi nào. Dừng đồng bộ.")
            return

        # =====================================================================
        # 5. THỰC HIỆN JOIN ĐỂ DÀN PHẲNG DỮ LIỆU (Denormalization)
        # =====================================================================
        # Grafana truy vấn trực tiếp trên một bảng lớn, phẳng sẽ nhanh hơn việc JOIN thời gian thực.
        # Ở đây ta thực hiện JOIN bảng Fact (giao dịch) với bảng chiều Thời gian (Date) và bảng chiều Tài sản (Asset).
        print("[*] Bước 2: Join bảng Fact với bảng Dimension để tạo bảng dữ liệu phẳng...")
        flat_df = (
            fact_df.join(
                date_df,
                fact_df.date_key == date_df.date_key,
                "inner"
            )
            .join(
                asset_df,
                fact_df.symbol == asset_df.symbol,
                "inner"
            )
            # Lựa chọn các cột đầu ra phục vụ biểu đồ Dashboard
            .select(
                fact_df.symbol,
                F.to_date(date_df.full_date).alias("fetch_date"),
                fact_df.open_price,
                fact_df.high_price,
                fact_df.low_price,
                fact_df.close_price,
                fact_df.volume,
                asset_df.currency,
                asset_df.asset_type
            )
        )

        # =====================================================================
        # 6. GHI DỮ LIỆU VÀO BẢNG TẠM THỜI (Staging Table) TRÊN POSTGRESQL
        # =====================================================================
        # Để đảm bảo tính nguyên tử (Atomic) và an toàn dữ liệu, ta không ghi đè trực tiếp lên bảng đích.
        # Đầu tiên ghi dữ liệu phẳng vào bảng 'daily_prices_staging'. Mode ghi đè (overwrite).
        staging_table = "daily_prices_staging"
        target_table = "daily_prices"
        print(f"[*] Bước 3: Ghi dữ liệu phẳng vào bảng staging tạm thời: {staging_table}...")
        
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

        # =====================================================================
        # 7. THỰC THI TRUY VẤN NATIVE UPSERT QUA KẾT NỐI JDBC TRỰC TIẾP (JVM)
        # =====================================================================
        # Dùng đối tượng JVM đi kèm của Spark để tạo kết nối Java JDBC trực tiếp tới Postgres.
        # Điều này cho phép thực thi trực tiếp các câu lệnh SQL Native của Postgres mà PySpark không trực tiếp hỗ trợ.
        print(f"[*] Bước 4: Thực thi truy vấn Upsert (ON CONFLICT) từ bảng Staging sang bảng đích: {target_table}...")
        jvm = spark._jvm
        conn = jvm.java.sql.DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASS)
        
        try:
            stmt = conn.createStatement()
            
            # Sử dụng cú pháp INSERT ... ON CONFLICT DO UPDATE của PostgreSQL
            # Nếu cặp (symbol, fetch_date) đã tồn tại thì cập nhật các chỉ số giá mới nhất
            # Nếu chưa tồn tại thì chèn mới. EXCLUDED đại diện cho dòng dữ liệu mới chuẩn bị ghi từ Staging
            upsert_query = f"""
                INSERT INTO {target_table} (
                    symbol, fetch_date, open_price, high_price, low_price, close_price, volume, currency, asset_type
                )
                SELECT 
                    symbol, fetch_date, open_price, high_price, low_price, close_price, volume, currency, asset_type
                FROM {staging_table}
                ON CONFLICT (symbol, fetch_date) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    currency = EXCLUDED.currency,
                    asset_type = EXCLUDED.asset_type
            """
            stmt.execute(upsert_query)
            
            # Đồng bộ kết thúc thành công, tiến hành DROP bảng staging để giải phóng tài nguyên DB
            print(f"[*] Bước 5: Đồng bộ hoàn tất, tiến hành xóa bảng tạm: {staging_table}...")
            stmt.execute(f"DROP TABLE IF EXISTS {staging_table}")
            
            print(f"[SUCCESS] Thực thi truy vấn Upsert thành công và xóa bảng tạm staging!")
            stmt.close()
        finally:
            # Đảm bảo đóng kết nối JDBC cơ sở dữ liệu để tránh rò rỉ tài nguyên mạng (connection leak)
            conn.close()

        print("[SUCCESS] Quy trình đồng bộ dữ liệu sang Serving Database hoàn thành thành công!")

    except Exception as e:
        print(f"[X] Lỗi trong quá trình đồng bộ dữ liệu sang PostgreSQL: {e}")
    finally:
        # Kết thúc Spark Session giải phóng tài nguyên CPU/RAM
        spark.stop()

if __name__ == "__main__":
    main()
