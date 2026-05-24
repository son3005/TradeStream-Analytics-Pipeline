import os
import sys
from urllib.parse import urlparse
from pyspark.sql import SparkSession

# Kích hoạt UTF-8 trên Windows để hiển thị tiếng Việt chính xác
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# =====================================================================
# 1. ĐỌC CẤU HÌNH KẾT NỐI DB (Cho Iceberg Catalog)
# =====================================================================
_TIMESCALE_CONN = os.environ.get(
    "TIMESCALE_CONN",
    "postgresql://postgres:postgres@timescaledb:5432/tradestream"
)

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
        .appName("LakehouseMaintenance")
        .master(SPARK_MASTER)
        .config("spark.jars.packages", SPARK_PACKAGES)
        # Tối ưu hiệu năng shuffle cho dữ liệu nhỏ
        .config("spark.sql.shuffle.partitions", "4")
        # Các cấu hình cho Apache Iceberg
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "jdbc")
        .config("spark.sql.catalog.lakehouse.uri", JDBC_URL)
        .config("spark.sql.catalog.lakehouse.jdbc.user", DB_USER)
        .config("spark.sql.catalog.lakehouse.jdbc.password", DB_PASS)
        .config("spark.sql.catalog.lakehouse.warehouse", f"s3a://{MINIO_BUCKET}/warehouse")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        # Cấu hình MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("[*] Khởi động Spark Job: Bảo trì hồ dữ liệu Lakehouse (Iceberg Maintenance)...")

    # Danh sách các bảng cần bảo trì định kỳ
    tables = [
        "lakehouse.trading.dim_assets",
        "lakehouse.trading.dim_date",
        "lakehouse.trading.fact_daily_prices"
    ]

    for table in tables:
        print(f"\n[*] ===================================================")
        print(f"[*] Đang thực thi các tác vụ bảo trì cho bảng: {table}")
        print(f"[*] ===================================================")

        # -----------------------------------------------------------------
        # A. Compaction (Gộp các tệp nhỏ - Optimize)
        # -----------------------------------------------------------------
        # Cơ chế: Mỗi khi Spark ghi dữ liệu (đặc biệt là streaming hoặc ghi hàng ngày), nó sẽ sinh ra
        # các tệp Parquet rất nhỏ (chỉ vài KB/MB). Quá nhiều file nhỏ sẽ làm chậm quá trình quét đĩa khi đọc.
        # Giải pháp: Thủ tục `rewrite_data_files` sẽ gộp các file nhỏ thành các file chuẩn lớn hơn (~128MB).
        print(f"[*] 1. Tiến hành nén file (rewrite_data_files) cho bảng {table}...")
        try:
            spark.sql(f"CALL lakehouse.system.rewrite_data_files(table => '{table}')").show(truncate=False)
        except Exception as e:
            print(f"[X] Quá trình nén file thất bại: {e}")

        # -----------------------------------------------------------------
        # B. Expire Snapshots (Xóa lịch sử Snapshots cũ)
        # -----------------------------------------------------------------
        # Cơ chế: Iceberg hỗ trợ "Time Travel" bằng cách giữ lại toàn bộ lịch sử thay đổi (snapshots).
        # Tuy nhiên, điều này chiếm dụng dung lượng đĩa cực kỳ lớn theo thời gian.
        # Giải pháp: Thủ tục `expire_snapshots` sẽ xóa bỏ các phiên bản snapshot quá cũ.
        # Ở đây ta cấu hình `retain_last => 1` để giữ lại duy nhất 1 snapshot mới nhất, dọn sạch dữ liệu cũ.
        print(f"[*] 2. Tiến hành xóa dọn snapshots cũ (chỉ giữ lại snapshot cuối cùng) cho bảng {table}...")
        try:
            spark.sql(f"CALL lakehouse.system.expire_snapshots(table => '{table}', retain_last => 1)").show(truncate=False)
        except Exception as e:
            print(f"[X] Quá trình dọn dẹp snapshots thất bại: {e}")

        # -----------------------------------------------------------------
        # C. Remove Orphan Files (Xóa các tệp mồ côi)
        # -----------------------------------------------------------------
        # Cơ chế: Khi một job Spark ghi dữ liệu bị crash giữa chừng, hoặc sau khi xóa snapshot cũ,
        # các file Parquet dữ liệu thực tế vẫn còn lưu trên MinIO nhưng không có metadata nào trỏ tới.
        # Các file này gọi là "Orphan Files" (Tệp mồ côi / Tệp rác).
        # Giải pháp: Thủ tục `remove_orphan_files` quét thư mục bảng trên MinIO và xóa sạch các file rác này.
        print(f"[*] 3. Tiến hành tìm kiếm và xóa bỏ các tệp tin mồ côi cho bảng {table}...")
        try:
            spark.sql(f"CALL lakehouse.system.remove_orphan_files(table => '{table}')").show(truncate=False)
        except Exception as e:
            print(f"[X] Quá trình xóa tệp mồ côi thất bại: {e}")

        print(f"[SUCCESS] Hoàn thành toàn bộ quy trình bảo trì cho bảng: {table}!")

    print("\n[SUCCESS] Hoàn thành bảo trì Lakehouse thành công cho tất cả các bảng!")
    spark.stop()

if __name__ == "__main__":
    main()
