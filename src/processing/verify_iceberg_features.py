import os
import sys
from urllib.parse import urlparse
from pyspark.sql import SparkSession

# Kích hoạt UTF-8 để hiển thị tiếng Việt trên Terminal Windows không lỗi font
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
        .appName("VerifyIcebergFeatures")
        .master(SPARK_MASTER)
        .config("spark.jars.packages", SPARK_PACKAGES)
        # Các cấu hình JDBC Catalog của Iceberg
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
    print("[*] Khởi động Spark Job: Xác minh các tính năng cao cấp của Apache Iceberg...")

    try:
        # =====================================================================
        # FEATURE 1: Schema Evolution (Tiến hóa cấu trúc bảng trực tuyến)
        # =====================================================================
        # Khác với các bảng truyền thống (như Hive/CSV), Iceberg cho phép bạn thêm, sửa, xóa cột
        # một cách độc lập ngay lập tức mà không làm hỏng dữ liệu lịch sử hoặc cần phải viết lại bảng.
        print("\n=== TÍNH NĂNG 1: SCHEMA EVOLUTION (TIẾN HÓA CẤU TRÚC BẢNG) ===")
        table_name = "lakehouse.trading.dim_assets"
        print(f"[*] Cấu trúc gốc của bảng {table_name}:")
        spark.sql(f"DESCRIBE {table_name}").show(truncate=False)

        # Thêm cột mới `market_cap_category` (Ví dụ phân loại vốn hoá) bằng lệnh ALTER TABLE
        print("[*] Đang thực thi thêm cột mới 'market_cap_category' (STRING) vào bảng...")
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS (market_cap_category STRING)")
        print("[*] Cấu trúc bảng sau khi được cập nhật (Thêm thành công):")
        spark.sql(f"DESCRIBE {table_name}").show(truncate=False)

        # Xóa cột mới thêm để trả lại cấu trúc sạch sẽ ban đầu cho bảng hệ thống
        print("[*] Đang thực thi xóa cột 'market_cap_category' để khôi phục cấu trúc gốc...")
        spark.sql(f"ALTER TABLE {table_name} DROP COLUMN market_cap_category")
        print("[SUCCESS] Xác minh thành công tính năng Schema Evolution!")

        # =====================================================================
        # FEATURE 2: Time Travel (Truy vấn theo thời gian / Lịch sử Snapshot)
        # =====================================================================
        # Mỗi lần dữ liệu được thêm hoặc sửa đổi, Iceberg tạo ra 1 "Snapshot".
        # Ta có thể truy vấn bảng dữ liệu tại bất kỳ Snapshot ID nào để xem trạng thái dữ liệu trong quá khứ.
        print("\n=== TÍNH NĂNG 2: TIME TRAVEL (TRUY VẤN LỊCH SỬ SNAPSHOT) ===")
        print(f"[*] Đọc bảng lịch sử commit (history) của {table_name}...")
        history_df = spark.read.format("iceberg").load(f"{table_name}.history")
        history_df.show(truncate=False)

        # Lấy chi tiết lịch sử snapshot ID và thời gian commit
        print(f"[*] Đọc danh sách chi tiết các snapshot của bảng {table_name}...")
        snapshots_df = spark.sql(f"SELECT snapshot_id, committed_at, parent_id FROM {table_name}.snapshots")
        snapshots_df.show(truncate=False)

        # Lấy snapshot_id đầu tiên trong lịch sử để chạy thử Time Travel
        snapshots = snapshots_df.collect()
        if len(snapshots) >= 1:
            first_snapshot_id = snapshots[0]["snapshot_id"]
            print(f"[*] Đang truy vấn bảng dữ liệu tại Snapshot ID đầu tiên: {first_snapshot_id}...")
            # Sử dụng option("snapshot-id") để tải dữ liệu của phiên bản snapshot này
            spark.read.option("snapshot-id", first_snapshot_id).table(table_name).show(5, truncate=False)
            print("[SUCCESS] Xác minh thành công tính năng Time Travel (Truy vấn lịch sử)!")
        else:
            print("[NOTE] Không có đủ snapshots lịch sử để chạy thử nghiệm Time Travel.")

        # =====================================================================
        # FEATURE 3: Partition Evolution (Tiến hóa phân vùng trực tuyến)
        # =====================================================================
        # Trong các hồ dữ liệu thế hệ cũ, khi thay đổi phân vùng (ví dụ từ phân vùng theo năm sang theo tháng),
        # bạn phải viết lại toàn bộ các thư mục chứa dữ liệu cũ.
        # Với Iceberg, bạn có thể thay đổi cột phân vùng bất kỳ lúc nào. Dữ liệu mới sẽ đi theo phân vùng mới,
        # dữ liệu cũ vẫn nằm ở phân vùng cũ và Iceberg tự động giải quyết các truy vấn kết hợp cả hai.
        print("\n=== TÍNH NĂNG 3: PARTITION EVOLUTION (TIẾN HÓA PHÂN VÙNG) ===")
        print(f"[*] Kiểm tra thông tin phân vùng hiện tại của bảng {table_name}...")
        partitions_df = spark.read.format("iceberg").load(f"{table_name}.partitions")
        partitions_df.show(truncate=False)

        # Tiến hành phân vùng bổ sung theo trường `currency` trên môi trường thực tế
        print("[*] Đang thực thi thêm trường phân vùng mới 'currency' vào bố cục bảng...")
        spark.sql(f"ALTER TABLE {table_name} ADD PARTITION FIELD currency")
        
        # Lưu ý: Lúc này, bất kỳ dữ liệu mới nào được ghi vào sẽ tự động lưu thư mục phân vùng theo currency.
        # Dữ liệu cũ trên MinIO vẫn giữ nguyên cấu trúc cũ, không bị ghi đè hay thay đổi đường dẫn.
        
        # Dọn dẹp: Khôi phục lại cấu trúc ban đầu bằng cách gỡ trường currency ra khỏi danh sách phân vùng
        print("[*] Đang xóa bỏ trường phân vùng 'currency' để khôi phục cấu trúc ban đầu...")
        spark.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD currency")
        print("[SUCCESS] Xác minh thành công tính năng Partition Evolution!")

    except Exception as e:
        print(f"[X] Gặp lỗi trong quá trình xác minh tính năng của Apache Iceberg: {e}")
    finally:
        # Đóng Spark Session giải phóng bộ nhớ
        spark.stop()

if __name__ == "__main__":
    main()
