import os
from urllib.parse import urlparse
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

def get_db_credentials() -> Dict[str, Any]:
    """Phân tích các cài đặt kết nối cơ sở dữ liệu từ biến môi trường TIMESCALE_CONN.

    Returns:
        Dict[str, Any]: Một từ điển chứa các thông tin cấu hình kết nối DB:
            - 'user': Tên đăng nhập cơ sở dữ liệu.
            - 'password': Mật khẩu cơ sở dữ liệu.
            - 'host': Địa chỉ máy chủ cơ sở dữ liệu.
            - 'port': Cổng kết nối cơ sở dữ liệu.
            - 'dbname': Tên cơ sở dữ liệu.
            - 'jdbc_url': Đường dẫn JDBC đã được định dạng.
    """
    conn_str: str = os.environ.get(
        "TIMESCALE_CONN",
        "postgresql://postgres:postgres@timescaledb:5432/tradestream"
    )

    if "postgresql://" not in conn_str:
        # Parse libpq style connection string (e.g., 'host=localhost port=5432 ...')
        pairs: Dict[str, str] = dict(item.split("=") for item in conn_str.split() if "=" in item)
        db_user: str = pairs.get("user", "postgres")
        db_pass: str = pairs.get("password", "postgres")
        db_host: str = pairs.get("host", "timescaledb")
        db_port: int = int(pairs.get("port", "5432"))
        db_name: str = pairs.get("dbname", "tradestream")
    else:
        # Parse connection URL
        parsed = urlparse(conn_str)
        db_user = parsed.username or "postgres"
        db_pass = parsed.password or "postgres"
        db_host = parsed.hostname or "timescaledb"
        db_port = parsed.port or 5432
        db_name = (parsed.path or "/tradestream").lstrip("/")

    jdbc_url: str = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    return {
        "user": db_user,
        "password": db_pass,
        "host": db_host,
        "port": db_port,
        "dbname": db_name,
        "jdbc_url": jdbc_url
    }

def get_spark_session(
    app_name: str, 
    enable_iceberg: bool = True, 
    custom_packages: Optional[str] = None
) -> SparkSession:
    """Khởi tạo và cấu hình một đối tượng SparkSession tích hợp các thiết lập MinIO và Iceberg.

    Args:
        app_name (str): Tên của ứng dụng Spark.
        enable_iceberg (bool): Cờ bật/tắt cấu hình catalog cho Apache Iceberg.
        custom_packages (Optional[str]): Danh sách các gói thư viện Spark tùy chỉnh cần cài đặt.

    Returns:
        SparkSession: Thực thể SparkSession đã được cấu hình hoàn chỉnh.
    """
    # 1. Load MinIO environment variables
    minio_user: str = os.environ.get("MINIO_ROOT_USER", "admin")
    minio_pass: str = os.environ.get("MINIO_ROOT_PASSWORD", "minioadminpassword")
    minio_bucket: str = os.environ.get("MINIO_LAKEHOUSE_BUCKET", "lakehouse")
    minio_endpoint: str = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    spark_master: str = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
    
    # Combined package list containing all necessary packages (Kafka, Iceberg, Postgres, AWS S3)
    default_packages: str = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.postgresql:postgresql:42.6.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )
    
    spark_packages: str = custom_packages or os.environ.get("SPARK_PACKAGES") or default_packages

    # 2. Get DB configuration
    db_config: Dict[str, Any] = get_db_credentials()

    # 3. Build SparkSession
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.jars.packages", spark_packages)
        .config("spark.sql.shuffle.partitions", "4")
        # Configure MinIO (S3A) settings
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_pass)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    # 4. Conditionally configure Iceberg Catalog
    if enable_iceberg:
        builder = (
            builder
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "jdbc")
            .config("spark.sql.catalog.lakehouse.uri", db_config["jdbc_url"])
            .config("spark.sql.catalog.lakehouse.jdbc.user", db_config["user"])
            .config("spark.sql.catalog.lakehouse.jdbc.password", db_config["password"])
            .config("spark.sql.catalog.lakehouse.warehouse", f"s3a://{minio_bucket}/warehouse")
            .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        )

    spark: SparkSession = builder.getOrCreate()
    return spark
