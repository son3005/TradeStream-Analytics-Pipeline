import os
from urllib.parse import urlparse 
from pyspark.sql import SparkSession

#Biến môi trường 
_TIMESCALE_CONN = os.environ.get(
    "TIMESCALE_CONN",
    "postgresql://postgres:postgres@timescaledb:5432/tradestream"
)

if "postgresql://" not in _TIMESCALE_CONN:
    # Parse libpq format: key=value key=value
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
    spark = (
        SparkSession.builder
        .appName("CreateStarSchema")
        .master(SPARK_MASTER)
        .config("spark.jars.packages", SPARK_PACKAGES)
        # Tối ưu hiệu năng shuffle cho dữ liệu nhỏ
        .config("spark.sql.shuffle.partitions", "4")
        #config catalog
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

        .config("spark.sql.catalog.lakehouse", 
                "org.apache.iceberg.spark.SparkCatalog")
                
        .config("spark.sql.catalog.lakehouse.type", "jdbc")
        .config("spark.sql.catalog.lakehouse.uri", JDBC_URL)
        .config("spark.sql.catalog.lakehouse.jdbc.user", DB_USER)
        .config("spark.sql.catalog.lakehouse.jdbc.password", DB_PASS)
        .config("spark.sql.catalog.lakehouse.warehouse", f"s3a://{MINIO_BUCKET}/warehouse")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        #config minio
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. Tạo Namespace (Database)
        print("[*] Creating database namespace 'trading'...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.trading")
        
        # 2. Tạo bảng dim_assets phân vùng theo asset_type
        print("[*] Creating Iceberg table: dim_assets...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.trading.dim_assets (
                symbol STRING,
                name STRING,
                asset_type STRING,
                currency STRING
            ) USING iceberg
            PARTITIONED BY (asset_type)
        """)
        
        # 3. Tạo bảng dim_date phân vùng theo year
        print("[*] Creating Iceberg table: dim_date...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.trading.dim_date (
                date_key INT,
                full_date STRING,
                year INT,
                quarter INT,
                month INT,
                day INT,
                day_of_week INT,
                day_name STRING,
                month_name STRING,
                is_weekend INT
            ) USING iceberg
            PARTITIONED BY (year)
        """)
        
        print("[*] Creating Iceberg table: fact_daily_prices...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.trading.fact_daily_prices (
                symbol STRING,
                date_key INT,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                volume LONG,
                daily_return DOUBLE,
                price_range DOUBLE
            ) USING iceberg
            PARTITIONED BY (symbol)
        """)
        print("[SUCCESS] All tables created successfully!")
        
        # 5. Nạp dữ liệu từ file dim_date.csv trên MinIO vào bảng dim_date Iceberg
        print("[*] Checking dim_date data status...")
        date_count = spark.sql("SELECT COUNT(*) FROM lakehouse.trading.dim_date").collect()[0][0]
        
        if date_count == 0:
            print("[*] Loading dim_date.csv from local container storage...")
            csv_path = "/opt/airflow/src/storage/dim_date.csv"
            
            # Đọc CSV từ MinIO
            date_df = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csv_path)
            )
            
            # Ghi vào bảng Iceberg dim_date
            print("[*] Writing records to Iceberg dim_date table...")
            (
                date_df.write
                .format("iceberg")
                .mode("append")
                .save("lakehouse.trading.dim_date")
            )
            
            new_count = spark.sql("SELECT COUNT(*) FROM lakehouse.trading.dim_date").collect()[0][0]
            print(f"[SUCCESS] Successfully loaded {new_count} records into dim_date table!")
        else:
            print(f"[NOTE] dim_date table already has {date_count} records. Skipping data load.")
        
        # 6. Nạp dữ liệu từ file symbols.json vào bảng dim_assets Iceberg
        print("[*] Checking dim_assets data status...")
        asset_count = spark.sql("SELECT COUNT(*) FROM lakehouse.trading.dim_assets").collect()[0][0]
        
        if asset_count == 0:
            print("[*] Loading symbols.json from config...")
            import json
            # Đường dẫn mount trong container
            config_path = "/opt/airflow/config/symbols.json"
            
            with open(config_path, "r", encoding="utf-8") as f:
                config_data = json.load(f)
            
            # Map dữ liệu thành danh sách các dictionary khớp với schema dim_assets
            asset_rows = []
            for item in config_data.get("symbols", []):
                asset_rows.append({
                    "symbol": item["symbol"],
                    "name": item["name"],
                    "asset_type": item["type"],  # map 'type' từ json sang 'asset_type' của bảng
                    "currency": "USD"            # Mặc định là USD
                })
            
            # Tạo Schema cho DataFrame
            from pyspark.sql.types import StructType, StructField, StringType
            schema = StructType([
                StructField("symbol", StringType(), True),
                StructField("name", StringType(), True),
                StructField("asset_type", StringType(), True),
                StructField("currency", StringType(), True)
            ])
            
            # [CHỖ TRỐNG 1]: Tạo DataFrame từ danh sách asset_rows và schema trên
            asset_df = spark.createDataFrame(asset_rows, schema=schema)
            
            # [CHỖ TRỐNG 2]: Ghi DataFrame vào bảng Iceberg 'lakehouse.trading.dim_assets'
            print("[*] Writing records to Iceberg dim_assets table...")
            (
                asset_df.write
                .format("iceberg")
                .mode("append")
                .save("lakehouse.trading.dim_assets")
            )
            
            new_asset_count = spark.sql("SELECT COUNT(*) FROM lakehouse.trading.dim_assets").collect()[0][0]
            print(f"[SUCCESS] Successfully loaded {new_asset_count} assets into dim_assets table!")
        else:
            print(f"[NOTE] dim_assets table already has {asset_count} records. Skipping data load.")



        print("[SUCCESS] All Star Schema tables created successfully in Apache Iceberg!")
        
    except Exception as e:
        print(f"[X] Error creating Star Schema: {e}")
    finally:
        # Bắt buộc đóng SparkSession để giải phóng RAM/CPU
        spark.stop()

if __name__ == "__main__":
    main()


