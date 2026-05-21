import os
from urllib.parse import urlparse
from pyspark.sql import SparkSession

# Read Postgres connection params
_TIMESCALE_CONN = os.environ.get(
    "TIMESCALE_CONN",
    "postgresql://postgres:postgres@timescaledb:5432/tradestream"
)
_parsed = urlparse(_TIMESCALE_CONN)
DB_USER = _parsed.username or "postgres"
DB_PASS = _parsed.password or "postgres"
DB_HOST = _parsed.hostname or "timescaledb"
DB_PORT = _parsed.port or 5432
DB_NAME = (_parsed.path or "/tradestream").lstrip("/")

# Construct JDBC URL
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Read MinIO parameters
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_PASS = os.environ.get("MINIO_ROOT_PASSWORD", "minioadminpassword")
MINIO_BUCKET = os.environ.get("MINIO_LAKEHOUSE_BUCKET", "lakehouse")

def main():
    print("=" * 60)
    print("  INITIALIZING SPARK SESSION WITH ICEBERG & MINIO")
    print("=" * 60)

    # Initialize SparkSession with Iceberg configuration
    spark = (
        SparkSession.builder
        .appName("TestSparkIceberg")
        # Run locally inside the master container for this test
        .master("local[*]")
        .config("spark.jars.packages", 
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                "org.postgresql:postgresql:42.6.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        # Iceberg catalog configuration using JDBC (Postgres backend)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "jdbc")
        .config("spark.sql.catalog.lakehouse.uri", JDBC_URL)
        .config("spark.sql.catalog.lakehouse.jdbc.user", DB_USER)
        .config("spark.sql.catalog.lakehouse.jdbc.password", DB_PASS)
        .config("spark.sql.catalog.lakehouse.warehouse", f"s3a://{MINIO_BUCKET}/warehouse")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        # MinIO S3 API connection configuration
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        print("[*] Creating namespace (database) in Iceberg...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.trading")
        
        print("[*] Creating Iceberg table...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.trading.test_iceberg (
                id INT,
                name STRING
            ) USING iceberg
        """)
        
        print("[*] Inserting test data into Iceberg table...")
        spark.sql("INSERT INTO lakehouse.trading.test_iceberg VALUES (1, 'Iceberg'), (2, 'MinIO')")
        
        print("[*] Querying data from Iceberg table:")
        df = spark.sql("SELECT * FROM lakehouse.trading.test_iceberg")
        df.show()
        
        print("[SUCCESS] Apache Iceberg Catalog and MinIO S3 integration is fully operational!")
        
    except Exception as e:
        print(f"[X] Error executing Iceberg Spark: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
