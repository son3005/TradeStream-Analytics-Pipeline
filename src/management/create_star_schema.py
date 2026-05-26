import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from src.utils.spark_helper import get_spark_session

def main() -> None:
    """Khởi tạo mô hình dữ liệu hình sao (Star Schema) cho Data Lakehouse.

    Tạo schema 'trading', các bảng Apache Iceberg: dim_assets, dim_date, 
    và fact_daily_prices trên MinIO. Nạp dữ liệu ban đầu cho các bảng 
    chiều thông tin (dimensions) từ các tệp cấu hình nguồn.

    Returns:
        None

    Raises:
        Exception: Nếu có lỗi kết nối MinIO hoặc thực thi câu lệnh DDL Iceberg.
    """
    spark = get_spark_session("CreateStarSchema")

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
            csv_path = "/opt/airflow/src/management/dim_date.csv"
            
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


