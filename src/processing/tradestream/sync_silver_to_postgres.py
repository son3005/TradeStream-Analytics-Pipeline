import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.utils.spark_helper import get_spark_session, get_db_credentials

# Kích hoạt chế độ mã hóa ký tự UTF-8 cho Windows để hiển thị log tiếng Việt chính xác
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def main() -> None:
    """Đồng bộ dữ liệu nến ngày từ tầng Silver Iceberg sang Serving DB TimescaleDB.

    Đọc bảng dữ liệu fact_daily_prices, JOIN với dim_date và dim_assets để dàn phẳng 
    dữ liệu (Denormalization), ghi vào bảng staging tạm thời trên Postgres, sau đó 
    thực thi native UPSERT (ON CONFLICT DO UPDATE) sang bảng daily_prices của TimescaleDB.

    Returns:
        None

    Raises:
        Exception: Nếu có lỗi kết nối JDBC hoặc thực thi câu lệnh SQL native trên Postgres.
    """
    # =====================================================================
    # 3. KHỞI TẠO SPARK SESSION VỚI CATALOG LAKEHOUSE ICEBERG
    # =====================================================================
    spark = get_spark_session("SilverToPostgresSync")
    
    # Lấy thông tin DB credentials để ghi dữ liệu JDBC
    db_config = get_db_credentials()
    DB_USER = db_config["user"]
    DB_PASS = db_config["password"]
    JDBC_URL = db_config["jdbc_url"]

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
