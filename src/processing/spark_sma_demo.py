"""
=============================================================
PHASE 2 — QUICK DEMO: Apache Spark tính SMA (Simple Moving Average)
=============================================================
Mục tiêu:
  - Hiểu Spark làm gì: Đọc dữ liệu → Xử lý → In kết quả
  - Chạy PySpark local (không cần Docker)
  - Tính SMA-5: Trung bình giá của 5 phiên gần nhất

Cách chạy:
  python src/processing/spark_sma_demo.py
=============================================================
"""
import sys
import os

# Fix lỗi UnicodeEncodeError trên Windows Terminal
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Đảm bảo Python tìm được thư viện trong project
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, round as spark_round,
    to_timestamp, lag
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# ============================
# BƯỚC 1: Khởi động "Nhà máy" Spark
# ============================
# SparkSession là cửa vào duy nhất để làm việc với Spark.
# "local[*]" nghĩa là chạy ngay trên máy tính của bạn,
# dùng tất cả CPU cores có sẵn.
print("\n🔥 Đang khởi động Spark Engine...")

spark = (
    SparkSession.builder
    .appName("TradeStream-SMA-Demo")   # Tên "Nhà máy"
    .master("local[*]")               # Chạy local, dùng hết CPU
    .config("spark.driver.memory", "1g")  # Cấp 1GB RAM cho Spark
    # Tắt log rác để dễ nhìn kết quả
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

# Tắt log ồn ào của Spark (chỉ hiện WARNING trở lên)
spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark khởi động thành công!\n")


# ============================
# BƯỚC 2: Định nghĩa "Bản thiết kế" dữ liệu (Schema)
# ============================
# Spark cần biết trước dữ liệu có những cột gì, kiểu dữ liệu là gì
# Giống như bạn khai báo kiểu biến trong Python — nhưng cho toàn bộ bảng.
schema = StructType([
    StructField("timestamp", StringType(), True),   # Thời gian (đọc như string trước)
    StructField("symbol",    StringType(), True),   # Tên coin (BTCUSDT)
    StructField("price",     DoubleType(), True),   # Giá (số thực)
    StructField("volume",    DoubleType(), True),   # Khối lượng giao dịch
])


# ============================
# BƯỚC 3: Đọc dữ liệu từ CSV
# ============================
CSV_PATH = "data/sample/btc_prices.csv"
print(f"📂 Đọc dữ liệu từ: {CSV_PATH}")

df_raw = (
    spark.read
    .option("header", "true")  # Dòng đầu tiên là tên cột
    .schema(schema)             # Dùng schema đã định nghĩa
    .csv(CSV_PATH)
)

# Chuyển cột timestamp từ String sang đúng kiểu Timestamp
df = df_raw.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)

print(f"✅ Đọc được {df.count()} dòng dữ liệu\n")
print("📊 5 dòng đầu tiên (dữ liệu thô):")
df.show(5, truncate=False)


# ============================
# BƯỚC 4: Tính SMA-5 (Simple Moving Average 5 phiên)
# ============================
# SMA là gì? Trung bình cộng giá của N phiên gần nhất.
# SMA-5 tại phiên thứ 6 = (Giá phiên 2 + 3 + 4 + 5 + 6) / 5
#
# Spark Window Function: Giống như "nhìn lại" N dòng trước đó
# rowsBetween(-4, 0) nghĩa là: từ 4 dòng trước đến dòng hiện tại (= 5 dòng)
print("⚙️  Đang tính SMA-5 (Trung bình động 5 phiên)...")

# Định nghĩa "khung nhìn" (Window):
# - Sắp xếp theo thời gian
# - Mỗi lần nhìn vào 5 dòng: 4 dòng trước + dòng hiện tại
window_5 = (
    Window
    .orderBy("timestamp")
    .rowsBetween(-4, 0)   # -4 = 4 dòng trước, 0 = dòng hiện tại
)

# Tính thêm SMA-10 cho vui
window_10 = (
    Window
    .orderBy("timestamp")
    .rowsBetween(-9, 0)
)

df_with_sma = df.select(
    col("timestamp"),
    col("symbol"),
    col("price"),
    col("volume"),
    # Tính SMA-5: avg(price) trong khung 5 dòng
    spark_round(avg(col("price")).over(window_5),  2).alias("sma_5"),
    # Tính SMA-10: avg(price) trong khung 10 dòng
    spark_round(avg(col("price")).over(window_10), 2).alias("sma_10"),
)


# ============================
# BƯỚC 5: Hiển thị kết quả
# ============================
print("\n📈 KẾT QUẢ — Giá BTC + SMA-5 + SMA-10:")
print("=" * 70)
df_with_sma.show(20, truncate=False)

# Tính thêm một insight đơn giản:
# Tín hiệu mua: Khi giá vượt lên trên SMA-5
# Tín hiệu bán: Khi giá xuống dưới SMA-5
print("\n💡 PHÂN TÍCH TÍN HIỆU (Giá so với SMA-5):")
print("-" * 50)

# Dùng SQL thuần — Spark hỗ trợ viết SQL giống database thông thường
df_with_sma.createOrReplaceTempView("btc_data")
df_signal = spark.sql("""
    SELECT
        timestamp,
        price,
        sma_5,
        ROUND(price - sma_5, 2)  AS diff_from_sma5,
        CASE
            WHEN price > sma_5 THEN '📈 Giá trên SMA-5 (xu hướng tăng)'
            WHEN price < sma_5 THEN '📉 Giá dưới SMA-5 (xu hướng giảm)'
            ELSE                    '➡️  Giá bằng SMA-5 (trung tính)'
        END AS signal
    FROM btc_data
    WHERE sma_5 IS NOT NULL
""")
df_signal.show(20, truncate=False)

# ============================
# BƯỚC 6: Tổng kết
# ============================
avg_price = df.agg(avg("price")).collect()[0][0]
max_price = df.agg({"price": "max"}).collect()[0][0]
min_price = df.agg({"price": "min"}).collect()[0][0]

print("\n📊 THỐNG KÊ TỔNG QUAN:")
print(f"  🔢 Tổng số phiên:    {df.count()} phiên")
print(f"  💰 Giá trung bình:   ${avg_price:,.2f}")
print(f"  📈 Giá cao nhất:     ${max_price:,.2f}")
print(f"  📉 Giá thấp nhất:    ${min_price:,.2f}")
print(f"  📊 Biên độ dao động: ${max_price - min_price:,.2f}")

print("\n✅ QUICK DEMO HOÀN THÀNH!")
print("🎯 Bước tiếp theo: Kết nối Spark vào Kafka để xử lý real-time!")

# Dừng Spark engine
spark.stop()
