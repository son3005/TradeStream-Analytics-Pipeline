from datetime import datetime

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType

# Import các hàm cần test từ transform_bronze_to_silver
from src.processing.tradestream.transform_bronze_to_silver import calculate_daily_indicators, calculate_daily_ohlcv


@pytest.fixture(scope="module")
def spark_session():
    """Khởi tạo SparkSession local phục vụ chạy tests."""
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("pytest-pyspark-local")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()

def test_calculate_daily_ohlcv(spark_session):
    """Kiểm định logic tính toán nến ngày daily OHLCV từ dữ liệu ticks thô."""
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("trade_time", LongType(), True),
        StructField("fetch_date", DateType(), True)
    ])

    # Gi giả lập dữ liệu ticks cho symbol AAPL
    data = [
        # Giao dịch 1: open_price = 150.0 (trade_time nhỏ nhất)
        ("AAPL", 150.0, 10.0, 1780643400000, datetime.strptime("2026-06-05", "%Y-%m-%d").date()),
        # Giao dịch 2: high_price = 153.0 (giá cao nhất)
        ("AAPL", 153.0, 5.0, 1780643460000, datetime.strptime("2026-06-05", "%Y-%m-%d").date()),
        # Giao dịch 3: close_price = 152.5 (trade_time lớn nhất)
        ("AAPL", 152.5, 15.0, 1780643520000, datetime.strptime("2026-06-05", "%Y-%m-%d").date())
    ]

    input_df = spark_session.createDataFrame(data, schema)

    # Thực thi tính toán
    result_df = calculate_daily_ohlcv(input_df)

    # Schema mong đợi
    expected_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("fetch_date", DateType(), True),
        StructField("open_price", DoubleType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("close_price", DoubleType(), True),
        StructField("volume", LongType(), True)
    ])

    # Kết quả mong đợi: open=150.0, high=153.0, low=150.0, close=152.5, volume=30
    expected_data = [
        ("AAPL", datetime.strptime("2026-06-05", "%Y-%m-%d").date(), 150.0, 153.0, 150.0, 152.5, 30)
    ]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    # So sánh kết quả sử dụng chispa
    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)

def test_calculate_daily_indicators(spark_session):
    """Kiểm định logic tính toán daily_return và price_range bằng Window functions."""
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("fetch_date", DateType(), True),
        StructField("open_price", DoubleType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("close_price", DoubleType(), True),
        StructField("volume", LongType(), True)
    ])

    # Giả lập dữ liệu OHLCV 2 ngày cho cùng 1 symbol
    data = [
        # Ngày 1: close = 100.0, low = 90.0, high = 110.0
        # -> daily_return = 0.0 (do không có ngày trước), price_range = ((110 - 90)/90)*100 = 22.22%
        ("MSFT", datetime.strptime("2026-06-04", "%Y-%m-%d").date(), 95.0, 110.0, 90.0, 100.0, 1000),
        # Ngày 2: close = 110.0, low = 100.0, high = 120.0
        # -> daily_return = ((110 - 100)/100)*100 = 10.0%, price_range = ((120 - 100)/100)*100 = 20.0%
        ("MSFT", datetime.strptime("2026-06-05", "%Y-%m-%d").date(), 105.0, 120.0, 100.0, 110.0, 1500)
    ]
    input_df = spark_session.createDataFrame(data, schema)

    # Thực thi tính toán
    result_df = calculate_daily_indicators(input_df)

    # Chỉ chọn các cột cần kiểm tra để so sánh
    result_selected = result_df.select("symbol", "fetch_date", "daily_return", "price_range")

    expected_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("fetch_date", DateType(), True),
        StructField("daily_return", DoubleType(), True),
        StructField("price_range", DoubleType(), True)
    ])

    expected_data = [
        ("MSFT", datetime.strptime("2026-06-04", "%Y-%m-%d").date(), 0.0, 22.22222222222222),
        ("MSFT", datetime.strptime("2026-06-05", "%Y-%m-%d").date(), 10.0, 20.0)
    ]
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    # So sánh kết quả sử dụng chispa
    assert_df_equality(result_selected, expected_df, ignore_row_order=True, ignore_nullable=True)
