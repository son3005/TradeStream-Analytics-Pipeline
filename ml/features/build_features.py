import logging
import os

import pandas as pd
from trino.dbapi import connect

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("build_features")

def get_historical_data() -> pd.DataFrame:
    """
    Truy vấn dữ liệu lịch sử giá từ Apache Iceberg thông qua công cụ truy vấn distributed SQL Trino.

    Args:
        Không có tham số đầu vào.

    Returns:
        pd.DataFrame: DataFrame chứa dữ liệu lịch sử giá với các cột:
            symbol (str), full_date (datetime), open_price (float), high_price (float),
            low_price (float), close_price (float), volume (float).

    Raises:
        Exception: Phát sinh lỗi khi không thể kết nối tới coordinator Trino hoặc truy vấn thất bại.
    """
    logger.info("Đang kết nối tới Trino coordinator...")
    # Trino host mặc định trong docker network là 'trino' và port '8080'
    trino_host: str = os.getenv("TRINO_HOST", "trino")
    trino_port: int = int(os.getenv("TRINO_PORT", "8080"))
    trino_user: str = os.getenv("TRINO_USER", "airflow")

    conn = connect(
        host=trino_host,
        port=trino_port,
        user=trino_user,
        catalog="lakehouse",
        schema="trading"
    )

    query: str = """
        SELECT
            f.symbol,
            d.full_date,
            f.open_price,
            f.high_price,
            f.low_price,
            f.close_price,
            f.volume
        FROM lakehouse.trading.fact_daily_prices f
        JOIN lakehouse.trading.dim_date d ON f.date_key = d.date_key
        ORDER BY f.symbol, d.full_date ASC
    """

    logger.info("Đang thực thi truy vấn Trino...")
    df: pd.DataFrame = pd.read_sql_query(query, conn)
    logger.info(f"Lấy dữ liệu thành công! Tổng số dòng: {len(df)}")
    return df

def calculate_features(df: pd.DataFrame, is_training: bool = True) -> pd.DataFrame:
    """
    Tính toán các chỉ báo kỹ thuật tài chính (SMA, Lag) và nhãn xu hướng giá (Target).

    Args:
        df (pd.DataFrame): DataFrame chứa dữ liệu thô được lấy từ Trino.
        is_training (bool): Cờ xác định chế độ xử lý. True cho huấn luyện mô hình, False cho chạy dự báo.
            Mặc định là True.

    Returns:
        pd.DataFrame: DataFrame sau khi tính toán các đặc trưng:
            - Nếu is_training=True: Trả về tập dữ liệu sạch, đã loại bỏ các hàng có giá trị NaN.
            - Nếu is_training=False: Trả về dòng đặc trưng mới nhất (ngày hiện tại) của mỗi symbol để chạy dự báo.

    Raises:
        ValueError: Nếu DataFrame đầu vào trống hoặc thiếu các cột dữ liệu bắt buộc.
    """
    if df.empty:
        raise ValueError("DataFrame đầu vào không được trống")

    logger.info("Đang tính toán các đặc trưng kỹ thuật...")

    # Đảm bảo dữ liệu được sắp xếp đúng thứ tự thời gian theo từng symbol
    df['full_date'] = pd.to_datetime(df['full_date'])
    df = df.sort_values(by=['symbol', 'full_date']).reset_index(drop=True)

    # 1. Tính toán SMA nhóm theo symbol
    df['sma_5'] = df.groupby('symbol')['close_price'].transform(lambda x: x.rolling(window=5).mean())
    df['sma_10'] = df.groupby('symbol')['close_price'].transform(lambda x: x.rolling(window=10).mean())
    df['sma_20'] = df.groupby('symbol')['close_price'].transform(lambda x: x.rolling(window=20).mean())

    # 2. Đặc trưng trễ (Lag)
    df['close_lag_1'] = df.groupby('symbol')['close_price'].shift(1)
    df['volume_lag_1'] = df.groupby('symbol')['volume'].shift(1)
    # Tỷ lệ sinh lời của ngày hôm trước
    df['return_lag_1'] = df.groupby('symbol')['close_price'].pct_change(1)

    # 3. Tạo nhãn target (1 nếu ngày mai tăng giá, 0 nếu giảm/không đổi)
    df['close_tomorrow'] = df.groupby('symbol')['close_price'].shift(-1)
    df['target'] = (df['close_tomorrow'] > df['close_price']).astype(int)

    if is_training:
        # Huấn luyện: cần drop toàn bộ dòng NaN (bao gồm dòng cuối vì close_tomorrow bị NaN)
        df_processed: pd.DataFrame = df.dropna().copy()
        # Loại bỏ các cột phụ trợ
        df_processed = df_processed.drop(columns=['close_tomorrow'])
        logger.info(f"Huấn luyện - Xử lý đặc trưng xong. Số lượng dòng còn lại: {len(df_processed)}")
        return df_processed
    else:
        # Suy diễn: chúng ta cần dự đoán cho ngày mai dựa trên đặc trưng của ngày hôm nay.
        # Ở đây, close_tomorrow của ngày hôm nay sẽ bị NaN, nhưng ta vẫn giữ lại dòng này.
        # Ta chỉ drop các dòng bị thiếu đặc trưng đầu vào (sma_20, return_lag_1, vv.)
        feature_cols: list[str] = ['sma_5', 'sma_10', 'sma_20', 'close_lag_1', 'volume_lag_1', 'return_lag_1']
        df_processed = df.dropna(subset=feature_cols).copy()

        # Chỉ lấy bản ghi mới nhất của mỗi symbol để chạy dự đoán
        latest_df: pd.DataFrame = df_processed.groupby('symbol').last().reset_index()
        latest_df = latest_df.drop(columns=['close_tomorrow', 'target'], errors='ignore')

        logger.info(f"Dự báo - Xử lý đặc trưng xong. Số lượng symbol cần dự đoán: {len(latest_df)}")
        return latest_df

if __name__ == "__main__":
    # Test thử script
    try:
        raw_df_test: pd.DataFrame = get_historical_data()
        train_df_test: pd.DataFrame = calculate_features(raw_df_test, is_training=True)
        logger.info(f"Dữ liệu huấn luyện mẫu:\n{train_df_test.head(5)}")
    except Exception as e:
        logger.error(f"Lỗi khi chạy thử build_features: {e}")

