import logging
import os
import sys
from datetime import timedelta

import mlflow
import mlflow.xgboost
import pandas as pd
import psycopg2

# Thêm đường dẫn root vào sys.path để import ml
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from ml.features.build_features import calculate_features, get_historical_data

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("predict")

def main() -> None:
    """
    Quy trình chính chạy dự báo xu hướng giá ngày mai từ mô hình Production trên MLflow và cập nhật TimescaleDB.

    Args:
        Không có tham số đầu vào.

    Returns:
        None.

    Raises:
        Exception: Lỗi phát sinh trong quá trình nạp mô hình từ MLflow, tính toán đặc trưng, chạy dự báo hoặc cập nhật TimescaleDB.
    """
    logger.info("Đang khởi chạy quy trình dự báo...")

    # 1. Cấu hình MLflow và nạp mô hình Production
    mlflow_uri: str = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(mlflow_uri)

    model_name: str = "price_direction_model"
    model_uri: str = f"models:/{model_name}/Production"

    logger.info(f"Đang nạp mô hình '{model_name}' phiên bản 'Production' từ Registry...")
    try:
        model = mlflow.xgboost.load_model(model_uri)
        logger.info("Nạp mô hình thành công.")

        # Lấy thông tin phiên bản mô hình từ Model Registry
        client: mlflow.tracking.MlflowClient = mlflow.tracking.MlflowClient()
        latest_versions = client.get_latest_versions(model_name, stages=["Production"])
        model_version: str = f"v{latest_versions[0].version}.0.0" if latest_versions else "v1.0.0"
        logger.info(f"Phiên bản mô hình đang hoạt động: {model_version}")
    except Exception as e:
        logger.error(f"Lỗi khi nạp mô hình từ MLflow: {e}")
        sys.exit(1)

    # 2. Lấy dữ liệu đặc trưng ngày hiện tại
    try:
        raw_df: pd.DataFrame = get_historical_data()
        latest_features_df: pd.DataFrame = calculate_features(raw_df, is_training=False)
    except Exception as e:
        logger.error(f"Lỗi khi chuẩn bị dữ liệu đặc trưng: {e}")
        sys.exit(1)

    if len(latest_features_df) == 0:
        logger.warning("Không tìm thấy dữ liệu đặc trưng hợp lệ nào cho các symbol. Dừng.")
        return

    features: list[str] = ['sma_5', 'sma_10', 'sma_20', 'close_lag_1', 'volume_lag_1', 'return_lag_1']

    # 3. Chạy dự báo cho từng symbol
    predictions: list[tuple] = []
    for _, row in latest_features_df.iterrows():
        symbol: str = row['symbol']
        full_date = row['full_date']
        # Ngày dự đoán là ngày tiếp theo (ngày mai)
        prediction_date = (full_date + timedelta(days=1)).date()

        # Chuẩn bị input features
        X = row[features].values.astype(float).reshape(1, -1)
        X_df: pd.DataFrame = pd.DataFrame(X, columns=features)

        # Chạy dự báo
        pred_class: int = int(model.predict(X_df)[0])
        # predict_proba trả về [prob_class_0, prob_class_1]
        prob: float = float(model.predict_proba(X_df)[0][1])

        logger.info(f"[PREDICTION] Symbol: {symbol} | Date: {prediction_date} | Dự báo: {'TĂNG' if pred_class == 1 else 'GIẢM'} (Xác suất: {prob:.4f})")

        predictions.append((symbol, prediction_date, pred_class, prob, model_version))

    # 4. Ghi kết quả vào TimescaleDB
    timescale_conn_str: str = os.getenv("TIMESCALE_CONN")
    if not timescale_conn_str:
        logger.error("Không tìm thấy cấu hình TIMESCALE_CONN trong biến môi trường.")
        sys.exit(1)

    logger.info("Đang kết nối tới TimescaleDB...")
    try:
        conn = psycopg2.connect(timescale_conn_str)
        cursor = conn.cursor()

        upsert_query: str = """
            INSERT INTO daily_predictions (symbol, prediction_date, predicted_direction, probability, model_version, created_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (symbol, prediction_date) DO UPDATE SET
                predicted_direction = EXCLUDED.predicted_direction,
                probability = EXCLUDED.probability,
                model_version = EXCLUDED.model_version,
                created_at = CURRENT_TIMESTAMP;
        """

        for pred in predictions:
            cursor.execute(upsert_query, pred)

        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Đã ghi thành công {len(predictions)} bản ghi dự báo vào TimescaleDB!")

    except Exception as e:
        logger.error(f"Lỗi khi ghi dữ liệu dự báo vào TimescaleDB: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

