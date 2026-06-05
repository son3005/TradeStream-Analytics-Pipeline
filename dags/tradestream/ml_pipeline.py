# ============================================================
# FILE: ml_pipeline.py
# MỤC ĐÍCH: Điều phối Machine Learning Training & Prediction Pipelines
# ============================================================

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from tradestream.utils.alerts import send_telegram_alert

logger = logging.getLogger("ml_pipeline")

default_args: dict = {
    "owner": "tradestream",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_telegram_alert,
}

# ------------------------------------------------------------
# 1. DAG HUẤN LUYỆN MÔ HÌNH (HÀNG TUẦN - CHỦ NHẬT)
# ------------------------------------------------------------
@dag(
    dag_id="ml_training_pipeline",
    default_args=default_args,
    description="Pipeline huấn luyện lại mô hình XGBoost hàng tuần và đăng ký lên MLflow",
    schedule="0 0 * * 0",  # Chủ nhật hàng tuần lúc 00:00
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=["tradestream", "ml", "training", "mlflow"],
    max_active_runs=1,
)
def ml_training_pipeline():

    @task(task_id="train_model_task")
    def train_model() -> None:
        """
        Task của Airflow gọi tiến trình huấn luyện mô hình và đăng ký lên MLflow.

        Args:
            Không có tham số đầu vào.

        Returns:
            None.

        Raises:
            Exception: Lỗi phát sinh từ script ml.training.train.
        """
        logger.info("Đang khởi chạy tiến trình huấn luyện mô hình từ Airflow task...")
        # Import bên trong hàm để tránh quá tải RAM/CPU cho Airflow Dag Processor
        from ml.training import train
        try:
            train.main()
            logger.info("Huấn luyện và đăng ký mô hình thành công!")
        except Exception as e:
            logger.error(f"Gặp lỗi khi huấn luyện mô hình: {e}")
            raise e

    train_model()

# Khởi tạo DAG Training
training_dag = ml_training_pipeline()


# ------------------------------------------------------------
# 2. DAG DỰ BÁO GIÁ HÀNG NGÀY (HÀNG NGÀY LÚC 23:00)
# ------------------------------------------------------------
@dag(
    dag_id="ml_prediction_pipeline",
    default_args=default_args,
    description="Pipeline chạy dự báo giá cho ngày mai dựa trên mô hình Production",
    schedule="0 23 * * *",  # Hàng ngày lúc 23:00
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=["tradestream", "ml", "prediction", "inference"],
    max_active_runs=1,
)
def ml_prediction_pipeline():

    @task(task_id="predict_price_task")
    def predict_price() -> None:
        """
        Task của Airflow gọi tiến trình dự đoán xu hướng giá và lưu vào TimescaleDB.

        Args:
            Không có tham số đầu vào.

        Returns:
            None.

        Raises:
            Exception: Lỗi phát sinh từ script ml.serving.predict.
        """
        logger.info("Đang khởi chạy tiến trình dự báo từ Airflow task...")
        # Import bên trong hàm để tránh quá tải RAM/CPU cho Airflow Dag Processor
        from ml.serving import predict
        try:
            predict.main()
            logger.info("Dự báo giá và cập nhật TimescaleDB thành công!")
        except Exception as e:
            logger.error(f"Gặp lỗi khi chạy dự báo: {e}")
            raise e

    predict_price()

# Khởi tạo DAG Prediction
prediction_dag = ml_prediction_pipeline()

