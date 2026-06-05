# ============================================================
# FILE: data_quality.py
# MỤC ĐÍCH: Airflow DAG tự động kiểm định chất lượng dữ liệu định kỳ
#            bằng Great Expectations và kích hoạt cảnh báo Telegram khi lỗi
# ============================================================

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from sqlalchemy import create_engine
from tradestream.utils.alerts import send_telegram_alert

from src.utils.data_quality_helper import DataQualityChecker

# Import Asset theo cơ chế tương thích ngược cho Airflow 3.x và 2.x
try:
    from airflow.sdk import Asset
except ImportError:
    from airflow.datasets import Dataset as Asset

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "tradestream",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": send_telegram_alert,
}

@dag(
    dag_id="tradestream_data_quality_pipeline",
    default_args=default_args,
    description="Pipeline tự động kiểm định chất lượng dữ liệu (Great Expectations)",
    schedule="1-59/5 * * * *",  # Chạy mỗi 5 phút, lệch 1 phút so với Cold Path (ví dụ: phút thứ 1, 6, 11...)
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=["tradestream", "data-quality", "great-expectations"],
    max_active_runs=1,
)
def tradestream_data_quality_pipeline() -> None:

    @task(
        task_id="run_data_quality_check",
        inlets=[Asset("postgres://timescaledb:5432/tradestream/public/daily_prices")]
    )
    def run_dq_check() -> str:
        """Đọc dữ liệu gần đây từ TimescaleDB và chạy kiểm định qua Great Expectations.

        Returns:
            str: Tóm tắt kết quả kiểm định.

        Raises:
            ValueError: Nếu kiểm định chất lượng dữ liệu thất bại.
        """
        conn_str = os.environ.get("TIMESCALE_CONN", "postgresql://postgres:postgres@timescaledb:5432/tradestream")
        if "postgresql://" not in conn_str:
            pairs = dict(item.split("=") for item in conn_str.split() if "=" in item)
            db_user = pairs.get("user", "postgres")
            db_pass = pairs.get("password", "postgres")
            db_host = pairs.get("host", "timescaledb")
            db_port = pairs.get("port", "5432")
            db_name = pairs.get("dbname", "tradestream")
            conn_str = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

        logger.info("Đang nạp dữ liệu từ TimescaleDB...")
        engine = create_engine(conn_str)

        # Load dữ liệu trong vòng 2 ngày để bao quát dữ liệu mới đồng bộ
        query = "SELECT * FROM daily_prices WHERE fetch_date >= CURRENT_DATE - INTERVAL '2 days'"
        df = pd.read_sql(query, engine)

        if df.empty:
            logger.warning("Không có dữ liệu mới trong 2 ngày qua để kiểm định.")
            return "No new data to check."

        checker = DataQualityChecker()
        result = checker.run_validation(df)

        if not result["success"]:
            raise ValueError(f"Kiểm định chất lượng dữ liệu thất bại! Tóm tắt: {result['summary']}")

        logger.info("Kiểm định chất lượng dữ liệu thành công!")
        return result["summary"]

    run_dq_check()

# Khởi tạo DAG
pipeline = tradestream_data_quality_pipeline()
