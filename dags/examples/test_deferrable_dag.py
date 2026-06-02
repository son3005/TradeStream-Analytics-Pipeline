# ============================================================
# FILE: test_deferrable_dag.py
# MỤC ĐÍCH: DAG kiểm thử nhanh Deferrable Operator và Cảnh báo Telegram
# ============================================================

from datetime import datetime, timedelta
from airflow import DAG
from tradestream.operators.docker_spark_operator import DockerSparkSubmitOperator
from tradestream.utils.alerts import send_telegram_alert

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # Đặt bằng 0 để fail ngay lập tức nhằm test Telegram Alert
    "on_failure_callback": send_telegram_alert,
}

with DAG(
    dag_id="test_deferrable_and_alert",
    default_args=default_args,
    description="Test Deferrable Operator và Telegram Alert khi xảy ra lỗi",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["test", "spark", "defer", "alert"],
) as dag:

    # Task này sẽ thất bại vì đường dẫn file script không tồn tại
    test_fail_task = DockerSparkSubmitOperator(
        task_id="test_fail_task",
        script_path="/opt/airflow/src/processing/tradestream/non_existent_script_test.py",
        poll_interval=5
    )
