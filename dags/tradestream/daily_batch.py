# ============================================================
# FILE: daily_batch.py
# MỤC ĐÍCH: Điều phối Real-Time Cold Path Pipeline (5-minute micro-batch)
# ============================================================

from datetime import datetime, timedelta
import logging
from airflow.decorators import dag
from tradestream.operators.docker_spark_operator import DockerSparkSubmitOperator
from tradestream.utils.alerts import send_telegram_alert

logger = logging.getLogger("realtime_cold_path_pipeline")

default_args = {
    "owner": "tradestream",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": send_telegram_alert,
}

@dag(
    dag_id="realtime_cold_path_pipeline",
    default_args=default_args,
    description="Real-Time Cold Path Medallion Pipeline (Kafka -> Bronze -> Silver -> Postgres)",
    schedule="*/5 * * * *",  # Chạy mỗi 5 phút một lần (Micro-batch)
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=["tradestream", "realtime", "cold-path", "spark"],
    max_active_runs=1,
)
def realtime_cold_path_pipeline():
    
    ingest_kafka_to_bronze = DockerSparkSubmitOperator(
        task_id="ingest_kafka_to_bronze",
        script_path="/opt/airflow/src/processing/tradestream/ingest_raw_to_bronze.py",
        poll_interval=10
    )

    transform_bronze_to_silver = DockerSparkSubmitOperator(
        task_id="transform_bronze_to_silver",
        script_path="/opt/airflow/src/processing/tradestream/transform_bronze_to_silver.py",
        poll_interval=10
    )

    sync_silver_to_postgres = DockerSparkSubmitOperator(
        task_id="sync_silver_to_postgres",
        script_path="/opt/airflow/src/processing/tradestream/sync_silver_to_postgres.py",
        poll_interval=10
    )

    # Thứ tự thực thi tuần tự trong pipeline
    ingest_kafka_to_bronze >> transform_bronze_to_silver >> sync_silver_to_postgres

# Khởi tạo DAG
pipeline = realtime_cold_path_pipeline()
