# ============================================================
# FILE: daily_batch.py
# MỤC ĐÍCH: Điều phối Real-Time Cold Path Pipeline (5-minute micro-batch)
# ============================================================

from datetime import datetime, timedelta
import subprocess
import logging
from airflow.decorators import dag, task

logger = logging.getLogger("realtime_cold_path_pipeline")

default_args = {
    "owner": "tradestream",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
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
    
    # Định nghĩa các JARs dùng chung
    JARS = ",".join([
        "/opt/spark/user-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar",
        "/opt/spark/user-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar",
        "/opt/spark/user-jars/kafka-clients-3.4.1.jar",
        "/opt/spark/user-jars/commons-pool2-2.12.0.jar",
        "/opt/spark/user-jars/postgresql-42.6.0.jar",
    ])

    def run_spark_script(script_path: str):
        cmd = [
            "docker", "exec", "spark-master",
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "--jars", JARS,
            script_path
        ]
        logger.info(f"Running command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Spark script {script_path} failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")
        logger.info(f"STDOUT:\n{result.stdout}")
        return result.stdout

    @task
    def ingest_kafka_to_bronze():
        """Task 1: Đọc từ Kafka và ghi JSON thô xuống Bronze Layer trên MinIO"""
        logger.info("Executing Ingestion Task (Kafka -> Bronze)...")
        run_spark_script("/opt/airflow/src/processing/ingest_raw_to_bronze.py")

    @task
    def transform_bronze_to_silver():
        """Task 2: Gom nhóm Ticks thành nến ngày (OHLCV) và MERGE INTO Silver Iceberg"""
        logger.info("Executing Transformation Task (Bronze -> Silver)...")
        run_spark_script("/opt/airflow/src/processing/transform_bronze_to_silver.py")

    @task
    def sync_silver_to_postgres():
        """Task 3: Đồng bộ dữ liệu mới nhất từ Silver Iceberg sang Postgres (Serving Layer)"""
        logger.info("Executing Sync Task (Silver -> Postgres)...")
        run_spark_script("/opt/airflow/src/processing/sync_silver_to_postgres.py")

    # Thứ tự thực thi tuần tự trong pipeline
    ingest_kafka_to_bronze() >> transform_bronze_to_silver() >> sync_silver_to_postgres()

# Khởi tạo DAG
pipeline = realtime_cold_path_pipeline()
