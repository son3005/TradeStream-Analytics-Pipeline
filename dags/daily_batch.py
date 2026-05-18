# ============================================================
# FILE: daily_batch.py
# MỤC ĐÍCH: Kích hoạt Pipeline Lấy dữ liệu 100k+ Symbols (Decoupled Kafka + Spark)
# ============================================================

from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator

# ============================================================
# CẤU HÌNH CHUNG
# ============================================================

default_args = {
    "owner": "tradestream",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="daily_batch_decoupled",
    default_args=default_args,
    description="Decoupled Pipeline using Kafka (Ingestion) and Spark (Processing)",
    schedule="0 23 * * *", # 11:00 PM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tradestream", "batch", "kafka", "spark"],
    max_active_runs=1,
)
def daily_prices_pipeline():
    
    # 1. Tầng Ingestion: Gọi script Python Async để bắn 100k API và đẩy vào Kafka
    ingest_to_kafka = BashOperator(
        task_id="ingest_to_kafka",
        bash_command="python /opt/airflow/src/producers/yahoo_batch_producer.py --symbols-file /opt/airflow/config/symbols.json",
        append_env=True
    )
    
    # 2. Tầng Processing: spark-submit dùng JARs có sẵn trên disk (không cần internet)
    # JARs đã được download vào infrastructure/spark/jars/ và mount vào /opt/spark/user-jars/
    JARS = ",".join([
        "/opt/spark/user-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar",
        "/opt/spark/user-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar",
        "/opt/spark/user-jars/kafka-clients-3.4.1.jar",
        "/opt/spark/user-jars/commons-pool2-2.12.0.jar",
        "/opt/spark/user-jars/postgresql-42.6.0.jar",
    ])
    process_with_spark = BashOperator(
        task_id="process_with_spark",
        bash_command=(
            f"docker exec spark-master "
            f"/opt/spark/bin/spark-submit "
            f"--master spark://spark-master:7077 "
            f"--jars {JARS} "
            f"/opt/airflow/src/processing/spark_batch_processor.py"
        ),
        append_env=True
    )

    # 3. Luồng thực thi
    ingest_to_kafka >> process_with_spark

# Khởi tạo DAG
pipeline = daily_prices_pipeline()
