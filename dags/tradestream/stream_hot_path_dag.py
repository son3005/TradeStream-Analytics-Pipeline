# ============================================================
# FILE: stream_hot_path_dag.py
# MUC DICH: Airflow DAG dieu phoi va giam sat Real-Time Hot Path Streaming Job.
#           Bao dam Spark Streaming luon chay 24/7 bang cach khoi dong ngam neu bi tat.
# ============================================================

from datetime import datetime, timedelta
import subprocess
import logging
from airflow.decorators import dag, task
from tradestream.utils.alerts import send_telegram_alert

logger = logging.getLogger("realtime_hot_path_pipeline")

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
    dag_id="realtime_hot_path_streaming_monitor",
    default_args=default_args,
    description="Giam sat va tu dong khoi dong lai Spark Structured Streaming Job (Hot Path)",
    schedule="*/5 * * * *",  # Quet kiem tra moi 5 phut
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=["tradestream", "realtime", "hot-path", "spark", "monitor"],
    max_active_runs=1,
)
def realtime_hot_path_streaming_monitor():
    """
    DAG dinh ky kiem tra xem process cua Spark Structured Streaming co hoat dong hay khong.
    Neu khong, se kich hoat ngam (daemonize) spark-submit.
    """
    
    # Dinh nghia cac JARs dung chung
    JARS = ",".join([
        "/opt/spark/user-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar",
        "/opt/spark/user-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar",
        "/opt/spark/user-jars/kafka-clients-3.4.1.jar",
        "/opt/spark/user-jars/commons-pool2-2.12.0.jar",
        "/opt/spark/user-jars/postgresql-42.6.0.jar",
    ])

    @task
    def monitor_and_restart_streaming() -> None:
        """
        Kiem tra process 'stream_hot_path.py' trong container spark-master.
        Neu khong ton tai thi khoi chay ngam.
        
        Returns:
            None
            
        Raises:
            Exception: Neu gap loi khi thuc thi lenh docker kiem tra hoac khoi chay.
        """
        script_path = "/opt/airflow/src/processing/tradestream/stream_hot_path.py"
        
        # 1. Kiem tra xem process dang chay hay khong bang pgrep
        check_cmd = [
            "docker", "exec", "spark-master",
            "pgrep", "-f", "stream_hot_path.py"
        ]
        
        logger.info(f"Checking streaming job status: {' '.join(check_cmd)}")
        check_result = subprocess.run(check_cmd, capture_output=True, text=True)
        
        # pgrep tra ve exit code = 0 neu thay process dang chay
        if check_result.returncode == 0:
            logger.info("[✓] Spark Structured Streaming job (Hot Path) is already running.")
            return

        # 2. Neu khong chay, tiến hanh start ngam bang cach dung option '-d' cua docker exec
        logger.warning("[!] Spark Structured Streaming job (Hot Path) is NOT running! Starting it now...")
        
        start_cmd = [
            "docker", "exec", "-d", "-u", "root", "-e", "PYTHONPATH=/opt/airflow", "spark-master",
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "--jars", JARS,
            script_path
        ]
        
        logger.info(f"Executing start command: {' '.join(start_cmd)}")
        start_result = subprocess.run(start_cmd, capture_output=True, text=True)
        
        if start_result.returncode != 0:
            raise Exception(
                f"Failed to start Spark streaming script. "
                f"STDOUT: {start_result.stdout}\nSTDERR: {start_result.stderr}"
            )
            
        logger.info("[SUCCESS] Spark Structured Streaming job has been launched in the background.")

    # Thuc thi task
    monitor_and_restart_streaming()

# Khoi tao DAG
pipeline = realtime_hot_path_streaming_monitor()
