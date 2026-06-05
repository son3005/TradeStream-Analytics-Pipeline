# ============================================================
# FILE: docker_spark_operator.py
# MỤC ĐÍCH: Custom Airflow Operator và Trigger để chạy Spark Job
#            ở chế độ không đồng bộ (Deferrable Mode)
# ============================================================

import asyncio
import logging
import subprocess
import uuid
from typing import Any, Dict, Optional, Sequence

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, Variable
from airflow.triggers.base import BaseTrigger, TriggerEvent

logger = logging.getLogger("airflow.task")

async def run_command_async(cmd: Sequence[str]) -> tuple[int, str, str]:
    """Chạy một lệnh shell không đồng bộ và trả về exit code, stdout, stderr.

    Args:
        cmd (Sequence[str]): Danh sách các phần tử lệnh cần thực thi.

    Returns:
        tuple[int, str, str]: Bộ ba giá trị gồm:
            - exit_code (int): Mã thoát của tiến trình.
            - stdout (str): Chuỗi đầu ra tiêu chuẩn.
            - stderr (str): Chuỗi đầu ra lỗi tiêu chuẩn.
    """
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    return proc.returncode, stdout.decode().strip(), stderr.decode().strip()

class DockerSparkJobTrigger(BaseTrigger):
    """Trigger kiểm tra trạng thái của Spark job không đồng bộ bằng cách kiểm tra file status.

    Attributes:
        container_name (str): Tên container Docker chạy Spark Master.
        status_file (str): Đường dẫn đến tệp lưu trạng thái exit code của Spark job.
        poll_interval (int): Khoảng thời gian (giây) giữa các lần kiểm tra.
    """
    def __init__(self, container_name: str, status_file: str, log_file: str, poll_interval: int = 10):
        super().__init__()
        self.container_name = container_name
        self.status_file = status_file
        self.log_file = log_file
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, Dict[str, Any]]:
        """Tuần tự hóa Trigger để gửi cho Triggerer process.

        Returns:
            tuple[str, Dict[str, Any]]: Tên class Trigger và các tham số khởi tạo.
        """
        return (
            "tradestream.operators.docker_spark_operator.DockerSparkJobTrigger",
            {
                "container_name": self.container_name,
                "status_file": self.status_file,
                "log_file": self.log_file,
                "poll_interval": self.poll_interval
            }
        )

    async def run(self):
        """Hàm thực thi chính của Trigger chạy không đồng bộ trong Triggerer.

        Yields:
            TriggerEvent: Sự kiện báo cáo trạng thái hoàn tất của Spark job.
        """
        cmd = ["docker", "exec", self.container_name, "cat", self.status_file]
        while True:
            try:
                code, out, err = await run_command_async(cmd)
                if code == 0:
                    # File status đã tồn tại, đọc exit code
                    exit_code = int(out.strip())
                    yield TriggerEvent({
                        "status": "complete",
                        "exit_code": exit_code,
                        "status_file": self.status_file,
                        "log_file": self.log_file
                    })
                    return
            except Exception as e:
                logger.error(f"Lỗi khi kiểm tra file status của Spark: {e}")

            await asyncio.sleep(self.poll_interval)

class DockerSparkSubmitOperator(BaseOperator):
    """Custom Operator thực hiện gửi Spark Job vào Docker container và trì hoãn không đồng bộ.

    Attributes:
        script_path (str): Đường dẫn tới script Spark bên trong container.
        spark_master_container (str): Tên container Spark Master.
        poll_interval (int): Khoảng thời gian thăm dò trạng thái.
    """
    template_fields: Sequence[str] = ("script_path",)

    def __init__(
        self,
        script_path: str,
        spark_master_container: str = "spark-master",
        poll_interval: int = 10,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.script_path = script_path
        self.spark_master_container = spark_master_container
        self.poll_interval = poll_interval
        self.status_file = ""
        self.log_file = ""

    def execute(self, context: Dict[str, Any]) -> None:
        """Thực thi Operator: Lấy cấu hình nhạy cảm, gửi Spark job chạy ngầm, và defer task.

        Args:
            context (Dict[str, Any]): Ngữ cảnh thực thi task của Airflow.
        """
        # 1. Truy xuất Connection và Variables một cách an toàn từ Airflow Vault
        try:
            db_conn = BaseHook.get_connection("timescaledb_conn")
            db_conn_str = db_conn.get_uri()
        except Exception:
            self.log.warning("Connection 'timescaledb_conn' không tìm thấy, sử dụng cấu hình mặc định.")
            db_conn_str = ""

        minio_user = Variable.get("minio_root_user", default_var="")
        minio_pass = Variable.get("minio_root_password", default_var="")
        minio_bucket = Variable.get("minio_lakehouse_bucket", default_var="")
        minio_endpoint = Variable.get("minio_endpoint", default_var="")

        # 2. Xây dựng danh sách các biến môi trường để truyền động cho container
        env_opts = []
        if db_conn_str:
            env_opts += ["-e", f"TIMESCALE_CONN={db_conn_str}"]
        if minio_user:
            env_opts += ["-e", f"MINIO_ROOT_USER={minio_user}"]
        if minio_pass:
            env_opts += ["-e", f"MINIO_ROOT_PASSWORD={minio_pass}"]
        if minio_bucket:
            env_opts += ["-e", f"MINIO_LAKEHOUSE_BUCKET={minio_bucket}"]
        if minio_endpoint:
            env_opts += ["-e", f"MINIO_ENDPOINT={minio_endpoint}"]

        # Tạo mã định danh duy nhất cho file status/log để tránh xung đột khi chạy song song
        run_id = str(uuid.uuid4())[:8]
        script_name = self.script_path.split("/")[-1].replace(".py", "")
        self.status_file = f"/tmp/{script_name}_{run_id}.status"
        self.log_file = f"/tmp/{script_name}_{run_id}.log"

        JARS = ",".join([
            "/opt/spark/user-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar",
            "/opt/spark/user-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar",
            "/opt/spark/user-jars/kafka-clients-3.4.1.jar",
            "/opt/spark/user-jars/commons-pool2-2.12.0.jar",
            "/opt/spark/user-jars/postgresql-42.6.0.jar",
            "/opt/spark/user-jars/openlineage-spark_2.12-1.15.0.jar",
        ])

        # 3. Chạy lệnh ngầm bằng docker exec -d
        cmd = [
            "docker", "exec", "-d", "-u", "root", "-e", "PYTHONPATH=/opt/airflow"
        ] + env_opts + [
            self.spark_master_container,
            "bash", "-c",
            f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 1 --jars {JARS} {self.script_path} > {self.log_file} 2>&1; echo $? > {self.status_file}"
        ]

        self.log.info(f"Đang gửi Spark job chạy ngầm: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Lỗi khi gửi Spark job: STDOUT: {result.stdout}\nSTDERR: {result.stderr}")

        # 4. Defer execution: Trì hoãn task và nhường slot cho Triggerer
        self.defer(
            trigger=DockerSparkJobTrigger(
                container_name=self.spark_master_container,
                status_file=self.status_file,
                log_file=self.log_file,
                poll_interval=self.poll_interval
            ),
            method_name="execute_complete"
        )

    def execute_complete(self, context: Dict[str, Any], event: Optional[Dict[str, Any]] = None) -> str:
        """Hàm callback được gọi lại sau khi Trigger kết thúc để xử lý kết quả.

        Args:
            context (Dict[str, Any]): Ngữ cảnh task.
            event (Optional[Dict[str, Any]]): Sự kiện truyền từ Trigger.

        Returns:
            str: Thông báo kết quả thực thi.
        """
        if not event:
            raise Exception("Không nhận được sự kiện hợp lệ từ Triggerer.")

        if event.get("status") == "complete":
            exit_code = event.get("exit_code")
            status_file = event.get("status_file")
            log_file = event.get("log_file")
            self.log.info(f"Spark job đã hoàn thành với mã thoát: {exit_code}")

            # Đọc và log 50 dòng cuối cùng từ tệp log của Spark
            if log_file:
                log_cmd = ["docker", "exec", self.spark_master_container, "tail", "-n", "50", log_file]
                log_res = subprocess.run(log_cmd, capture_output=True, text=True)
                self.log.info(f"Spark Job logs thực tế (50 dòng cuối):\n{log_res.stdout}")

            # Dọn dẹp các tệp tạm trong container
            if status_file or log_file:
                cleanup_files = []
                if status_file:
                    cleanup_files.append(status_file)
                if log_file:
                    cleanup_files.append(log_file)
                cleanup_cmd = ["docker", "exec", self.spark_master_container, "rm", "-f"] + cleanup_files
                subprocess.run(cleanup_cmd)

            if exit_code != 0:
                raise Exception(f"Spark job thất bại với exit code {exit_code}.")

            return "Spark Job đã chạy thành công."
        else:
            raise Exception(f"Spark trigger báo lỗi: {event.get('message')}")
