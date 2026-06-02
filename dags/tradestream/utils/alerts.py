# ============================================================
# FILE: alerts.py
# MỤC ĐÍCH: Hỗ trợ gửi thông báo cảnh báo qua Telegram/Slack khi task thất bại
# ============================================================

import os
import html
import logging
import requests
from typing import Any, Dict, Optional
from airflow.models import Variable

logger = logging.getLogger("airflow.task")

def send_telegram_alert(context: Dict[str, Any]) -> None:
    """Hàm callback được gọi khi Airflow Task thất bại để gửi tin nhắn cảnh báo qua Telegram.

    Args:
        context (Dict[str, Any]): Ngữ cảnh thực thi task của Airflow chứa các thông tin metadata.
    """
    # 1. Thu thập các thông tin metadata của task bị lỗi
    ti = context.get("task_instance")
    if not ti:
        logger.warning("Không tìm thấy TaskInstance trong context.")
        return

    dag_id: str = getattr(ti, "dag_id", "Unknown_DAG")
    task_id: str = getattr(ti, "task_id", "Unknown_Task")
    execution_date = context.get("logical_date") or getattr(ti, "logical_date", getattr(ti, "start_date", "N/A"))
    exception = context.get("exception")
    log_url: str = getattr(ti, "log_url", "N/A")
    
    # Nếu log_url là N/A (thường xảy ra ở Airflow 3.x Task SDK do ti bị giới hạn thuộc tính), tự xây dựng URL thủ công
    if not log_url or log_url == "N/A":
        dag_run = context.get("dag_run")
        run_id = getattr(ti, "run_id", None) or (getattr(dag_run, "run_id", None) if dag_run else None)
        try_number = getattr(ti, "try_number", 1)
        if run_id:
            from urllib.parse import quote
            run_id_encoded = quote(str(run_id))
            base_url = os.environ.get("AIRFLOW__WEBSERVER__BASE_URL") or "http://localhost:8085"
            base_url = base_url.rstrip("/")
            log_url = f"{base_url}/dags/{dag_id}/runs/{run_id_encoded}/tasks/{task_id}?try_number={try_number}"
    
    # Hỗ trợ ánh xạ lại port truy cập từ máy host (thay thế localhost thành 127.0.0.1 để Telegram nhận diện là URL hợp lệ)
    if log_url and log_url != "N/A":
        log_url = log_url.replace("localhost:8080", "127.0.0.1:8085")
        log_url = log_url.replace("localhost:8085", "127.0.0.1:8085")
        log_url = log_url.replace("airflow-webserver:8080", "127.0.0.1:8085")
        log_url = log_url.replace("0.0.0.0:8080", "127.0.0.1:8085")
        log_url = log_url.replace("localhost", "127.0.0.1")




    # 2. Truy xuất thông tin cấu hình Telegram Bot từ biến môi trường hoặc Airflow Variables
    token: Optional[str] = os.environ.get("TELEGRAM_BOT_TOKEN") or Variable.get("telegram_bot_token", default_var=None)
    chat_id: Optional[str] = os.environ.get("TELEGRAM_CHAT_ID") or Variable.get("telegram_chat_id", default_var=None)

    if not token or not chat_id:
        logger.info("Telegram Bot token hoặc chat_id chưa được cấu hình. Bỏ qua gửi cảnh báo.")
        return

    # 3. Tạo nội dung tin nhắn báo lỗi dạng HTML (để tránh lỗi parser Markdown với dấu gạch dưới trong URL)
    dag_id_escaped = html.escape(str(dag_id))
    task_id_escaped = html.escape(str(task_id))
    execution_date_escaped = html.escape(str(execution_date))
    exception_escaped = html.escape(str(exception))

    message: str = (
        f"🚨 <b>Airflow Task Failure Alert</b> 🚨\n\n"
        f"<b>DAG:</b> <code>{dag_id_escaped}</code>\n"
        f"<b>Task:</b> <code>{task_id_escaped}</code>\n"
        f"<b>Thời gian:</b> <code>{execution_date_escaped}</code>\n"
        f"<b>Lỗi phát sinh:</b> <code>{exception_escaped}</code>\n\n"
        f"🔗 <a href=\"{log_url}\">Xem chi tiết logs tại đây</a>"
    )

    url: str = f"https://api.telegram.org/bot{token}/sendMessage"
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }

    # 4. Gửi request tới Telegram Bot API
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info(f"Đã gửi cảnh báo lỗi thành công tới Telegram cho task: {task_id}.")
        else:
            logger.error(f"Gửi cảnh báo tới Telegram thất bại. Mã lỗi: {response.status_code}, Nội dung: {response.text}")
    except Exception as e:
        logger.error(f"Gặp lỗi khi kết nối tới Telegram API: {e}")
