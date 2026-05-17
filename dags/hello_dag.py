# ============================================================
# FILE: hello_dag.py
# MỤC ĐÍCH: DAG đầu tiên — "Hello World" của Airflow
# ĐẶT Ở ĐÂU: Thư mục dags/ → Airflow tự quét và tìm thấy
# ============================================================

# ---- BƯỚC 1: Import các thư viện cần thiết ----

# "DAG" là class chính — đại diện cho 1 bản kế hoạch
from airflow import DAG

# "BashOperator" là loại công nhân chạy lệnh terminal
# (Chúng ta đã học: cần chạy lệnh terminal → dùng BashOperator)
from airflow.operators.bash import BashOperator

# "datetime" để khai báo ngày bắt đầu chạy DAG
from datetime import datetime

# ---- BƯỚC 2: Khai báo DAG (Bản kế hoạch) ----

# Cú pháp "with DAG(...) as dag:" nghĩa là:
# "Tạo 1 bản kế hoạch tên là 'hello_tradestream', và tất cả task
#  bên trong block này đều thuộc về bản kế hoạch đó"
with DAG(
    # "dag_id" — Tên định danh DUY NHẤT của DAG
    # Tên này sẽ hiện trên giao diện web Airflow
    # Quy ước: dùng snake_case, không có dấu cách
    dag_id="hello_tradestream",

    # "description" — Mô tả ngắn, hiện trên web UI khi hover vào tên DAG
    description="DAG đầu tiên - Hello World của Airflow",

    # "start_date" — Ngày BẮT ĐẦU lịch chạy
    # Airflow sẽ KHÔNG chạy DAG trước ngày này
    # Ví dụ: nếu để 2026-01-01, Airflow sẽ cố backfill từ ngày đó tới nay
    # → Để ngày hôm nay để tránh backfill không cần thiết
    start_date=datetime(2026, 5, 17),

    # "schedule" — Lịch chạy tự động:
    #   "@daily"   = mỗi ngày 1 lần (lúc 00:00 UTC)
    #   "@hourly"  = mỗi giờ 1 lần
    #   "*/5 * * * *" = mỗi 5 phút (cú pháp cron)
    #   None       = KHÔNG tự chạy, chỉ chạy khi bạn bấm nút thủ công
    # → Để None vì đây là DAG học tập, chúng ta sẽ bấm nút chạy thủ công
    schedule=None,

    # "catchup" — Có chạy bù các lần đã bỏ lỡ không?
    #   True  = nếu DAG lẽ ra chạy 30 ngày qua mà chưa chạy → chạy bù 30 lần
    #   False = bỏ qua, chỉ chạy từ bây giờ trở đi
    # → Luôn để False khi đang học, tránh Airflow chạy hàng trăm lần liên tục
    catchup=False,

    # "tags" — Nhãn để phân loại DAG trên web UI (giống tags trong Grafana)
    tags=["learning", "hello-world"],

) as dag:

    # ---- BƯỚC 3: Khai báo các Task (Công đoạn) ----

    # Task 1: In ra dòng chữ "Bắt đầu"
    # BashOperator = chạy 1 lệnh trên terminal
    task_start = BashOperator(
        # "task_id" — Tên định danh DUY NHẤT của task trong DAG này
        task_id="bat_dau",

        # "bash_command" — Lệnh terminal sẽ được chạy
        # Lệnh "echo" đơn giản chỉ in ra màn hình (giống print trong Python)
        bash_command='echo "🚀 Bắt đầu TradeStream Pipeline!"',
    )

    # Task 2: In ra ngày giờ hiện tại
    task_show_date = BashOperator(
        task_id="hien_thi_ngay",

        # Lệnh "date" trên Linux in ra ngày giờ hiện tại của hệ thống
        bash_command='echo "📅 Thời gian hiện tại:" && date',
    )

    # Task 3: In ra dòng chữ "Kết thúc"
    task_end = BashOperator(
        task_id="ket_thuc",
        bash_command='echo "✅ Pipeline hoàn thành!"',
    )

    # ---- BƯỚC 4: Khai báo THỨ TỰ chạy ----

    # Dấu ">>" nghĩa là "chạy xong cái bên trái, MỚI chạy cái bên phải"
    # Đây chính là chữ "D" trong DAG (Directed = có hướng)
    #
    # task_start >> task_show_date >> task_end
    # Nghĩa là:
    #   1. Chạy "bat_dau" TRƯỚC
    #   2. Xong rồi → chạy "hien_thi_ngay"
    #   3. Xong rồi → chạy "ket_thuc"
    #
    # Nếu "bat_dau" LỖI → "hien_thi_ngay" và "ket_thuc" KHÔNG chạy
    task_start >> task_show_date >> task_end
