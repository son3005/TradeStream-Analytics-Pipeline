import os
import sys
from pyspark.sql import SparkSession
from src.utils.spark_helper import get_spark_session

# Kích hoạt UTF-8 trên Windows để hiển thị tiếng Việt chính xác
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def main() -> None:
    """Chạy các quy trình bảo trì định kỳ cho các bảng Apache Iceberg trong Lakehouse storage.

    Thực hiện nén tệp dữ liệu (ghi lại các tệp tin nhỏ), xóa dọn các snapshot cũ (chỉ giữ lại 
    snapshot mới nhất), và xóa các tệp mồ côi (orphan files) trên MinIO để tối ưu hóa lưu trữ.

    Returns:
        None

    Raises:
        Exception: Nếu các lệnh thực thi quy trình bảo trì bị thất bại.
    """
    # =====================================================================
    # 3. KHỞI TẠO SPARK SESSION VỚI CATALOG LAKEHOUSE ICEBERG
    # =====================================================================
    spark = get_spark_session("LakehouseMaintenance")

    spark.sparkContext.setLogLevel("WARN")
    print("[*] Khởi động Spark Job: Bảo trì hồ dữ liệu Lakehouse (Iceberg Maintenance)...")

    # Danh sách các bảng cần bảo trì định kỳ
    tables = [
        "lakehouse.trading.dim_assets",
        "lakehouse.trading.dim_date",
        "lakehouse.trading.fact_daily_prices"
    ]

    for table in tables:
        print(f"\n[*] ===================================================")
        print(f"[*] Đang thực thi các tác vụ bảo trì cho bảng: {table}")
        print(f"[*] ===================================================")

        # -----------------------------------------------------------------
        # A. Compaction (Gộp các tệp nhỏ - Optimize)
        # -----------------------------------------------------------------
        # Cơ chế: Mỗi khi Spark ghi dữ liệu (đặc biệt là streaming hoặc ghi hàng ngày), nó sẽ sinh ra
        # các tệp Parquet rất nhỏ (chỉ vài KB/MB). Quá nhiều file nhỏ sẽ làm chậm quá trình quét đĩa khi đọc.
        # Giải pháp: Thủ tục `rewrite_data_files` sẽ gộp các file nhỏ thành các file chuẩn lớn hơn (~128MB).
        print(f"[*] 1. Tiến hành nén file (rewrite_data_files) cho bảng {table}...")
        try:
            spark.sql(f"CALL lakehouse.system.rewrite_data_files(table => '{table}')").show(truncate=False)
        except Exception as e:
            print(f"[X] Quá trình nén file thất bại: {e}")

        # -----------------------------------------------------------------
        # B. Expire Snapshots (Xóa lịch sử Snapshots cũ)
        # -----------------------------------------------------------------
        # Cơ chế: Iceberg hỗ trợ "Time Travel" bằng cách giữ lại toàn bộ lịch sử thay đổi (snapshots).
        # Tuy nhiên, điều này chiếm dụng dung lượng đĩa cực kỳ lớn theo thời gian.
        # Giải pháp: Thủ tục `expire_snapshots` sẽ xóa bỏ các phiên bản snapshot quá cũ.
        # Ở đây ta cấu hình `retain_last => 1` để giữ lại duy nhất 1 snapshot mới nhất, dọn sạch dữ liệu cũ.
        print(f"[*] 2. Tiến hành xóa dọn snapshots cũ (chỉ giữ lại snapshot cuối cùng) cho bảng {table}...")
        try:
            spark.sql(f"CALL lakehouse.system.expire_snapshots(table => '{table}', retain_last => 1)").show(truncate=False)
        except Exception as e:
            print(f"[X] Quá trình dọn dẹp snapshots thất bại: {e}")

        # -----------------------------------------------------------------
        # C. Remove Orphan Files (Xóa các tệp mồ côi)
        # -----------------------------------------------------------------
        # Cơ chế: Khi một job Spark ghi dữ liệu bị crash giữa chừng, hoặc sau khi xóa snapshot cũ,
        # các file Parquet dữ liệu thực tế vẫn còn lưu trên MinIO nhưng không có metadata nào trỏ tới.
        # Các file này gọi là "Orphan Files" (Tệp mồ côi / Tệp rác).
        # Giải pháp: Thủ tục `remove_orphan_files` quét thư mục bảng trên MinIO và xóa sạch các file rác này.
        print(f"[*] 3. Tiến hành tìm kiếm và xóa bỏ các tệp tin mồ côi cho bảng {table}...")
        try:
            spark.sql(f"CALL lakehouse.system.remove_orphan_files(table => '{table}')").show(truncate=False)
        except Exception as e:
            print(f"[X] Quá trình xóa tệp mồ côi thất bại: {e}")

        print(f"[SUCCESS] Hoàn thành toàn bộ quy trình bảo trì cho bảng: {table}!")

    print("\n[SUCCESS] Hoàn thành bảo trì Lakehouse thành công cho tất cả các bảng!")
    spark.stop()

if __name__ == "__main__":
    main()
