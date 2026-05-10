# 🧠 Learning Log - TradeStream

*Nhật ký học tập dành cho bộ não ADHD. Chỉ ghi những gì thực sự hiểu và thấy thú vị. Không cần văn hoa, copy/paste thoải mái.*

---

## Phase 0: Foundation

### Ngày 1 (Hôm nay)
- Đã khởi động dự án dù năng lượng thấp.
- Đã có hệ thống tracking tự động bằng AI Mentor.
- **Bài học rút ra**: Không cần phải lúc nào cũng 100% năng lượng. Những ngày mệt mỏi, làm việc nhỏ (như setup docs) vẫn tốt hơn là không làm gì. Pipeline vẫn ở đó chờ mình.
- **Kiến thức**: Sự khác biệt giữa REST API (Polling) và WebSocket. 
  - **REST API**: Khách chủ động hỏi, Server trả lời (VD: Liên tục gọi điện hỏi "Pizza xong chưa?"). Dùng để lấy giá Stock từ Yahoo Finance mỗi 1 phút.
  - **WebSocket**: Mở kết nối 1 lần, Server tự động đẩy data về (VD: Nhà hàng tự gọi báo "Pizza xong rồi"). Dùng để lấy giá Crypto real-time từ Binance.
- **Business Insight**: Tại sao Yahoo Finance (Stock) dùng REST API thay vì WebSocket như Binance (Crypto)?
  1. **Giờ hoạt động**: Stock có giờ đóng/mở cửa, nghỉ cuối tuần. Duy trì WebSocket 24/7 là lãng phí tài nguyên.
  2. **Chi phí**: Data WebSocket tick-by-tick của chứng khoán rất đắt tiền (thường bán cho quỹ đầu tư). Yahoo cung cấp data miễn phí nên dùng REST API (trễ 15s - 1 phút) để tiết kiệm server.
  3. **Đối tượng**: Nhà đầu tư cá nhân không cần độ trễ mili-giây như High Frequency Trading bots.
- **Thực tiễn Data Engineering**: Khác với dữ liệu chứng khoán truyền thống, các sàn Crypto lớn như **Binance cung cấp luồng dữ liệu WebSocket thị trường (giá cả, lệnh mua bán) hoàn toàn MIỄN PHÍ và MỞ PUBLIC**. Bạn thậm chí không cần đăng ký tài khoản hay tạo API Key để lấy dữ liệu này. Đây là lý do dữ liệu Crypto luôn là lựa chọn số 1 để học và làm các dự án Real-time Streaming Data Pipeline.
- **Kiến thức**: Hiểu về Docker qua phép ẩn dụ "Nhà hàng":
  1. **Dockerfile (Công thức nấu ăn)**: Tờ giấy hướng dẫn cài đặt môi trường.
  2. **Image (Đồ ăn đông lạnh)**: Gói phần mềm đã đóng gói sẵn (OS, Code, Thư viện). Tải trên mạng về (Docker Hub) và không thể thay đổi.
  3. **Container (Bữa ăn đang dọn ra bàn)**: Là khi Image được "bật" lên và đang chạy. Bạn có thể bật 100 containers từ 1 Image (giống hệt nhau).
  4. **Volume (Cái tủ lạnh)**: Container khi tắt đi là mất hết dữ liệu. Volume là tủ lạnh cắm ở ngoài, ghi dữ liệu vào đó thì dù container có bị "giết", dữ liệu vẫn còn.
  5. **Docker Compose (Bếp trưởng)**: Bản giao việc để bật một lúc nhiều containers (Kafka, Zookeeper) theo đúng thứ tự mà không phải gõ tay từng lệnh.
- **Kiến thức**: 4 nguyên tắc vàng để viết Dockerfile chuẩn Expert (Tối ưu & An toàn):
  1. **Layer Caching (Copy file cài đặt trước)**: Luôn copy `requirements.txt` và chạy `pip install` TRƯỚC KHI copy toàn bộ code. Nếu code thay đổi nhưng thư viện không đổi, Docker sẽ dùng lại cache, giúp build nhanh gấp 10 lần.
  2. **Multi-stage Build (Không bê cả cái bếp đi ship đồ ăn)**: Dùng một môi trường (stage) nặng để cài đặt và compile code, sau đó chỉ copy những file chạy được sang một môi trường (stage) thứ hai mỏng nhẹ để mang đi chạy thực tế.
  3. **Non-root User (Bảo mật)**: Mặc định Docker chạy bằng quyền cao nhất (root). Nếu bị hack, hacker sẽ kiểm soát cả hệ thống. Luôn tạo một "user thường" ở cuối Dockerfile để chạy app.
  4. **.dockerignore**: Bắt buộc phải có file này để loại bỏ các thư mục rác (như `venv/`, `.git/`, `.env`) giúp giảm dung lượng gửi cho Docker.

## Bản đồ kỹ năng Data Engineer (The Utility Belt)
Một Data Engineer thực thụ không phải là người biết code nhiều nhất, mà là người biết **lắp ráp các mảnh ghép** thành một hệ thống tự động, trơn tru và rẻ tiền. Dưới đây là 5 "vai trò" bạn phải đóng vai:
1. **Thợ sửa ống nước (Streaming & Batch Pipelines)**: Xây dựng đường ống dẫn nước từ nguồn về hồ chứa. *Vũ khí*: Apache Kafka (Phase 1 dự án chúng ta), Apache Spark, dbt.
2. **Kiến trúc sư kho bãi (Data Modeling & Warehousing)**: Thiết kế cái hồ chứa sao cho dễ lấy nước nhất (Star Schema, Data Vault). *Vũ khí*: Apache Iceberg, Trino, Snowflake, PostgreSQL.
3. **Cảnh sát giao thông (Orchestration)**: Đứng ở ngã tư để chỉ đạo: "Mấy giờ ống nước này chạy, ống nước kia dừng, cái nào hỏng thì báo động". *Vũ khí*: Apache Airflow, Prefect.
4. **Chuyên gia nền tảng (DevOps & Infra)**: Xây dựng móng nhà, cài đặt máy chủ tự động. *Vũ khí*: Docker (bạn vừa thành thạo ở Phase 0!), Kubernetes, Git.
5. **Đội trưởng kiểm định (Quality & Governance)**: Lọc cặn bã trong nước, đảm bảo nước sạch 100% trước khi cho người dùng uống. *Vũ khí*: Great Expectations, DataHub.
