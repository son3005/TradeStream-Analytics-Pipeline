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

---

## Phase 1: Kafka 📨

### Ngày 2 (Thực hành Kafka Producer)
- **Hoàn thành**: Cập nhật cả 2 script (Crypto & Stock) để không chỉ in log ra màn hình mà còn gửi thẳng vào Kafka.
- **Kiến thức**: Apache Kafka hoạt động giống như một **TRUNG TÂM BƯU ĐIỆN KHỔNG LỒ**:
  1. **Kafka Cluster**: Tòa nhà bưu điện trung tâm (Chạy trên Docker ở `localhost:9092`).
  2. **Topic**: Các hộp thư phân loại. Mình vừa tạo 2 hộp thư là `crypto_trades` và `stock_trades`.
  3. **Producer (Người gửi)**: Là các đoạn code Python mà mình vừa viết (`crypto_producer.py` và `stock_producer.py`).
  4. **Consumer (Người nhận)**: Hiện tại chưa có. Bưu điện cứ giữ thư ở đó vài ngày, bao giờ mình viết code Consumer (Spark) thì mới ra lấy.
- **Kiến thức**: Thư viện `confluent_kafka`:
  - Đây là "nhân viên chuyển phát nhanh" xịn nhất trong giới Python. Dưới gầm máy nó được viết bằng ngôn ngữ C (`librdkafka`) nên tốc độ gửi thư cực kỳ khủng khiếp (hàng triệu tin nhắn/giây).
  - 3 Lệnh quan trọng nhất để làm việc với nhân viên này:
    3. `producer.flush()`: Khi chuẩn bị tắt máy nghỉ làm, phải gọi lệnh này để bắt nhân viên đem nốt những lá thư đang cầm trên tay đi gửi, không được vứt dọc đường.

### Ngày 2 (Tiếp tục) - Trả lời câu hỏi kiến trúc
- **Câu hỏi**: Nếu Server Kafka (hoặc Topic) bị chết/cháy ổ cứng thì mất hết dữ liệu à? Có cơ chế nào bảo vệ không?
- **Trả lời**: Kafka không bao giờ chạy 1 mình trên 1 máy, mà nó chạy thành một **Cụm (Cluster)** gồm nhiều máy tính ghép lại. Nó có 2 cơ chế "bảo hiểm" cực mạnh:
  1. **Replication (Nhân bản)**: Khi bạn gửi 1 tin nhắn vào Topic, Kafka không chỉ lưu ở máy số 1, mà nó tự động "photocopy" ra máy số 2 và máy số 3. Nếu máy số 1 bốc cháy, dữ liệu vẫn còn nguyên ở máy số 2.
  2. **Zookeeper (Ông bầu/Quản lý)**: Trong hệ thống luôn có một dịch vụ tên là Zookeeper đi kèm (bạn có thể thấy nó trong file `docker-compose.yml`). Nhiệm vụ của nó là cứ 2 giây lại "nháy máy" các máy chủ Kafka. Nếu máy 1 không nghe máy (chết), Zookeeper lập tức hô hoán: *"Máy 1 chết rồi, mọi người chuyển qua đọc dữ liệu ở máy 2 đi!"*. Toàn bộ quá trình này diễn ra tự động trong vài mili-giây, người dùng hoàn toàn không hề hay biết!

### Ngày 2 (Tiếp tục) - Khám phá Kafka-UI và Quản trị Kafka
- **Kafka-UI là gì?**: Nó là cái màn hình điều khiển (Dashboard) để giám sát toàn bộ cái Bưu điện Kafka (địa chỉ `localhost:8080`).
- **Giải mã các thuật ngữ trên Kafka-UI**:
  1. `tradestream` **(Cluster)**: Tên của cả hệ thống Bưu điện (do mình tự đặt trong file Docker Compose).
  2. **Brokers**: Chính là số lượng Server vật lý (Máy chủ) đang chạy. Hiện tại mình cấu hình chạy 1 Broker (Máy số 1). Ở các cty lớn, chỗ này sẽ hiện 3, 5 hoặc 10 Brokers.
  3. **Topics**: Là các Nhóm Chat/Hộp thư. Nơi bạn click vào để "nhìn lén" dữ liệu (Messages) đang bay qua bay lại.
  4. **Consumers**: Danh sách những người đang "nằm vùng" đọc tin nhắn. Khi sang Phase 2 (Apache Spark), bạn sẽ thấy Spark xuất hiện chễm chệ ở mục này.
- **3 Bí kíp quản trị Kafka hiệu quả (Dành cho Kỹ sư)**:
  1. **Retention (Dọn rác định kỳ)**: Kafka không phải database để lưu vĩnh viễn. Phải cài đặt "Chỉ giữ tin nhắn trong 7 ngày, sau đó tự xóa". Nếu không, rác sẽ làm nổ tung ổ cứng server!
  3. **Giám sát Consumer Lag**: Từ quan trọng nhất trong quản lý Kafka là **"LAG"** (Độ trễ). Nghĩa là: Producer bắn 100 tin/giây, nhưng Consumer (Spark) chỉ đọc kịp 50 tin/giây. Dữ liệu sẽ bị dồn ứ lại. Nhìn vào mục Consumers trên UI, nếu thấy cột `Lag` nhảy số liên tục, bạn phải lập tức nâng cấp sức mạnh cho máy Consumer!

### Ngày 2 (Tiếp tục) - Hiểu sâu về tin nhắn (Partition & Offset)
Khi bạn bấm vào xem một tin nhắn trong Kafka-UI, bạn sẽ luôn thấy 2 thông số đi kèm là `Partition` và `Offset`. Chúng là "trái tim" giúp Kafka hoạt động với tốc độ ánh sáng mà không bao giờ nhầm lẫn:

1. **Partition (Làn đường giao thông)**
   - Hãy tưởng tượng Topic `crypto_trades` là một con đường cao tốc.
   - Nếu đường chỉ có 1 làn xe (1 Partition), các xe (tin nhắn) sẽ phải nối đuôi nhau đi rất chậm.
   - Khi dữ liệu quá nhiều, Kafka cho phép bạn chia Topic đó thành 3, 5, hoặc 100 làn đường (Partitions). Lúc này, tin nhắn sẽ được phân bổ đều ra các làn để chạy song song. Ở phía cuối đường, bạn cũng có thể gắn 100 máy Consumer để hút dữ liệu cùng lúc. Đây chính là bí mật đằng sau khả năng mở rộng (Scale) vô hạn của Kafka.
2. **Offset (Số thứ tự/Số báo danh)**
   - Bên trong mỗi làn đường (Partition), mỗi một tin nhắn khi đi vào sẽ được cấp một số thứ tự tăng dần vĩnh viễn (0, 1, 2, 3...). Số này gọi là **Offset**.
   - **Tác dụng thần thánh của nó**: Hãy tưởng tượng Consumer (App Spark) là một người đang đọc một cuốn sách. Hôm nay nó đọc tới trang 500 (Offset = 500) thì bị cúp điện. Khi có điện lại, nó chỉ việc nói với Kafka: *"Ê, hôm trước tao đọc tới Offset 500 rồi, hãy đưa tao đọc tiếp từ Offset 501 đi"*. Nhờ cái số Offset này, hệ thống sẽ KHÔNG BAO GIỜ bị đọc sót tin nhắn, cũng KHÔNG BAO GIỜ đọc trùng một tin nhắn 2 lần!
