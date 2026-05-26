---
name: pipeline-visualizer
description: "Expert Visual Designer and Prompt Engineer for creating high-fidelity architectural pipeline diagrams, isometric system visualizations, and technical icons."
---

# 🎨 Pipeline Visual Architect & Prompt Engineer

## 🎯 Mục đích

Skill này đóng vai trò là một **Chuyên gia Thiết kế Trực quan & Kỹ sư Prompt (Prompt Engineer)** chuyên nghiệp. Hướng dẫn này cung cấp cấu trúc, quy chuẩn thiết kế và biểu mẫu prompt tối ưu để AI Agent (sử dụng Gemini 3.5 Flash hoặc các mô hình sinh ảnh khác) tạo ra các sơ đồ kiến trúc, hình ảnh dòng chảy dữ liệu (dataflow) và các biểu tượng hệ thống có tính thẩm mỹ cao, đồng bộ về mặt mỹ thuật và bố cục.

---

## 📐 Nguyên tắc Thiết kế Trực quan (Visual Standards)

Để tạo ra hình ảnh sơ đồ kiến trúc chuyên nghiệp và "WOW" người xem, prompt sinh ảnh cần tuân thủ 4 nguyên tắc cốt lõi:

1.  **Bố cục (Layout)**: Sử dụng góc nhìn **Isometric 3D (hình chiếu trục đo)** từ góc trên chéo. Dòng chảy dữ liệu di chuyển mạch lạc từ Trái qua Phải (Left-to-Right) hoặc từ Trên xuống Dưới.
2.  **Màu sắc (Color Palette - Cyberpunk Dark Mode)**:
    *   **Background**: Màu xám tối, xanh đen sâu thẳm `#0d1117` hoặc đen mờ (Matte Black) để làm nổi bật các thành phần phát sáng.
    *   **Hot Path (Real-time)**: Sử dụng luồng sáng neon màu Cam/Vàng (Orange/Amber Neon glow) biểu thị tốc độ cao và năng động.
    *   **Cold Path (Batch/Lakehouse)**: Sử dụng luồng sáng neon màu Xanh dương/Tím (Cyan/Purple Neon glow) biểu thị sự ổn định và lưu trữ sâu.
3.  **Ngôn ngữ Ký hiệu (Symbolic Language)**:
    *   **Kafka**: Cụm máy chủ (Server Nodes) dạng khối lập phương phát sáng màu xanh, kết nối với nhau bởi các sợi quang học truyền tin nhắn.
    *   **Apache Spark**: Động cơ lõi (Engine Core) dạng turbine hoặc lò phản ứng hình tròn phát sáng, có các tia lửa năng lượng vàng cam bao quanh thể hiện tính toán phân tán.
    *   **MinIO / Iceberg (Lakehouse)**: Khối băng nổi (Iceberg) dạng 3D thủy tinh bán trong suốt (Glassmorphism), bên trong chứa các khối dữ liệu hình lục giác phát sáng xanh lục (Bronze/Silver layers).
    *   **Serving DB (TimescaleDB)**: Cột trụ cơ sở dữ liệu (Database Cylinder) bằng kim loại tối với vòng LED tròn màu lam quanh thân biểu thị chuỗi thời gian (Timescale).
    *   **Trino / Analytics**: Tháp pha lê đa diện hoặc kính lúp quét holographic màu xanh ngọc.
    *   **Airflow**: Một luồng gió hoặc quạt đẩy dạng phản lực điều hướng các dòng sáng chạy qua lại.
    *   **Grafana**: Màn hình Hologram hiển thị biểu đồ sóng trực quan đa sắc màu bay lơ lửng phía cuối pipeline.

---

## ✍️ Biểu mẫu Prompt Sinh ảnh Đỉnh cao cho TradeStream Pipeline

Dưới đây là prompt mẫu đã được tối ưu hóa để đưa trực tiếp vào công cụ `generate_image`:

### 🎯 Prompt Tiếng Anh (Dành cho AI Image Generator):

```text
An isometric 3D system architecture diagram of a real-time financial data pipeline, titled "TradeStream Analytics Pipeline". Cyberpunk dark mode aesthetic with deep dark blue background. 

Layout flows from left to right:
1. Left side (Data Sources): Two glowing neon holographic globes representing "Binance WebSocket Streams" and "Yahoo Finance API", emitting orange and blue light beams.
2. Ingestion (Kafka): Light beams enter a cluster of futuristic modular servers connected by glowing optical fibers, representing Apache Kafka.
3. Processing & Lakehouse (Spark, MinIO, Iceberg): 
   - A central glowing fusion reactor engine representing Apache Spark processing the data.
   - The data is structured into a translucent glassmorphism iceberg floating on a dark liquid metal database floor. The iceberg has visible layers: a bottom raw layer (Bronze JSON), a middle refined layer (Silver Parquet), and a gold top layer.
4. Serving (PostgreSQL/TimescaleDB): Glowing blue database cylinders with time rings representing TimescaleDB receiving metrics from the iceberg.
5. Visualization (Grafana): Floating glowing holographic analytics dashboard screens showing vibrant candle charts and real-time line charts at the end of the pipeline.

Data paths:
- Hot path: Fast flowing bright neon amber/orange energy lines running directly from sources to serving database.
- Cold path: Steady glowing neon cyan/magenta energy lines flowing from sources through Kafka, into the Iceberg, and finally into the serving database.

Art style: Ultra-detailed, cinematic lighting, futuristic 3D vector graphics style, glassmorphism nodes, glowing paths, clean text-less symbols, high resolution, professional presentation, 8k.
```

---

## 🛠️ Quy trình thực hiện của AI Agent khi nhận lệnh tạo ảnh

Khi người dùng yêu cầu tạo ảnh cho pipeline dự án, Agent cần thực thi các bước sau:

1.  **Bước 1**: Đọc sơ đồ thiết kế kiến trúc hiện tại từ `README.md` hoặc `docs/progress.md` để đảm bảo không bị thiếu dịch vụ (ví dụ: Trino, Airflow, MinIO, Kafka, Spark).
2.  **Bước 2**: Sử dụng Prompt mẫu bên trên, hiệu chỉnh danh sách symbol hoặc tông màu nếu người dùng có yêu cầu đặc biệt.
3.  **Bước 3**: Gọi công cụ `generate_image` với prompt đã chuẩn hóa, đặt tên ảnh đầu ra là `tradestream_pipeline_architecture.png`.
4.  **Bước 4**: Hiển thị ảnh cho người dùng kèm giải thích chi tiết về bố cục thiết kế trực quan.
