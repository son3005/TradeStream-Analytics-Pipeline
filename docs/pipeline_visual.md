# 🎨 Sơ đồ Kiến trúc Trực quan - TradeStream Analytics Pipeline

Dưới đây là sơ đồ kiến trúc hệ thống 2D được thiết kế trực quan dưới dạng sơ đồ khối phẳng kèm biểu tượng (icons) của các công cụ sử dụng trong hệ thống, với các phân vùng lớp rõ ràng, các luồng dữ liệu Hot/Cold đi song song mạch lạc và không bị chéo/rối mắt.

## 🖼️ Sơ đồ Kiến trúc 2D (2D Simplified System Architecture Flowchart with Icons)

![Sơ đồ kiến trúc 2D chi tiết - TradeStream Analytics Pipeline](pipeline_2d_detailed_architecture.png)

---

## 🛠️ Quy trình và Cách thức cập nhật sơ đồ (HTML & SVG Source)

Sơ đồ trên được thiết kế và sinh tự động bằng mã HTML/CSS kết hợp hệ thống đường dẫn SVG chuẩn trong dự án để đảm bảo tuyệt đối không có lỗi sai lệch luồng.

Mã nguồn thiết kế nằm tại file: [docs/architecture_diagram.html](architecture_diagram.html)

**Cách cập nhật sơ đồ:**
1. Chỉnh sửa tọa độ các node và đường liên kết SVG trong file `docs/architecture_diagram.html`.
2. Khởi chạy HTTP server cục bộ (`python -m http.server 8000`).
3. Mở trình duyệt và truy cập `http://127.0.0.1:8000/docs/architecture_diagram.html`.
4. Điều chỉnh kích thước cửa sổ 1300x850 và chụp lại màn hình dưới dạng file PNG, ghi đè lên file `docs/pipeline_2d_detailed_architecture.png`.








