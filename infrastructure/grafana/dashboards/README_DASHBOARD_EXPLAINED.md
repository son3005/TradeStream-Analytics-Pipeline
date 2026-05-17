# 📖 Giải thích chi tiết file `tradestream_dashboard.json`

> **Tại sao không viết comment trực tiếp trong file JSON?**
> Vì JSON **không hỗ trợ comment** (khác với YAML). Đây là lý do nhiều người
> ghét JSON và thích YAML hơn khi viết file cấu hình.

---

## 🏗️ Cấu trúc tổng quan

File JSON này là **bản thiết kế** của một trang dashboard.
Grafana đọc file này và tự vẽ giao diện web theo đúng mô tả bên trong.

```
Dashboard (Trang tổng quan)
├── Panel 1: Biểu đồ đường (Time Series) — vẽ VWAP & SMA
│   └── Target A: Câu SQL lấy dữ liệu từ TimescaleDB
│
├── Panel 2: Bảng dữ liệu (Table) — hiển thị 50 dòng mới nhất
│   └── Target A: Câu SQL lấy dữ liệu từ TimescaleDB
│
└── Cài đặt chung: thời gian, refresh, timezone...
```

---

## 📝 Giải thích từng block

### Block 1: Thông tin chung của Dashboard

```jsonc
{
  // "annotations" — Các đường đánh dấu sự kiện trên biểu đồ
  // (ví dụ: đánh dấu lúc deploy code, lúc sự cố xảy ra)
  // Đây là cấu hình mặc định, không cần thay đổi
  "annotations": { ... },

  // "editable" — Cho phép chỉnh sửa dashboard trên giao diện web
  //   true  = ai cũng sửa được
  //   false = chỉ đọc (dùng trong production)
  "editable": true,

  // "fiscalYearStartMonth" — Tháng bắt đầu năm tài chính
  //   0 = tháng 1 (January). Một số công ty bắt đầu từ tháng 4 hoặc tháng 7
  "fiscalYearStartMonth": 0,

  // "graphTooltip" — Cách hiển thị tooltip khi rê chuột lên biểu đồ
  //   0 = chỉ hiện tooltip cho panel đang rê chuột
  //   1 = hiện tooltip đồng bộ trên TẤT CẢ panel (crosshair chung)
  "graphTooltip": 0,

  // "id" — ID nội bộ do Grafana tự sinh, không cần khai báo
  "id": null,

  // "links" — Danh sách link ngoài gắn vào dashboard (ví dụ: link tới wiki)
  "links": []
}
```

### Block 2: Panel 1 — Biểu đồ đường (Time Series)

```jsonc
{
  // --- DATASOURCE: Panel này lấy dữ liệu từ đâu? ---
  "datasource": {
    // Loại plugin — phải khớp với "type" trong datasource.yml
    "type": "grafana-postgresql-datasource",
    // UID — phải khớp với "uid" trong datasource.yml
    // ⚠️ Đây chính là lỗi gốc hôm qua: uid không khớp → No data
    "uid": "TimescaleDB"
  },

  // --- FIELD CONFIG: Cách hiển thị dữ liệu ---
  "fieldConfig": {
    "defaults": {
      "color": {
        // "palette-classic" = Grafana tự chọn màu đẹp cho mỗi đường
        // (đường 1 xanh lá, đường 2 vàng, đường 3 tím...)
        "mode": "palette-classic"
      },
      "custom": {
        // "drawStyle" — Kiểu vẽ: "line" (đường), "bars" (cột), "points" (chấm)
        "drawStyle": "line",

        // "lineInterpolation" — Kiểu nối điểm:
        //   "linear" = nối thẳng (góc cạnh)
        //   "smooth" = nối cong (mượt mà hơn, đẹp hơn)
        "lineInterpolation": "smooth",

        // "lineWidth" — Độ dày đường kẻ (pixel)
        "lineWidth": 2,

        // "fillOpacity" — Độ trong suốt vùng tô dưới đường (0-100)
        //   0  = không tô
        //   10 = tô nhẹ (hiệu ứng đẹp)
        //   100 = tô đặc
        "fillOpacity": 10,

        // "pointSize" — Kích thước chấm tại mỗi điểm dữ liệu
        "pointSize": 5,

        // "showPoints" — Có hiện chấm không?
        //   "auto"   = Grafana tự quyết định (ít điểm → hiện, nhiều → ẩn)
        //   "always" = luôn hiện
        //   "never"  = không bao giờ hiện
        "showPoints": "auto",

        // "spanNulls" — Khi gặp khoảng trống (không có dữ liệu):
        //   false = đứt đường (hiện rõ chỗ thiếu data)
        //   true  = nối liền qua khoảng trống (giấu đi)
        "spanNulls": false,

        // "axisBorderShow" — Hiện viền trục X/Y
        "axisBorderShow": false,
        // "axisCenteredZero" — Trục Y có đặt 0 ở giữa không
        "axisCenteredZero": false,
        // "axisColorMode" — Màu trục: "text" = theo màu chữ theme
        "axisColorMode": "text",
        // "axisLabel" — Nhãn tùy chỉnh cho trục Y (để trống = tự động)
        "axisLabel": "",
        // "axisPlacement" — Vị trí trục Y: "auto", "left", "right", "hidden"
        "axisPlacement": "auto",

        // "barAlignment" — Căn lề thanh bar (chỉ dùng khi drawStyle = "bars")
        "barAlignment": 0,
        "barWidthFactor": 0.6,

        // "gradientMode" — Tô gradient dưới đường:
        //   "none"   = không gradient
        //   "opacity"= gradient theo độ mờ
        //   "scheme" = gradient theo bảng màu
        "gradientMode": "none",

        // "hideFrom" — Ẩn dữ liệu khỏi legend/tooltip/biểu đồ
        "hideFrom": { "legend": false, "tooltip": false, "viz": false },

        // "insertNulls" — Tự chèn giá trị null khi khoảng cách giữa 2 điểm quá xa
        "insertNulls": false,

        // "scaleDistribution" — Kiểu chia trục Y:
        //   "linear" = đều nhau (0, 10, 20, 30...)
        //   "log"    = logarithmic (1, 10, 100, 1000...) — dùng cho data chênh lệch lớn
        "scaleDistribution": { "type": "linear" },

        // "stacking" — Xếp chồng các đường lên nhau
        //   "none"    = không xếp chồng (mỗi đường riêng biệt)
        //   "normal"  = xếp chồng cộng dồn
        //   "percent" = xếp chồng theo tỷ lệ %
        "stacking": { "group": "A", "mode": "none" },

        // "thresholdsStyle" — Hiển thị đường ngưỡng trên biểu đồ
        //   "off"  = không hiện
        //   "line" = vẽ đường ngang
        //   "area" = tô vùng
        "thresholdsStyle": { "mode": "off" }
      },

      // "mappings" — Chuyển đổi giá trị hiển thị
      // (ví dụ: 0 → "Offline", 1 → "Online")
      "mappings": [],

      // "thresholds" — Ngưỡng đổi màu
      // (ví dụ: dưới 80 = xanh, trên 80 = đỏ)
      // Ở đây không áp dụng vì giá BTC là hàng chục nghìn USD
      "thresholds": {
        "mode": "absolute",
        "steps": [
          { "color": "green", "value": null },
          { "color": "red", "value": 80 }
        ]
      }
    },
    // "overrides" — Ghi đè cấu hình cho 1 field cụ thể
    // (ví dụ: đường VWAP màu xanh, đường SMA màu cam)
    "overrides": []
  },

  // --- GRID POSITION: Vị trí và kích thước panel trên dashboard ---
  // Dashboard Grafana là lưới 24 cột
  "gridPos": {
    "h": 12,  // Chiều cao: 12 ô (chiếm ~60% màn hình)
    "w": 24,  // Chiều rộng: 24 ô (chiếm TOÀN BỘ chiều ngang)
    "x": 0,   // Vị trí ngang: cột 0 (sát trái)
    "y": 0    // Vị trí dọc: hàng 0 (trên cùng)
  },

  // "id" — ID duy nhất của panel trong dashboard này
  "id": 1,

  // --- OPTIONS: Cấu hình legend và tooltip ---
  "options": {
    "legend": {
      "calcs": [],              // Không tính toán gì thêm (avg, min, max...)
      "displayMode": "list",    // Hiển thị legend dạng danh sách
      "placement": "bottom",    // Legend nằm ở dưới biểu đồ
      "showLegend": true        // Hiện legend
    },
    "tooltip": {
      "mode": "single",   // Tooltip chỉ hiện cho 1 đường gần nhất
      "sort": "none"      // Không sắp xếp giá trị trong tooltip
    }
  },

  // --- TARGETS: Câu SQL lấy dữ liệu ---
  // ⭐ ĐÂY LÀ PHẦN QUAN TRỌNG NHẤT ⭐
  "targets": [
    {
      "datasource": {
        "type": "grafana-postgresql-datasource",
        "uid": "TimescaleDB"
      },
      // "format" — Định dạng kết quả trả về:
      //   "time_series" = Grafana hiểu đây là dữ liệu chuỗi thời gian
      //                   (bắt buộc có cột "time")
      //   "table"       = Grafana hiểu đây là bảng thông thường
      "format": "time_series",

      // "rawQuery" — Dùng câu SQL thô thay vì query builder
      //   true = viết SQL tay (linh hoạt hơn)
      //   false = dùng giao diện kéo thả
      "rawQuery": true,

      // "rawSql" — Câu SQL thực tế gửi tới TimescaleDB
      // ⚠️ Quy tắc bắt buộc cho format "time_series":
      //   1. PHẢI có cột tên "time" (dùng AS "time")
      //   2. Các cột còn lại sẽ trở thành các đường trên biểu đồ
      //   3. ORDER BY time ASC để vẽ từ trái sang phải
      "rawSql": "SELECT window_start AS \"time\", vwap AS \"VWAP\", sma AS \"SMA\" FROM crypto_indicators WHERE symbol = 'BTCUSDT' ORDER BY window_start ASC",

      // "refId" — Mã tham chiếu (A, B, C...) khi panel có nhiều query
      "refId": "A"
    }
  ],

  // "title" — Tiêu đề panel hiển thị trên giao diện
  "title": "BTC - VWAP & SMA Real-time",

  // "type" — Loại visualization:
  //   "timeseries" = biểu đồ đường/cột theo thời gian
  //   "table"      = bảng dữ liệu
  //   "gauge"      = đồng hồ đo
  //   "stat"       = số lớn (big number)
  //   "barchart"   = biểu đồ cột
  //   "piechart"   = biểu đồ tròn
  "type": "timeseries"
}
```

### Block 3: Panel 2 — Bảng dữ liệu (Table)

```jsonc
{
  // Datasource giống panel 1 — cùng lấy từ TimescaleDB
  "datasource": { "type": "grafana-postgresql-datasource", "uid": "TimescaleDB" },

  "fieldConfig": {
    "defaults": {
      "color": {
        // "thresholds" = màu ô thay đổi theo ngưỡng (xanh/vàng/đỏ)
        "mode": "thresholds"
      },
      "custom": {
        "align": "auto",      // Căn lề tự động
        "cellOptions": {
          "type": "auto"      // Kiểu hiển thị ô: auto, sparkline, gauge...
        },
        "inspect": false      // Có cho phép click để xem chi tiết ô không
      }
    }
  },

  // Panel 2 nằm dưới Panel 1
  "gridPos": {
    "h": 9,   // Chiều cao: 9 ô (nhỏ hơn panel 1)
    "w": 24,  // Chiều rộng: toàn bộ
    "x": 0,
    "y": 12   // ⚠️ y = 12 nghĩa là nằm SAU panel 1 (panel 1 cao 12 ô)
  },

  "options": {
    "cellHeight": "sm",    // Chiều cao mỗi hàng: sm (nhỏ), md (vừa), lg (lớn)
    "showHeader": true,    // Hiện tiêu đề cột
    "footer": {
      "show": false        // Ẩn footer (tổng, đếm...)
    }
  },

  "targets": [
    {
      // "format": "table" — Kết quả hiển thị dạng bảng
      // Khác với "time_series", không cần cột "time"
      "format": "table",
      "rawQuery": true,

      // Câu SQL: Lấy 50 dòng mới nhất, sắp xếp mới nhất lên trên
      // LIMIT 50 — giới hạn để tránh load quá nhiều dữ liệu
      "rawSql": "SELECT symbol, window_start, window_end, sma, vwap, trade_count FROM crypto_indicators ORDER BY window_start DESC LIMIT 50",
      "refId": "A"
    }
  ],

  "title": "Recent Indicators (Raw Table)",
  "type": "table"  // Loại panel: bảng
}
```

### Block 4: Cài đặt chung của Dashboard

```jsonc
{
  // "refresh" — Tự động làm mới dashboard mỗi 10 giây
  // Grafana sẽ gửi lại câu SQL tới DB mỗi 10s để lấy dữ liệu mới
  // Giá trị khác: "5s", "30s", "1m", "5m", "" (tắt auto-refresh)
  "refresh": "10s",

  // "schemaVersion" — Phiên bản cấu trúc JSON (Grafana tự quản lý)
  // Mỗi bản Grafana mới có thể tăng số này
  "schemaVersion": 39,

  // "tags" — Nhãn để tìm kiếm và phân loại dashboard
  "tags": ["crypto", "trading"],

  // "templating" — Biến động (variable) cho dashboard
  // Ví dụ: dropdown chọn symbol (BTCUSDT, ETHUSDT...)
  // Hiện tại chưa dùng
  "templating": { "list": [] },

  // "time" — Khoảng thời gian mặc định khi mở dashboard
  //   "from": "now-24h" = 24 giờ trước
  //   "to": "now+12h"   = 12 giờ sau (để bắt dữ liệu có timezone lệch)
  "time": { "from": "now-24h", "to": "now+12h" },

  // "timepicker" — Cấu hình bộ chọn thời gian (để mặc định)
  "timepicker": {},

  // "timezone" — Múi giờ hiển thị trên trục X
  //   "utc"     = giờ UTC (không lệch múi giờ)
  //   "browser" = theo múi giờ máy người dùng
  "timezone": "utc",

  // "title" — Tên dashboard hiển thị trên breadcrumb
  "title": "TradeStream Market Overview",

  // "uid" — Mã định danh DUY NHẤT của dashboard
  // Dùng trong URL: localhost:3000/d/tradestream-overview/...
  // ⚠️ Nếu đổi uid, Grafana sẽ tạo dashboard MỚI thay vì cập nhật cái cũ
  "uid": "tradestream-overview",

  // "version" — Số phiên bản dashboard
  // Mỗi lần sửa trên UI, Grafana tự tăng số này
  // Khi provisioning từ file, Grafana chỉ ghi đè nếu version trong file
  // CAO HƠN version đang lưu trong DB nội bộ
  "version": 1
}
```

---

## 🔗 Mối quan hệ giữa 3 file

```
datasource.yml                    dashboards.yml
┌─────────────────┐               ┌──────────────────────┐
│ uid: TimescaleDB │◄─── KHỚP ───►│ path: /var/lib/...   │
│ type: grafana-   │               │ (thư mục chứa JSON)  │
│   postgresql-... │               └──────────┬───────────┘
│ database:        │                          │
│   tradestream    │                          ▼
└────────┬─────────┘               tradestream_dashboard.json
         │                        ┌──────────────────────┐
         │                        │ datasource.uid:      │
         └────── PHẢI KHỚP ──────►│   "TimescaleDB"      │
                                  │ datasource.type:     │
                                  │   "grafana-          │
                                  │    postgresql-..."   │
                                  │ rawSql: SELECT ...   │
                                  │   FROM crypto_       │
                                  │   indicators         │
                                  └──────────────────────┘
```

**Quy tắc vàng:** `uid` và `type` trong 2 file **PHẢI KHỚP NHAU 100%**.
Sai 1 ký tự = Grafana báo lỗi đỏ "No data".
