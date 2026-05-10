---
name: dashboard-learning
description: "Hướng dẫn học Grafana dashboard và alerting từ zero cho dự án TradeStream. Real-time visualization, panel design, và alert rules cho trading data."
---

# Dashboard & Alerting — TradeStream

## Mục đích

Hướng dẫn xây dựng real-time dashboard với Grafana và hệ thống alerting cho dự án TradeStream.

## Khi nào sử dụng

- Khi bắt đầu Phase 7 của TradeStream
- Khi cần tạo/sửa Grafana panels
- Khi thiết lập alert rules
- Khi tối ưu dashboard performance

---

## Docker Setup

```yaml
# docker-compose.yml (profile: viz)
services:
  grafana:
    image: grafana/grafana:latest
    profiles: ["viz"]
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: tradestream
      GF_INSTALL_PLUGINS: grafana-clock-panel,grafana-simple-json-datasource
    volumes:
      - grafana-data:/var/lib/grafana
      - ./dashboards:/etc/grafana/provisioning/dashboards

volumes:
  grafana-data:
```

---

## Dashboard Panels dự kiến

### 1. Market Overview Dashboard
```
┌──────────────────────────────────────────────────┐
│  📊 TradeStream — Market Overview                │
├────────────────────┬─────────────────────────────┤
│ BTC Price (live)   │ ETH Price (live)            │
│ [Line Chart]       │ [Line Chart]                │
├────────────────────┼─────────────────────────────┤
│ Volume Comparison  │ RSI Signals                 │
│ [Bar Chart]        │ [Gauge: 0-100]              │
├────────────────────┼─────────────────────────────┤
│ Top Movers (24h)   │ ML Prediction Accuracy      │
│ [Table]            │ [Stat Panel]                │
├────────────────────┴─────────────────────────────┤
│ Anomaly Timeline                                 │
│ [Annotations on Time Series]                     │
└──────────────────────────────────────────────────┘
```

### 2. Technical Indicators Dashboard
```
┌──────────────────────────────────────────────────┐
│  📈 Technical Analysis — {Symbol}                │
├──────────────────────────────────────────────────┤
│ Price + SMA + EMA + Bollinger Bands              │
│ [Multi-line Chart]                               │
├────────────────────┬─────────────────────────────┤
│ RSI (14)           │ Volume                      │
│ [Line + Thresholds]│ [Bar Chart]                 │
├────────────────────┼─────────────────────────────┤
│ VWAP vs Price      │ Price Direction Prediction   │
│ [Dual Axis Chart]  │ [Pie: Lên/Xuống %]          │
└────────────────────┴─────────────────────────────┘
```

---

## Data Source: Trino → Grafana

```
Grafana → PostgreSQL plugin → Trino (JDBC) → Iceberg Lakehouse
```

Hoặc dùng Trino PostgreSQL wire protocol:
- Host: `trino`
- Port: `8080`
- Database: `lakehouse`
- User: `trino`

---

## Alert Rules

| Alert | Điều kiện | Severity |
|-------|-----------|----------|
| Price Spike | Giá thay đổi > 5% trong 5 phút | 🔴 Critical |
| Volume Anomaly | Volume > 3x trung bình 1 giờ | 🟡 Warning |
| RSI Overbought | RSI > 80 | 🟡 Warning |
| RSI Oversold | RSI < 20 | 🟡 Warning |
| Pipeline Down | Không có data mới > 5 phút | 🔴 Critical |
| ML Drift | Prediction accuracy < 55% | 🟡 Warning |

---

## Checklist Phase 7

```
□ Grafana chạy tại http://localhost:3000
□ Login: admin/tradestream thành công
□ Data source Trino đã kết nối
□ Market Overview dashboard hoạt động
□ Technical Indicators dashboard hoạt động
□ Alert rules đã thiết lập
□ Alerts gửi notification thành công
□ Dashboard auto-refresh hoạt động
□ Learning-log: Dashboard concepts
```
