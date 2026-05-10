---
name: ml-pipeline-learning
description: "Hướng dẫn học ML pipeline từ zero cho dự án TradeStream. Feature engineering, model training, MLflow tracking, và prediction serving cho trading data."
---

# ML Pipeline — TradeStream

## Mục đích

Hướng dẫn xây dựng ML pipeline cho dự đoán giá, phát hiện bất thường, và dự báo biến động trong dự án TradeStream.

## Khi nào sử dụng

- Khi bắt đầu Phase 6 của TradeStream
- Khi thiết kế features cho ML models
- Khi train/evaluate models
- Khi tích hợp predictions vào pipeline

---

## Concepts cốt lõi

### ML Pipeline là gì?
Quy trình từ dữ liệu thô → dự đoán có giá trị:

```
Raw Data → Feature Engineering → Model Training → Evaluation → Serving
  (Iceberg)    (tạo features)     (train model)    (đánh giá)   (dự đoán)
```

### Models dự kiến

| Model | Mục đích | Loại | Thư viện |
|-------|---------|------|----------|
| **Price Direction** | Giá lên hay xuống trong 1h? | Classification | XGBoost |
| **Anomaly Detection** | Volume/price bất thường? | Unsupervised | Isolation Forest |
| **Volatility Forecast** | Độ biến động dự kiến? | Regression | LightGBM |

---

## Quick Demo: Predict giá BTC từ CSV

```python
"""Quick Demo: Simple price direction prediction"""
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

# 1. Load data
df = pd.read_csv("data/btc_hourly.csv")

# 2. Tạo features đơn giản
df['return_1h'] = df['price'].pct_change()
df['return_4h'] = df['price'].pct_change(4)
df['sma_20'] = df['price'].rolling(20).mean()
df['price_vs_sma'] = df['price'] / df['sma_20']
df['volume_change'] = df['volume'].pct_change()

# 3. Tạo label: giá sẽ lên (1) hay xuống (0) sau 1h?
df['target'] = (df['price'].shift(-1) > df['price']).astype(int)
df = df.dropna()

# 4. Train/Test split
features = ['return_1h', 'return_4h', 'price_vs_sma', 'volume_change']
X = df[features]
y = df['target']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

# 5. Train
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# 6. Evaluate
y_pred = model.predict(X_test)
print(f"✅ Accuracy: {accuracy_score(y_test, y_pred):.2%}")
print(classification_report(y_test, y_pred, target_names=['Xuống', 'Lên']))
```

---

## Feature Engineering từ Lakehouse

```python
"""Features cho ML models — đọc từ Iceberg via Trino"""

FEATURE_QUERY = """
SELECT 
    symbol,
    event_time,
    price,
    volume,
    sma_20,
    ema_12,
    rsi_14,
    vwap,
    
    -- Price features
    price / sma_20 AS price_sma_ratio,
    price / vwap AS price_vwap_ratio,
    (price - LAG(price, 1) OVER w) / LAG(price, 1) OVER w AS return_1,
    (price - LAG(price, 4) OVER w) / LAG(price, 4) OVER w AS return_4,
    (price - LAG(price, 24) OVER w) / LAG(price, 24) OVER w AS return_24,
    
    -- Volume features
    volume / AVG(volume) OVER (PARTITION BY symbol ORDER BY event_time ROWS 20 PRECEDING) AS volume_ratio,
    
    -- Volatility features
    STDDEV(price) OVER (PARTITION BY symbol ORDER BY event_time ROWS 20 PRECEDING) AS volatility_20,
    
    -- RSI features
    CASE WHEN rsi_14 > 70 THEN 1 WHEN rsi_14 < 30 THEN -1 ELSE 0 END AS rsi_signal,
    
    -- Target: giá lên (1) hay xuống (0) sau 1 giờ?
    CASE WHEN LEAD(price, 1) OVER w > price THEN 1 ELSE 0 END AS target_1h
    
FROM lakehouse.trading.fact_trades
WHERE symbol = 'BTCUSDT'
WINDOW w AS (PARTITION BY symbol ORDER BY event_time)
ORDER BY event_time
"""
```

---

## MLflow Tracking

```yaml
# docker-compose.yml (profile: ml)
services:
  mlflow:
    image: ghcr.io/mlflow/mlflow:latest
    profiles: ["ml"]
    ports:
      - "5000:5000"
    command: >
      mlflow server
        --host 0.0.0.0
        --port 5000
        --backend-store-uri sqlite:///mlflow.db
        --default-artifact-root /mlflow/artifacts
    volumes:
      - mlflow-data:/mlflow

volumes:
  mlflow-data:
```

### Training với MLflow

```python
"""Train model với MLflow tracking"""
import mlflow
import mlflow.sklearn
from xgboost import XGBClassifier

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("tradestream-price-direction")

with mlflow.start_run(run_name="xgboost-v1"):
    # Log parameters
    params = {"n_estimators": 200, "max_depth": 6, "learning_rate": 0.1}
    mlflow.log_params(params)
    
    # Train
    model = XGBClassifier(**params)
    model.fit(X_train, y_train)
    
    # Evaluate
    accuracy = accuracy_score(y_test, model.predict(X_test))
    mlflow.log_metric("accuracy", accuracy)
    
    # Save model
    mlflow.sklearn.log_model(model, "price_direction_model")
    
    print(f"✅ Model saved! Accuracy: {accuracy:.2%}")
    print(f"📊 View tại: http://localhost:5000")
```

---

## Anomaly Detection

```python
"""Phát hiện volume bất thường bằng Isolation Forest"""
from sklearn.ensemble import IsolationForest

def detect_anomalies(df):
    features = ['volume_ratio', 'return_1', 'volatility_20']
    
    model = IsolationForest(
        contamination=0.05,  # 5% data là anomaly
        random_state=42
    )
    
    df['is_anomaly'] = model.fit_predict(df[features])
    # -1 = anomaly, 1 = normal
    
    anomalies = df[df['is_anomaly'] == -1]
    print(f"🚨 Phát hiện {len(anomalies)} anomalies trong {len(df)} records")
    return anomalies
```

---

## Checklist Phase 6

```
□ MLflow chạy tại http://localhost:5000
□ Quick Demo: predict giá BTC từ CSV thành công
□ Feature engineering query chạy đúng
□ XGBoost price direction model trained
□ Isolation Forest anomaly detection hoạt động
□ Models tracked trên MLflow
□ Prediction pipeline tích hợp vào Airflow
□ Backtesting kết quả hợp lý
□ Learning-log: ML concepts
```
