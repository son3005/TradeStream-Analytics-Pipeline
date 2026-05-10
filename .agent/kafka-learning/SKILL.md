---
name: kafka-learning
description: "Hướng dẫn học Apache Kafka từ zero cho dự án TradeStream. Bao gồm concepts, patterns, và hands-on exercises cho streaming data ingestion."
---

# Kafka Learning Guide — TradeStream

## Mục đích

Hướng dẫn học và triển khai Apache Kafka trong dự án TradeStream Analytics Pipeline, từ khái niệm cơ bản đến production patterns.

## Khi nào sử dụng

- Khi bắt đầu Phase 1 của TradeStream
- Khi gặp lỗi liên quan đến Kafka
- Khi cần tối ưu Kafka performance
- Khi thiết kế schema cho messages

---

## Concepts cốt lõi (Học trước khi code)

### Kafka là gì?
Kafka là **distributed message broker** — hệ thống trung gian nhận và phát dữ liệu giữa các ứng dụng.

```
Producer → [Kafka Topic] → Consumer
(Binance)   (crypto.prices)   (Spark)
```

### Thuật ngữ quan trọng

| Thuật ngữ | Giải thích dễ hiểu | Ví dụ trong TradeStream |
|-----------|--------------------|-----------------------|
| **Topic** | Kênh dữ liệu, như channel YouTube | `crypto.prices`, `stock.prices` |
| **Partition** | Chia topic thành phần nhỏ để xử lý song song | Topic `crypto.prices` có 3 partitions |
| **Producer** | Ứng dụng gửi dữ liệu vào topic | `crypto_producer.py` |
| **Consumer** | Ứng dụng đọc dữ liệu từ topic | Spark Structured Streaming |
| **Consumer Group** | Nhóm consumers chia nhau đọc partitions | `tradestream-processors` |
| **Offset** | Vị trí đọc trong partition (bookmark) | Consumer đã đọc đến message #1523 |
| **Broker** | Server Kafka chạy trên | 1 broker cho dev local |
| **Zookeeper** | Quản lý metadata Kafka | Sẽ bỏ ở Kafka 4.0+ (KRaft) |

### Message Format

```json
{
  "key": "BTCUSDT",
  "value": {
    "symbol": "BTCUSDT",
    "price": 67234.50,
    "volume": 1.234,
    "timestamp": "2026-05-08T00:00:00Z",
    "exchange": "binance",
    "asset_type": "crypto"
  },
  "headers": {
    "source": "binance-websocket",
    "version": "1.0"
  }
}
```

---

## Docker Setup cho Kafka

```yaml
# docker-compose.yml (profile: core)
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    profiles: ["core"]
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    healthcheck:
      test: echo ruok | nc localhost 2181
      interval: 10s
      timeout: 5s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    profiles: ["core"]
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    healthcheck:
      test: kafka-broker-api-versions --bootstrap-server localhost:9092
      interval: 10s
      timeout: 10s
      retries: 5

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    profiles: ["core"]
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: tradestream
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
```

---

## Python Libraries

```
# requirements.txt
confluent-kafka==2.3.0      # Kafka client
kafka-python==2.0.2          # Alternative client (simpler)
```

---

## Patterns cho TradeStream

### Pattern 1: Simple Producer (Hello Kafka)

```python
"""Quick Demo: Gửi message đầu tiên vào Kafka"""
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Gửi message
producer.send('test.hello', value={'message': 'Hello Kafka!', 'from': 'TradeStream'})
producer.flush()
print("✅ Đã gửi message đầu tiên!")
```

### Pattern 2: Crypto Producer (Binance WebSocket)

```python
"""Binance WebSocket → Kafka Producer"""
import asyncio
import websockets
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

async def stream_crypto(symbol: str = "btcusdt"):
    uri = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
    async with websockets.connect(uri) as ws:
        async for message in ws:
            trade = json.loads(message)
            kafka_message = {
                "symbol": trade["s"],
                "price": float(trade["p"]),
                "volume": float(trade["q"]),
                "timestamp": trade["T"],
                "exchange": "binance",
                "asset_type": "crypto"
            }
            producer.send(
                'crypto.prices',
                key=trade["s"],
                value=kafka_message
            )

asyncio.run(stream_crypto())
```

### Pattern 3: Simple Consumer

```python
"""Đọc messages từ Kafka topic"""
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'crypto.prices',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='tradestream-debug'
)

for message in consumer:
    print(f"📨 {message.value['symbol']}: ${message.value['price']}")
```

---

## Troubleshooting phổ biến

| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| `NoBrokersAvailable` | Kafka chưa start xong | Chờ 30s sau `docker compose up`, check healthcheck |
| `TopicNotFound` | Topic chưa được tạo | Kafka auto-create topics, hoặc tạo manual qua Kafka UI |
| Messages không hiện | Consumer group đã đọc hết | Set `auto_offset_reset='earliest'` hoặc đổi `group_id` |
| Connection refused port 9092 | Docker networking | Dùng `localhost:9092` từ host, `kafka:29092` từ container |

---

## Checklist Phase 1

```
□ Docker Compose chạy được Kafka + Zookeeper + UI
□ Kafka UI accessible tại http://localhost:8080
□ Hello Kafka producer/consumer hoạt động
□ Crypto producer stream data từ Binance
□ Stock producer poll data từ Yahoo Finance
□ Messages hiển thị trên Kafka UI
□ Simple consumer in được messages
□ Learning-log đã ghi: Kafka concepts
```
