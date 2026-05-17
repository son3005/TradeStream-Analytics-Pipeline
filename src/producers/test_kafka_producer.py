"""
Test script: Gửi vài message giả vào Kafka để verify stream processor nhận được
"""
import sys, os
sys.stdout.reconfigure(encoding='utf-8')

import json, time
from confluent_kafka import Producer

KAFKA_BROKER = "localhost:9092"
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Gửi 10 messages BTC giả vào topic crypto_trades
messages = [
    {"symbol": "BTCUSDT", "price": 95000.0 + i * 100, "quantity": 0.5 + i * 0.1, "trade_time": int(time.time() * 1000)}
    for i in range(10)
]

print(f"Gửi {len(messages)} messages test vào Kafka...")
for msg in messages:
    producer.produce("crypto_trades", key="BTCUSDT", value=json.dumps(msg))
    time.sleep(0.5)

producer.flush()
print("✅ Đã gửi xong! Chờ Spark xử lý trong 10-15 giây...")
