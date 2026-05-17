import asyncio
import websockets
import json
import logging
import os
import sys
import time
from dotenv import load_dotenv
from confluent_kafka import Producer

# 1. Cấu hình logging chuyên nghiệp thay vì dùng print()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('CryptoProducer')

# Sửa lỗi hiển thị tiếng Việt trên Terminal Windows
sys.stdout.reconfigure(encoding='utf-8')

# 2. Tải cấu hình từ .env
load_dotenv()
BINANCE_WSS_URL = os.getenv('BINANCE_WSS_URL')
KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
KAFKA_TOPIC = 'crypto_trades'

# 3. Cấu hình Kafka Producer
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER_URL,
    'client.id': 'crypto-producer'
}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    """Callback function để xác nhận Kafka đã nhận message chưa"""
    if err is not None:
        logger.error(f"❌ Lỗi gửi message: {err}")
    # Không log info ở đây để tránh spam màn hình do data real-time quá nhanh

async def stream_crypto_data():
    """Hàm kết nối tới Binance và đẩy dữ liệu vào Kafka"""
    logger.info(f"Đang kết nối tới Binance: {BINANCE_WSS_URL}...")
    logger.info(f"Đẩy dữ liệu tới Kafka Broker: {KAFKA_BROKER_URL}, Topic: {KAFKA_TOPIC}")
    
    async with websockets.connect(BINANCE_WSS_URL) as ws:
        while True:
            response = await ws.recv()
            data = json.loads(response)
            
            # 4. Định nghĩa Data Schema (Cấu trúc dữ liệu JSON)
            kafka_message = {
                'symbol': data['s'],         # Mã giao dịch (VD: BTCUSDT)
                'price': float(data['p']),   # Giá khớp lệnh
                'quantity': float(data['q']),# Khối lượng khớp lệnh
                'trade_time': data['T']      # Thời gian giao dịch (Unix Timestamp ms)
            }
            
            # 5. Gửi dữ liệu vào Kafka
            producer.produce(
                topic=KAFKA_TOPIC,
                key=kafka_message['symbol'], # Dùng symbol làm key để phân luồng (Partition)
                value=json.dumps(kafka_message),
                callback=delivery_report
            )
            producer.poll(0) # Trigger events/callbacks
            
            logger.info(f"💰 Đã gửi Kafka -> {kafka_message['symbol']}: {kafka_message['price']} USD")

if __name__ == '__main__':
    try:
        asyncio.run(stream_crypto_data())
    except KeyboardInterrupt:
        logger.info("Đang xả các messages còn tồn đọng trong queue...")
        producer.flush() # Đảm bảo Kafka gửi hết message trước khi tắt app
        logger.info("Đã dừng chương trình an toàn.")
