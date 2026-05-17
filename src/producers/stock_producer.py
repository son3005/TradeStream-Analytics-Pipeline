import asyncio
import aiohttp
import json
import logging
import os
import sys
import time
from dotenv import load_dotenv
from confluent_kafka import Producer

# 1. Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('StockProducer')

# Sửa lỗi hiển thị tiếng Việt trên Terminal Windows
sys.stdout.reconfigure(encoding='utf-8')

# 2. Tải cấu hình từ .env
load_dotenv()
YAHOO_URL = os.getenv('YAHOO_FINANCE_URL', 'https://query1.finance.yahoo.com/v8/finance/chart/')
KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
KAFKA_TOPIC = 'stock_trades'
SYMBOL = "AAPL" # Mã chứng khoán Apple

# 3. Cấu hình Kafka Producer
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER_URL,
    'client.id': 'stock-producer'
}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    """Callback function để xác nhận Kafka đã nhận message chưa"""
    if err is not None:
        logger.error(f"❌ Lỗi gửi message: {err}")

async def fetch_stock_price(session):
    """Gửi yêu cầu REST API để lấy giá cổ phiếu"""
    url = f"{YAHOO_URL}{SYMBOL}"
    
    # Yahoo Finance chặn các luồng tự động, nên ta phải giả mạo là một trình duyệt thật (Chrome/Firefox)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # ========================================================
    # THỬ THÁCH CHO BẠN: LẤY DỮ LIỆU TỪ REST API (Dùng aiohttp)
    # ========================================================
    # Gợi ý 1: Dùng lệnh async with session.get(url, headers=headers) as response:
    # Gợi ý 2: Kiểm tra nếu response.status == 200 thì đọc dữ liệu: data = await response.json()
    # Gợi ý 3: Giá cổ phiếu hiện tại nằm tít sâu trong lớp JSON này:
    # price = data['chart']['result'][0]['meta']['regularMarketPrice']
    # Sau đó in ra bằng logger.info()

    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data = await response.json() 
            price = data['chart']['result'][0]['meta']['regularMarketPrice']
            
            # 4. Định nghĩa Data Schema (Cấu trúc JSON)
            kafka_message = {
                'symbol': SYMBOL,
                'price': float(price),
                'quantity': 1.0, # Yahoo API cơ bản không có khối lượng real-time, ta giả lập là 1
                'trade_time': int(time.time() * 1000) # Unix timestamp (milliseconds)
            }
            
            # 5. Gửi dữ liệu vào Kafka
            producer.produce(
                topic=KAFKA_TOPIC,
                key=kafka_message['symbol'],
                value=json.dumps(kafka_message),
                callback=delivery_report
            )
            producer.poll(0)
            
            logger.info(f"📈 Đã gửi Kafka -> {kafka_message['symbol']}: {kafka_message['price']} USD")
        else:
            logger.warning(f"⚠️ Lỗi API Yahoo: HTTP {response.status}") 

async def stream_stock_data():
    """Hàm chạy liên tục, mỗi 2 giây gửi 1 request lấy giá (Giả lập Streaming)"""
    logger.info(f"Đang chuẩn bị lấy giá cổ phiếu {SYMBOL} từ Yahoo Finance...")
    
    # Khởi tạo 1 phiên kết nối mạng (Giống như mở trình duyệt)
    async with aiohttp.ClientSession() as session:
        while True:
            await fetch_stock_price(session)
            
            # API chứng khoán gọi nhiều quá sẽ bị block, nên phải cho nó nghỉ 2 giây sau mỗi lần gọi!
            await asyncio.sleep(2)

if __name__ == '__main__':
    try:
        asyncio.run(stream_stock_data())
    except KeyboardInterrupt:
        logger.info("Đang xả nốt các messages còn tồn đọng trong queue...")
        producer.flush()
        logger.info("Đã dừng chương trình chứng khoán an toàn.")
