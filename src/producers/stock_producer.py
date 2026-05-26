import asyncio
import aiohttp
import json
import logging
import os
import sys
import time
from dotenv import load_dotenv
from confluent_kafka import Producer, KafkaError, Message
from typing import List, Dict, Optional, Any

# 1. Cấu hình logging chuyên nghiệp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('StockProducer')

# Sửa lỗi hiển thị tiếng Việt trên Terminal Windows
sys.stdout.reconfigure(encoding='utf-8')

# 2. Tải cấu hình từ .env
load_dotenv()
KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
KAFKA_TOPIC = 'stock_trades'
YAHOO_FINANCE_URL = os.getenv('YAHOO_FINANCE_URL', 'https://query1.finance.yahoo.com/v8/finance/chart')

# 3. Cấu hình Kafka Producer
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER_URL,
    'client.id': 'stock-producer',
    'acks': 'all',  # Đảm bảo bảo toàn dữ liệu
    'linger.ms': 5,
    'batch.size': 16384,
    'compression.type': 'snappy'
}
producer = Producer(producer_conf)

def delivery_report(err: Optional[KafkaError], msg: Message) -> None:
    """Hàm phản hồi (callback) được gọi khi tin nhắn được gửi thành công hoặc thất bại tới Kafka.

    Args:
        err (Optional[KafkaError]): Đối tượng lỗi nếu gửi thất bại, ngược lại là None.
        msg (Message): Đối tượng tin nhắn Kafka đã được gửi.

    Returns:
        None
    """
    if err is not None:
        logger.error(f"❌ Lỗi gửi message tới Kafka: {err}")

def load_stock_symbols() -> List[str]:
    """Tải danh sách các mã cổ phiếu đang hoạt động từ cấu hình dự án.

    Đọc tệp tin config/symbols.json, lọc các tài sản thuộc loại cổ phiếu (stock) và trả về danh sách ký hiệu.

    Returns:
        List[str]: Danh sách các chuỗi ký hiệu cổ phiếu (ví dụ: ['AAPL', 'MSFT']).
    """
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config', 'symbols.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        stock_symbols = []
        for item in config.get('symbols', []):
            if item.get('type') == 'stock':
                stock_symbols.append(item['symbol'])
        return stock_symbols
    except Exception as e:
        logger.error(f"❌ Lỗi đọc file symbols.json: {e}")
        return ['AAPL', 'MSFT']

async def fetch_stock_price(session: aiohttp.ClientSession, symbol: str) -> Optional[Dict[str, Any]]:
    """Truy vấn Yahoo Finance API để lấy thông tin giá mới nhất cho một mã cổ phiếu.

    Args:
        session (aiohttp.ClientSession): Phiên làm việc HTTP client bất đồng bộ.
        symbol (str): Ký hiệu (ticker) của mã cổ phiếu.

    Returns:
        Optional[Dict[str, Any]]: Bản ghi giao dịch đã chuẩn hóa nếu thành công, ngược lại là None.
    """
    url = f"{YAHOO_FINANCE_URL}/{symbol}"
    # Yahoo chặn request nếu không giả lập User-Agent
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        async with session.get(url, headers=headers, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                result = data.get('chart', {}).get('result', [])
                if result:
                    meta = result[0].get('meta', {})
                    price = meta.get('regularMarketPrice')
                    # Nếu thị trường đóng cửa, có thể lấy close trước đó hoặc giá hiện tại
                    if price is not None:
                        # Epoch timestamp của Yahoo thường là giây, ta chuyển sang ms
                        trade_time = meta.get('regularMarketTime', int(time.time())) * 1000
                        return {
                            'symbol': symbol,
                            'price': float(price),
                            'quantity': 1.0,          # Mặc định khối lượng giao dịch cho stock
                            'trade_time': int(trade_time)
                        }
            else:
                logger.warning(f"⚠️ API trả về status code {response.status} cho symbol {symbol}")
    except Exception as e:
        logger.error(f"❌ Lỗi fetch dữ liệu cho {symbol}: {e}")
    return None

async def poll_stocks_loop() -> None:
    """Vòng lặp vô hạn để lấy giá của toàn bộ các cổ phiếu đang hoạt động theo chu kỳ xác định.

    Returns:
        None
    """
    stock_symbols = load_stock_symbols()
    logger.info(f"Khởi động StockProducer cho danh sách: {stock_symbols}")
    logger.info(f"Đẩy dữ liệu tới Kafka Broker: {KAFKA_BROKER_URL}, Topic: {KAFKA_TOPIC}")
    
    async with aiohttp.ClientSession() as session:
        while True:
            start_time = time.time()
            
            # Chạy concurrent fetch giá cho tất cả các symbol
            tasks = [fetch_stock_price(session, sym) for sym in stock_symbols]
            results = await asyncio.gather(*tasks)
            
            for res in results:
                if res:
                    # Gửi Kafka
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=res['symbol'],
                        value=json.dumps(res),
                        callback=delivery_report
                    )
                    producer.poll(0)
                    logger.info(f"📈 [Stock] Kafka <- {res['symbol']}: {res['price']} USD")
            
            # Điều chỉnh thời gian chờ để vòng lặp đúng chu kỳ 10 giây
            elapsed = time.time() - start_time
            sleep_time = max(10.0 - elapsed, 0.1)
            await asyncio.sleep(sleep_time)

if __name__ == '__main__':
    try:
        asyncio.run(poll_stocks_loop())
    except KeyboardInterrupt:
        logger.info("Đang flush các messages còn tồn đọng trong Kafka queue...")
        producer.flush()
        logger.info("Đã dừng StockProducer thành công.")
