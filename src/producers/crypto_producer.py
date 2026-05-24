import asyncio
import websockets
import json
import logging
import os
import sys
import time
from dotenv import load_dotenv
from confluent_kafka import Producer

# 1. Cấu hình logging chuyên nghiệp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('CryptoProducer')

# Sửa lỗi hiển thị tiếng Việt trên Terminal Windows
sys.stdout.reconfigure(encoding='utf-8')

# 2. Tải cấu hình từ .env
load_dotenv()
KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
KAFKA_TOPIC = 'crypto_trades'
BINANCE_WSS_URL = os.getenv('BINANCE_WSS_URL', 'wss://stream.binance.com:9443/stream')


# 3. Cấu hình Kafka Producer
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER_URL,
    'client.id': 'crypto-producer',
    'acks': 'all',  # Đảm bảo bảo toàn dữ liệu
    'linger.ms': 5,
    'batch.size': 16384,
    'compression.type': 'snappy'
}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    """Callback function để xác nhận Kafka đã nhận message chưa"""
    if err is not None:
        logger.error(f"❌ Lỗi gửi message tới Kafka: {err}")

def load_crypto_symbols():
    """Đọc config/symbols.json để lấy danh sách coin và thiết lập mapping"""
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config', 'symbols.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        crypto_symbols = []
        symbol_map = {}  # Map BTCUSDT -> BTC-USD
        
        for item in config.get('symbols', []):
            if item.get('type') == 'crypto':
                std_symbol = item['symbol']  # VD: BTC-USD
                # Binance sử dụng USDT thay cho USD và viết liền không dấu gạch ngang
                binance_symbol = std_symbol.replace('-USD', 'USDT')
                crypto_symbols.append(binance_symbol)
                symbol_map[binance_symbol] = std_symbol
                
        return crypto_symbols, symbol_map
    except Exception as e:
        logger.error(f"❌ Lỗi đọc file symbols.json: {e}")
        # Default fallback
        return ['BTCUSDT', 'ETHUSDT'], {'BTCUSDT': 'BTC-USD', 'ETHUSDT': 'ETH-USD'}

async def stream_crypto_data():
    """Hàm kết nối tới Binance WebSocket Multi-stream và đẩy dữ liệu vào Kafka"""
    binance_symbols, symbol_map = load_crypto_symbols()
    
    # Xây dựng Combined Stream URL
    # Format: wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
    streams = "/".join([f"{sym.lower()}@trade" for sym in binance_symbols])
    wss_url = f"{BINANCE_WSS_URL}?streams={streams}"

    
    logger.info(f"Đang kết nối tới Binance Multi-Stream: {wss_url}...")
    logger.info(f"Đẩy dữ liệu tới Kafka Broker: {KAFKA_BROKER_URL}, Topic: {KAFKA_TOPIC}")
    
    async with websockets.connect(wss_url) as ws:
        while True:
            response = await ws.recv()
            data = json.loads(response)
            
            # Với combined stream, data nhận về có cấu trúc {"stream": "...", "data": {...}}
            trade_data = data.get('data', data)
            raw_symbol = trade_data.get('s')  # VD: BTCUSDT
            
            if not raw_symbol or raw_symbol not in symbol_map:
                continue
                
            std_symbol = symbol_map[raw_symbol]
            
            # Định nghĩa Schema dữ liệu thô (Flat trade schema)
            kafka_message = {
                'symbol': std_symbol,                     # Chuẩn hóa về BTC-USD
                'price': float(trade_data['p']),           # Giá khớp lệnh
                'quantity': float(trade_data['q']),        # Khối lượng
                'trade_time': int(trade_data['T'])         # Timestamp (ms)
            }
            
            # Gửi dữ liệu vào Kafka
            producer.produce(
                topic=KAFKA_TOPIC,
                key=std_symbol,
                value=json.dumps(kafka_message),
                callback=delivery_report
            )
            producer.poll(0)
            
            logger.info(f"💰 [Crypto] Kafka <- {std_symbol}: {kafka_message['price']} USD (Vol: {kafka_message['quantity']})")

async def run_resilient_producer():
    """Vòng lặp duy trì kết nối WebSocket Binance"""
    while True:
        try:
            await stream_crypto_data()
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"⚠️ Kết nối Binance bị đóng: {e}. Đang kết nối lại sau 5 giây...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"❌ Lỗi không xác định: {e}. Đang kết nối lại sau 5 giây...")
            await asyncio.sleep(5)

if __name__ == '__main__':
    try:
        asyncio.run(run_resilient_producer())
    except KeyboardInterrupt:
        logger.info("Đang flush các messages còn tồn đọng trong Kafka queue...")
        producer.flush()
        logger.info("Đã dừng CryptoProducer thành công.")
