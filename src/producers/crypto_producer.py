import asyncio
import websockets
import json
import logging
import os
import sys
from dotenv import load_dotenv

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

async def stream_crypto_data():
    """Hàm kết nối tới Binance và lấy dữ liệu"""
    logger.info(f"Đang kết nối tới {BINANCE_WSS_URL}...")
    
    async with websockets.connect(BINANCE_WSS_URL) as ws:
        while True:
            response = await ws.recv()
            data = json.loads(response)
            price = data['p']
            logger.info(f"💰 Giá Bitcoin (BTC): {price} USD")

if __name__ == '__main__':
    try:
        asyncio.run(stream_crypto_data())
    except KeyboardInterrupt:
        logger.info("Đã dừng chương trình an toàn.")
