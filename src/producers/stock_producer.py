import asyncio
import aiohttp
import json
import logging
import os
import sys
from dotenv import load_dotenv

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
SYMBOL = "AAPL" # Mã chứng khoán Apple

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
            logger.info(data['chart']['result'][0]['meta']['regularMarketPrice']) 

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
        logger.info("Đã dừng chương trình chứng khoán an toàn.")
