import sys
import asyncio
import aiohttp
import json
import os
import argparse
from datetime import datetime
from confluent_kafka import Producer

# Fix Unicode error on Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Tự động detect môi trường chạy (Docker vs Host)
IS_DOCKER = os.path.exists("/.dockerenv")
if not IS_DOCKER:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

# Nếu chạy trong Docker -> dùng kafka:29092. Nếu chạy ở Host -> dùng KAFKA_BROKER_URL (localhost:9092)
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092") if IS_DOCKER else os.environ.get("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_TOPIC = "raw_daily_prices"

YAHOO_API_URL = os.environ.get("YAHOO_FINANCE_URL", "https://query1.finance.yahoo.com/v8/finance/chart")

# Hàm callback khi Kafka gửi message thành công hoặc thất bại
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Lỗi gửi Kafka ({msg.key()}): {err}")
    # Có thể bật dòng dưới lên để debug, nhưng với 100k symbol sẽ làm spam log
    # else:
    #     print(f"✅ Đã gửi {msg.key()} vào {msg.topic()} [{msg.partition()}]")

async def fetch_symbol(session, symbol_info, producer):
    """
    Gọi API Yahoo và đẩy thẳng dữ liệu thô (RAW JSON) vào Kafka.
    """
    symbol = symbol_info["symbol"]
    url = f"{YAHOO_API_URL}/{symbol}?range=1d&interval=1d"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    try:
        # Giới hạn timeout để không bị kẹt khi API lỗi
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status == 200:
                raw_text = await response.text()
                data = json.loads(raw_text)
                
                # Trích xuất phần chart result
                if "chart" in data and "result" in data["chart"] and data["chart"]["result"]:
                    result = data["chart"]["result"][0]
                    
                    # Gói thêm thông tin metadata (để Spark dễ xử lý sau này)
                    payload = {
                        "metadata": {
                            "symbol": symbol,
                            "name": symbol_info.get("name", ""),
                            "asset_type": symbol_info.get("type", ""),
                            "fetch_timestamp": datetime.now().isoformat()
                        },
                        "raw_data": result
                    }
                    
                    # Nén thành JSON string và đẩy vào Kafka
                    # Dùng symbol làm key để các record của cùng 1 symbol luôn vào cùng 1 partition
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=symbol.encode('utf-8'),
                        value=json.dumps(payload).encode('utf-8'),
                        callback=delivery_report
                    )
                else:
                    print(f"⚠️ {symbol}: Dữ liệu trống hoặc sai định dạng JSON từ Yahoo")
            else:
                print(f"❌ {symbol}: Lỗi HTTP {response.status}")
                
    except Exception as e:
        print(f"❌ {symbol}: Không thể kết nối - {str(e)}")

async def main(symbols_file_path):
    # Khởi tạo Kafka Producer
    # Chỉnh 'linger.ms' và 'batch.size' để tối ưu hóa khi đẩy hàng chục ngàn message
    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'linger.ms': 50, # Đợi 50ms để gom nhiều tin nhắn gửi đi cùng lúc
        'batch.num.messages': 1000
    }
    producer = Producer(producer_conf)
    
    print(f"🚀 Kết nối Kafka tại {KAFKA_BROKER}...")
    
    # Đọc danh sách Symbols
    try:
        with open(symbols_file_path, "r") as f:
            symbols = json.load(f).get("symbols", [])
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file: {symbols_file_path}")
        return
        
    print(f"📋 Bắt đầu nạp {len(symbols)} symbols vào Kafka...")
    start_time = datetime.now()
    
    # Tạo Connection Pool (Session) để bắn API song song
    # connector=aiohttp.TCPConnector(limit=100) -> Chỉ cho phép gọi song song tối đa 100 API cùng lúc để tránh bị Yahoo khóa IP
    connector = aiohttp.TCPConnector(limit=100)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Tạo danh sách các tác vụ (tasks)
        tasks = [fetch_symbol(session, sym, producer) for sym in symbols]
        
        # Chạy đồng thời tất cả các tasks
        await asyncio.gather(*tasks)
    
    # Đợi Kafka gửi nốt những message còn kẹt trong buffer
    print("⏳ Đang dọn dẹp hàng đợi Kafka (flush)...")
    producer.flush()
    
    duration = (datetime.now() - start_time).total_seconds()
    print(f"🎉 Hoàn thành nạp {len(symbols)} symbols vào Kafka trong {duration:.2f} giây!")


if __name__ == "__main__":
    # Nhận đường dẫn file JSON từ tham số dòng lệnh
    parser = argparse.ArgumentParser(description="Yahoo Batch Producer to Kafka")
    parser.add_argument("--symbols-file", required=True, help="Path to symbols.json file")
    args = parser.parse_args()
    
    # Chạy vòng lặp bất đồng bộ
    asyncio.run(main(args.symbols_file))
