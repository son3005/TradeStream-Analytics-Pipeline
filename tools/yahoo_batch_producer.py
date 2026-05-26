import sys
import asyncio
import aiohttp
import json
import os
import argparse
from datetime import datetime
from confluent_kafka import Producer, KafkaError, Message
from typing import Dict, Any, List, Optional

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
def delivery_report(err: Optional[KafkaError], msg: Message) -> None:
    """Hàm phản hồi (callback) được gọi sau khi tin nhắn được gửi thành công hoặc thất bại tới Kafka broker.

    Args:
        err (Optional[KafkaError]): Đối tượng chứa thông tin lỗi nếu việc gửi thất bại, ngược lại là None.
        msg (Message): Đối tượng tin nhắn Kafka được xử lý gửi đi.

    Returns:
        None
    """
    if err is not None:
        print(f"❌ Lỗi gửi Kafka ({msg.key()}): {err}")

async def fetch_symbol(session: aiohttp.ClientSession, symbol_info: Dict[str, Any], producer: Producer) -> None:
    """Gọi API Yahoo Finance và gửi trực tiếp dữ liệu thô (raw JSON) vào Kafka broker.

    Hàm này thực hiện truy vấn dữ liệu giá của mã tài sản tương ứng, áp dụng cơ chế tự động gửi lại 
    (retry) kết hợp giãn cách số mũ (exponential backoff) khi gặp lỗi hoặc bị giới hạn tần suất (rate limited).

    Args:
        session (aiohttp.ClientSession): Phiên làm việc Client HTTP bất đồng bộ để gọi API.
        symbol_info (Dict[str, Any]): Từ điển chứa thông tin của mã tài sản (ký hiệu, tên, loại).
        producer (Producer): Thực thể Kafka Producer dùng để phát tin nhắn.

    Returns:
        None
    """
    symbol = symbol_info["symbol"]
    url = f"{YAHOO_API_URL}/{symbol}?range=1d&interval=1d"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    retries = 3
    backoff = 1.0  # Bắt đầu với 1 giây
    
    for attempt in range(retries):
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
                        break
                    else:
                        print(f"⚠️ {symbol}: Dữ liệu trống hoặc sai định dạng JSON từ Yahoo")
                        break
                elif response.status == 429: # Rate limited
                    wait_time = backoff * (2 ** attempt)
                    print(f"⚠️ {symbol}: Bị giới hạn tần suất (429). Thử lại sau {wait_time}s... (Lần {attempt + 1}/{retries})")
                    await asyncio.sleep(wait_time)
                else:
                    print(f"❌ {symbol}: Lỗi HTTP {response.status} (Lần {attempt + 1}/{retries})")
                    if attempt < retries - 1:
                        await asyncio.sleep(backoff * (2 ** attempt))
                    else:
                        break
        except Exception as e:
            print(f"❌ {symbol}: Lỗi kết nối - {str(e)} (Lần {attempt + 1}/{retries})")
            if attempt < retries - 1:
                await asyncio.sleep(backoff * (2 ** attempt))
            else:
                break

async def main(symbols_file_path: str) -> None:
    """Đọc danh sách các mã tài sản từ tệp tin cấu hình và phát song song thông tin giá của chúng vào Kafka.

    Khởi tạo Kafka Producer được tối ưu hóa khả năng chịu lỗi và hiệu năng mạng, sau đó
    tạo Connection Pool gọi song song API Yahoo Finance cho danh sách các mã cấu hình.

    Args:
        symbols_file_path (str): Đường dẫn dẫn tới tệp tin cấu hình symbols.json chứa danh sách mã tài sản.

    Returns:
        None
    """
    # Khởi tạo Kafka Producer với cấu hình tối ưu hóa khả năng chống lỗi và nén dữ liệu
    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'acks': 'all',                  # Bảo toàn dữ liệu: Đảm bảo toàn bộ replicas xác nhận đã nhận tin nhắn
        'linger.ms': 5,                 # Gom tin nhắn gửi đi trong vòng 5ms (giảm tải I/O mạng)
        'batch.size': 16384,            # Lô tin nhắn tối đa 16KB trước khi gửi
        'compression.type': 'snappy',   # Nén dữ liệu snappy hiệu năng cao để giảm tải I/O mạng
        'retries': 5,                   # Tự động gửi lại tối đa 5 lần nếu gặp lỗi kết nối tạm thời của Kafka
        'retry.backoff.ms': 1000,       # Thời gian chờ giữa các lần retry tự động của Kafka
        'client.id': 'yahoo-batch-producer'
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
