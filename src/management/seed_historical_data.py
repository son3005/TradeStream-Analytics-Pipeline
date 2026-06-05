import os
import sys
import json
import urllib.request
from datetime import datetime

# Đảm bảo mã hóa UTF-8 trên Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Cấu hình
SYMBOLS = [
    {"symbol": "BTC-USD", "name": "Bitcoin", "type": "crypto"},
    {"symbol": "ETH-USD", "name": "Ethereum", "type": "crypto"},
    {"symbol": "AAPL", "name": "Apple", "type": "stock"},
    {"symbol": "MSFT", "name": "Microsoft", "type": "stock"}
]

def fetch_historical_yahoo(symbol, range_val="90d"):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={range_val}&interval=1d"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    req = urllib.request.Request(url, headers=headers)
    
    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode('utf-8'))
            result = data["chart"]["result"][0]
            timestamps = result["timestamp"]
            indicators = result["indicators"]["quote"][0]
            
            opens = indicators["open"]
            highs = indicators["high"]
            lows = indicators["low"]
            closes = indicators["close"]
            volumes = indicators["volume"]
            
            records = []
            for i in range(len(timestamps)):
                # Lọc bỏ giá trị None nếu có
                if None in (opens[i], highs[i], lows[i], closes[i], volumes[i]):
                    continue
                
                dt = datetime.fromtimestamp(timestamps[i])
                date_key = int(dt.strftime("%Y%m%d"))
                
                open_val = float(opens[i])
                high_val = float(highs[i])
                low_val = float(lows[i])
                close_val = float(closes[i])
                volume_val = int(volumes[i])
                
                records.append({
                    "symbol": symbol,
                    "date_key": date_key,
                    "open_price": open_val,
                    "high_price": high_val,
                    "low_price": low_val,
                    "close_price": close_val,
                    "volume": volume_val,
                })
            
            # Tính toán daily_return cho các records đã sort
            records = sorted(records, key=lambda x: x["date_key"])
            for idx in range(len(records)):
                if idx == 0:
                    records[idx]["daily_return"] = 0.0
                else:
                    prev_close = records[idx-1]["close_price"]
                    records[idx]["daily_return"] = ((records[idx]["close_price"] - prev_close) / prev_close) * 100.0
                
                low_val = records[idx]["low_price"]
                high_val = records[idx]["high_price"]
                records[idx]["price_range"] = ((high_val - low_val) / low_val) * 100.0 if low_val > 0 else 0.0
                
            return records
    except Exception as e:
        print(f"[X] Lỗi khi tải dữ liệu cho {symbol}: {e}")
        return []

def main():
    print("[*] Đang khởi động tiến trình nạp dữ liệu lịch sử...")
    
    # Gom tất cả records của các symbols
    all_records = []
    for item in SYMBOLS:
        symbol = item["symbol"]
        print(f"[*] Đang tải dữ liệu 90 ngày của {symbol} từ Yahoo Finance...")
        records = fetch_historical_yahoo(symbol, "90d")
        print(f"[SUCCESS] Tải thành công {len(records)} ngày.")
        all_records.extend(records)
        
    if not all_records:
        print("[X] Không lấy được dữ liệu nào! Thoát.")
        return
        
    # Khởi tạo Spark Session
    # Vì script này chạy trong spark-master nên ta import spark_helper của local
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from utils.spark_helper import get_spark_session
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
    
    spark = get_spark_session("SeedHistoricalData")
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Schema của fact_daily_prices
        schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("date_key", IntegerType(), True),
            StructField("open_price", DoubleType(), True),
            StructField("high_price", DoubleType(), True),
            StructField("low_price", DoubleType(), True),
            StructField("close_price", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("daily_return", DoubleType(), True),
            StructField("price_range", DoubleType(), True)
        ])
        
        # Tạo DataFrame
        df = spark.createDataFrame(all_records, schema=schema)
        df.createOrReplaceTempView("temp_seed_prices")
        
        # Merge vào bảng Iceberg
        print("[*] Đang thực hiện MERGE INTO dữ liệu lịch sử vào bảng fact_daily_prices...")
        spark.sql("""
            MERGE INTO lakehouse.trading.fact_daily_prices target
            USING temp_seed_prices source
            ON target.symbol = source.symbol AND target.date_key = source.date_key
            WHEN MATCHED THEN UPDATE SET 
                open_price = source.open_price,
                high_price = source.high_price,
                low_price = source.low_price,
                close_price = source.close_price,
                volume = source.volume,
                daily_return = source.daily_return,
                price_range = source.price_range
            WHEN NOT MATCHED THEN INSERT (
                symbol, date_key, open_price, high_price, low_price, close_price, volume, daily_return, price_range
            ) VALUES (
                source.symbol, source.date_key, source.open_price, source.high_price, source.low_price, source.close_price, source.volume, source.daily_return, source.price_range
            )
        """)
        
        count = spark.sql("SELECT COUNT(*) FROM lakehouse.trading.fact_daily_prices").collect()[0][0]
        print(f"[SUCCESS] Đã nạp thành công! Số lượng bản ghi hiện tại trong fact_daily_prices: {count}")
        
    except Exception as e:
        print(f"[X] Gặp lỗi khi nạp dữ liệu vào Iceberg: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
