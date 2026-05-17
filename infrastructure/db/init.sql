-- Khởi tạo Database cho TradeStream
-- Script này sẽ tự động chạy khi Container TimescaleDB được khởi tạo lần đầu tiên

CREATE DATABASE tradestream;
\c tradestream;

-- 1. Tạo bảng tiêu chuẩn PostgreSQL
CREATE TABLE crypto_indicators (
    symbol VARCHAR(20) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    sma DOUBLE PRECISION,
    vwap DOUBLE PRECISION,
    trade_count INTEGER,
    -- Cột thời gian luôn phải có trong bảng time-series
    UNIQUE (symbol, window_start) 
);

-- 2. "Phép thuật" của TimescaleDB: Biến bảng thường thành Hypertable
-- Phân vùng dữ liệu dựa trên cột window_start, mỗi vùng cắt theo 1 ngày
SELECT create_hypertable('crypto_indicators', 'window_start', chunk_time_interval => INTERVAL '1 day');

-- 3. Tạo Index để truy vấn siêu nhanh theo đồng coin và thời gian
CREATE INDEX ix_crypto_symbol_time ON crypto_indicators (symbol, window_start DESC);

-- In log ra console lúc khởi tạo
\echo '====================================='
\echo '🔥 Đã khởi tạo Hypertable thành công!'
\echo '====================================='
