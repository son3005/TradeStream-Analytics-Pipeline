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
SELECT create_hypertable('crypto_indicators', 'window_start', chunk_time_interval => INTERVAL '1 day');

-- 3. Tạo Index để truy vấn siêu nhanh theo đồng coin và thời gian
CREATE INDEX ix_crypto_symbol_time ON crypto_indicators (symbol, window_start DESC);

-- 4. Bảng lưu trữ giá hàng ngày (phục vụ dashboard)
CREATE TABLE daily_prices (
    symbol VARCHAR(20) NOT NULL,
    fetch_date DATE NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume BIGINT,
    currency VARCHAR(10),
    asset_type VARCHAR(20),
    PRIMARY KEY (symbol, fetch_date)
);

-- Biến daily_prices thành Hypertable phân vùng theo ngày (interval 7 ngày)
SELECT create_hypertable('daily_prices', 'fetch_date', chunk_time_interval => INTERVAL '7 days');

-- Tạo Index truy vấn nhanh theo symbol và fetch_date
CREATE INDEX ix_daily_prices_symbol_time ON daily_prices (symbol, fetch_date DESC);

-- In log ra console lúc khởi tạo
\echo '====================================='
\echo '🔥 Đã khởi tạo Hypertable thành công!'
\echo '====================================='

