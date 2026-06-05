-- Khởi tạo bảng dự báo cho Machine Learning Pipeline
\c tradestream;

CREATE TABLE IF NOT EXISTS daily_predictions (
    symbol VARCHAR(20) NOT NULL,
    prediction_date DATE NOT NULL,
    predicted_direction INTEGER NOT NULL, -- 1: Tăng, 0: Giảm/Không đổi
    probability DOUBLE PRECISION NOT NULL, -- Xác suất dự đoán (0.0 -> 1.0)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, prediction_date)
);

-- Chuyển đổi bảng daily_predictions thành Hypertable phân vùng theo ngày (interval 7 ngày)
SELECT create_hypertable('daily_predictions', 'prediction_date', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);

-- Tạo Index truy vấn nhanh theo symbol và prediction_date
CREATE INDEX IF NOT EXISTS ix_daily_predictions_symbol_time ON daily_predictions (symbol, prediction_date DESC);

\echo '====================================='
\echo '🔥 Đã khởi tạo bảng daily_predictions thành công!'
\echo '====================================='
