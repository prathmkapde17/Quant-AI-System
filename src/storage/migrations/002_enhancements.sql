-- =============================================================================
-- Quant Trading System — Enhancement Migration
-- Adds: multi-horizon aggregates, latency tracking, data quality views
-- =============================================================================


-- =============================================================================
-- 1. MULTI-HORIZON CONTINUOUS AGGREGATES
-- Deterministic resampling: 1m → 5m → 15m → 1h → 4h
-- =============================================================================

-- 5-minute bars from 1m data
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', timestamp) AS timestamp,
    symbol,
    exchange,
    '5m'::TEXT AS timeframe,
    first(open, timestamp)  AS open,
    max(high)               AS high,
    min(low)                AS low,
    last(close, timestamp)  AS close,
    sum(volume)             AS volume,
    sum(turnover)           AS turnover,
    sum(num_trades)         AS num_trades
FROM ohlcv
WHERE timeframe = '1m'
GROUP BY time_bucket('5 minutes', timestamp), symbol, exchange
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_5m',
    start_offset    => INTERVAL '2 hours',
    end_offset      => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists   => TRUE
);

-- 15-minute bars from 1m data
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_15m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', timestamp) AS timestamp,
    symbol,
    exchange,
    '15m'::TEXT AS timeframe,
    first(open, timestamp)  AS open,
    max(high)               AS high,
    min(low)                AS low,
    last(close, timestamp)  AS close,
    sum(volume)             AS volume,
    sum(turnover)           AS turnover,
    sum(num_trades)         AS num_trades
FROM ohlcv
WHERE timeframe = '1m'
GROUP BY time_bucket('15 minutes', timestamp), symbol, exchange
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_15m',
    start_offset    => INTERVAL '6 hours',
    end_offset      => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists   => TRUE
);

-- 1-hour bars from 1m data
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp) AS timestamp,
    symbol,
    exchange,
    '1h'::TEXT AS timeframe,
    first(open, timestamp)  AS open,
    max(high)               AS high,
    min(low)                AS low,
    last(close, timestamp)  AS close,
    sum(volume)             AS volume,
    sum(turnover)           AS turnover,
    sum(num_trades)         AS num_trades
FROM ohlcv
WHERE timeframe = '1m'
GROUP BY time_bucket('1 hour', timestamp), symbol, exchange
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_1h',
    start_offset    => INTERVAL '1 day',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists   => TRUE
);

-- 4-hour bars from 1m data
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_4h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('4 hours', timestamp) AS timestamp,
    symbol,
    exchange,
    '4h'::TEXT AS timeframe,
    first(open, timestamp)  AS open,
    max(high)               AS high,
    min(low)                AS low,
    last(close, timestamp)  AS close,
    sum(volume)             AS volume,
    sum(turnover)           AS turnover,
    sum(num_trades)         AS num_trades
FROM ohlcv
WHERE timeframe = '1m'
GROUP BY time_bucket('4 hours', timestamp), symbol, exchange
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlcv_4h',
    start_offset    => INTERVAL '3 days',
    end_offset      => INTERVAL '4 hours',
    schedule_interval => INTERVAL '4 hours',
    if_not_exists   => TRUE
);


-- =============================================================================
-- 2. LATENCY TRACKING TABLE
-- Records processing timestamps at each pipeline stage
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_latency (
    id                  BIGSERIAL,
    timestamp           TIMESTAMPTZ   NOT NULL,   -- candle/tick timestamp (exchange time)
    symbol              TEXT          NOT NULL,
    exchange            TEXT          NOT NULL,
    data_type           TEXT          NOT NULL DEFAULT 'candle',  -- 'candle' | 'tick'

    -- Timestamps at each stage (all UTC)
    exchange_ts         TIMESTAMPTZ,              -- when the exchange produced this data
    received_ts         TIMESTAMPTZ   NOT NULL,   -- when we received it from the API/WebSocket
    normalized_ts       TIMESTAMPTZ,              -- after normalization
    stored_ts           TIMESTAMPTZ,              -- after DB write
    published_ts        TIMESTAMPTZ,              -- after Redis publish

    -- Computed latencies (milliseconds)
    ingestion_latency_ms   DOUBLE PRECISION,      -- received - exchange
    processing_latency_ms  DOUBLE PRECISION,      -- stored - received
    e2e_latency_ms         DOUBLE PRECISION,      -- stored - exchange

    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Convert to hypertable (1-day chunks, auto-drop after 7 days)
SELECT create_hypertable(
    'pipeline_latency', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Index for querying latencies by symbol
CREATE INDEX IF NOT EXISTS idx_latency_symbol
    ON pipeline_latency (symbol, exchange, timestamp DESC);

-- Auto-drop old latency data (keep 7 days — it's metrics, not business data)
SELECT add_retention_policy(
    'pipeline_latency',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Compression for latency data
ALTER TABLE pipeline_latency SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, exchange',
    timescaledb.compress_orderby = 'timestamp'
);

SELECT add_compression_policy(
    'pipeline_latency',
    compress_after => INTERVAL '1 day',
    if_not_exists => TRUE
);


-- =============================================================================
-- 3. DATA QUALITY VIEWS
-- Pre-computed analytics for monitoring data completeness and anomalies
-- =============================================================================

-- Completeness: expected vs actual candles per symbol per day
CREATE OR REPLACE VIEW data_completeness_daily AS
SELECT
    date_trunc('day', timestamp) AS day,
    symbol,
    exchange,
    timeframe,
    COUNT(*) AS actual_candles,
    CASE
        -- Indian equity: ~375 1m candles per day (9:15-15:30 = 375 min)
        WHEN exchange = 'angel_one' AND timeframe = '1m' THEN 375
        -- Crypto: 1440 1m candles per day (24h)
        WHEN exchange = 'binance' AND timeframe = '1m' THEN 1440
        -- Daily: 1 candle per trading day
        WHEN timeframe = '1d' THEN 1
        ELSE NULL
    END AS expected_candles,
    ROUND(
        COUNT(*)::NUMERIC / NULLIF(
            CASE
                WHEN exchange = 'angel_one' AND timeframe = '1m' THEN 375
                WHEN exchange = 'binance' AND timeframe = '1m' THEN 1440
                WHEN timeframe = '1d' THEN 1
                ELSE NULL
            END, 0
        ) * 100, 1
    ) AS completeness_pct
FROM ohlcv
GROUP BY date_trunc('day', timestamp), symbol, exchange, timeframe
ORDER BY day DESC, symbol;

-- Anomaly summary: count of quality issues per symbol per day
CREATE OR REPLACE VIEW data_anomaly_summary AS
SELECT
    date_trunc('day', timestamp) AS day,
    symbol,
    exchange,
    issue_type,
    severity,
    COUNT(*) AS issue_count
FROM data_quality_log
GROUP BY date_trunc('day', timestamp), symbol, exchange, issue_type, severity
ORDER BY day DESC, issue_count DESC;

-- Latency summary: average latencies per symbol (last 24 hours)
CREATE OR REPLACE VIEW latency_summary_24h AS
SELECT
    symbol,
    exchange,
    data_type,
    COUNT(*) AS sample_count,
    ROUND(AVG(ingestion_latency_ms)::NUMERIC, 1) AS avg_ingestion_ms,
    ROUND(AVG(processing_latency_ms)::NUMERIC, 1) AS avg_processing_ms,
    ROUND(AVG(e2e_latency_ms)::NUMERIC, 1) AS avg_e2e_ms,
    ROUND(MAX(e2e_latency_ms)::NUMERIC, 1) AS max_e2e_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY e2e_latency_ms)::NUMERIC, 1) AS p95_e2e_ms
FROM pipeline_latency
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY symbol, exchange, data_type
ORDER BY avg_e2e_ms DESC;

-- Overall data health: one row per symbol with key metrics
CREATE OR REPLACE VIEW data_health_overview AS
SELECT
    i.symbol,
    i.exchange,
    i.asset_class,
    i.is_active,
    (SELECT MAX(timestamp) FROM ohlcv WHERE symbol = i.symbol AND exchange = i.exchange AND timeframe = '1m') AS latest_1m,
    (SELECT COUNT(*) FROM ohlcv WHERE symbol = i.symbol AND exchange = i.exchange AND timeframe = '1m'
        AND timestamp > NOW() - INTERVAL '24 hours') AS candles_24h,
    (SELECT COUNT(*) FROM data_quality_log WHERE symbol = i.symbol AND exchange = i.exchange
        AND timestamp > NOW() - INTERVAL '24 hours') AS issues_24h,
    (SELECT ROUND(AVG(e2e_latency_ms)::NUMERIC, 1) FROM pipeline_latency
        WHERE symbol = i.symbol AND exchange = i.exchange
        AND timestamp > NOW() - INTERVAL '1 hour') AS avg_latency_1h_ms
FROM instruments i
WHERE i.is_active = TRUE
ORDER BY i.symbol;
