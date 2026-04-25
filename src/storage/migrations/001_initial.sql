-- =============================================================================
-- Quant Trading System — Initial Schema Migration
-- Database: TimescaleDB (PostgreSQL 16 + TimescaleDB extension)
-- =============================================================================

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- =============================================================================
-- 1. OHLCV Hypertable — Core candlestick data
-- =============================================================================

CREATE TABLE IF NOT EXISTS ohlcv (
    timestamp     TIMESTAMPTZ   NOT NULL,
    symbol        TEXT          NOT NULL,
    exchange      TEXT          NOT NULL,
    timeframe     TEXT          NOT NULL,
    open          DOUBLE PRECISION NOT NULL,
    high          DOUBLE PRECISION NOT NULL,
    low           DOUBLE PRECISION NOT NULL,
    close         DOUBLE PRECISION NOT NULL,
    volume        DOUBLE PRECISION NOT NULL DEFAULT 0,
    turnover      DOUBLE PRECISION,
    num_trades    INTEGER,
    quality       TEXT          NOT NULL DEFAULT 'raw',
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Convert to hypertable, partitioned by time (7-day chunks for intraday data)
SELECT create_hypertable(
    'ohlcv', 'timestamp',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Unique constraint for idempotent upserts
CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_unique
    ON ohlcv (timestamp, symbol, exchange, timeframe);

-- Query index: fetch candles for a symbol/exchange/timeframe over a time range
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_tf
    ON ohlcv (symbol, exchange, timeframe, timestamp DESC);


-- =============================================================================
-- 2. Ticks Hypertable — Real-time tick data
-- =============================================================================

CREATE TABLE IF NOT EXISTS ticks (
    timestamp     TIMESTAMPTZ   NOT NULL,
    symbol        TEXT          NOT NULL,
    exchange      TEXT          NOT NULL,
    ltp           DOUBLE PRECISION NOT NULL,
    volume        DOUBLE PRECISION NOT NULL DEFAULT 0,
    bid           DOUBLE PRECISION,
    ask           DOUBLE PRECISION,
    oi            DOUBLE PRECISION,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Convert to hypertable (1-day chunks for high-throughput tick data)
SELECT create_hypertable(
    'ticks', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Index for querying ticks by symbol
CREATE INDEX IF NOT EXISTS idx_ticks_symbol
    ON ticks (symbol, exchange, timestamp DESC);


-- =============================================================================
-- 3. Instruments Table — Metadata for tracked assets
-- =============================================================================

CREATE TABLE IF NOT EXISTS instruments (
    id              SERIAL PRIMARY KEY,
    symbol          TEXT          NOT NULL,
    exchange        TEXT          NOT NULL,
    asset_class     TEXT          NOT NULL,
    name            TEXT          NOT NULL DEFAULT '',
    lot_size        DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    tick_size       DOUBLE PRECISION NOT NULL DEFAULT 0.01,
    expiry          TIMESTAMPTZ,
    is_active       BOOLEAN       NOT NULL DEFAULT TRUE,
    exchange_token  TEXT,
    metadata        JSONB         NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_instrument UNIQUE (symbol, exchange)
);

-- Index for quick lookup by exchange token
CREATE INDEX IF NOT EXISTS idx_instruments_token
    ON instruments (exchange_token)
    WHERE exchange_token IS NOT NULL;


-- =============================================================================
-- 4. Data Quality Log — Track anomalies and cleaning actions
-- =============================================================================

CREATE TABLE IF NOT EXISTS data_quality_log (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ   NOT NULL,
    symbol          TEXT          NOT NULL,
    exchange        TEXT          NOT NULL,
    timeframe       TEXT,
    issue_type      TEXT          NOT NULL,   -- 'gap', 'outlier', 'invalid_ohlc', etc.
    severity        TEXT          NOT NULL DEFAULT 'warning',  -- 'info', 'warning', 'error'
    description     TEXT,
    raw_value       JSONB,                    -- Original problematic data
    corrected_value JSONB,                    -- What it was corrected to (if any)
    auto_fixed      BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_log_symbol
    ON data_quality_log (symbol, exchange, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_dq_log_type
    ON data_quality_log (issue_type, severity);


-- =============================================================================
-- 5. Ingestion Progress — Track backfill state for resumability
-- =============================================================================

CREATE TABLE IF NOT EXISTS ingestion_progress (
    id              SERIAL PRIMARY KEY,
    symbol          TEXT          NOT NULL,
    exchange        TEXT          NOT NULL,
    timeframe       TEXT          NOT NULL,
    last_timestamp  TIMESTAMPTZ   NOT NULL,
    total_records   BIGINT        NOT NULL DEFAULT 0,
    status          TEXT          NOT NULL DEFAULT 'in_progress',  -- 'in_progress', 'complete', 'error'
    error_message   TEXT,
    started_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_ingestion_progress UNIQUE (symbol, exchange, timeframe)
);


-- =============================================================================
-- 6. Continuous Aggregate — Auto-generate daily bars from 1-min data
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', timestamp) AS timestamp,
    symbol,
    exchange,
    '1d'::TEXT AS timeframe,
    first(open, timestamp)  AS open,
    max(high)               AS high,
    min(low)                AS low,
    last(close, timestamp)  AS close,
    sum(volume)             AS volume,
    sum(turnover)           AS turnover,
    sum(num_trades)         AS num_trades
FROM ohlcv
WHERE timeframe = '1m'
GROUP BY time_bucket('1 day', timestamp), symbol, exchange
WITH NO DATA;

-- Refresh policy: update daily aggregate every hour, covering the last 3 days
SELECT add_continuous_aggregate_policy(
    'ohlcv_daily',
    start_offset    => INTERVAL '3 days',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists   => TRUE
);


-- =============================================================================
-- 7. Compression Policies — Compress old data to save disk space
-- =============================================================================

-- Enable compression on ohlcv
ALTER TABLE ohlcv SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, exchange, timeframe',
    timescaledb.compress_orderby = 'timestamp'
);

-- Auto-compress chunks older than 7 days
SELECT add_compression_policy(
    'ohlcv',
    compress_after => INTERVAL '7 days',
    if_not_exists  => TRUE
);

-- Enable compression on ticks
ALTER TABLE ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, exchange',
    timescaledb.compress_orderby = 'timestamp'
);

-- Auto-compress tick chunks older than 3 days
SELECT add_compression_policy(
    'ticks',
    compress_after => INTERVAL '3 days',
    if_not_exists  => TRUE
);


-- =============================================================================
-- 8. Retention Policies (Optional — uncomment if disk space is a concern)
-- =============================================================================

-- Drop raw tick data older than 30 days (we keep aggregated OHLCV forever)
-- SELECT add_retention_policy('ticks', INTERVAL '30 days', if_not_exists => TRUE);


-- =============================================================================
-- 9. Helper Functions
-- =============================================================================

-- Function to update the updated_at timestamp automatically
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for instruments table
DROP TRIGGER IF EXISTS trigger_instruments_updated_at ON instruments;
CREATE TRIGGER trigger_instruments_updated_at
    BEFORE UPDATE ON instruments
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger for ingestion_progress table
DROP TRIGGER IF EXISTS trigger_ingestion_progress_updated_at ON ingestion_progress;
CREATE TRIGGER trigger_ingestion_progress_updated_at
    BEFORE UPDATE ON ingestion_progress
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
