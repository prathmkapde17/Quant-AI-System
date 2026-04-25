-- Phase 2: Feature Store Schema
-- Stores pre-calculated technical indicators and alpha signals.

CREATE TABLE IF NOT EXISTS features (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    feature_value DOUBLE PRECISION NOT NULL,
    metadata JSONB -- For parameters like { "period": 14 }
);

-- Convert to hypertable for performance
SELECT create_hypertable('features', 'timestamp', if_not_exists => TRUE);

-- Create index for fast retrieval of all features for a symbol/timeframe
CREATE INDEX IF NOT EXISTS idx_features_symbol_timeframe 
ON features (symbol, exchange, timeframe, timestamp DESC);

-- Unique constraint to prevent duplicate feature calculations for the same bar
CREATE UNIQUE INDEX IF NOT EXISTS idx_features_unique 
ON features (timestamp, symbol, exchange, timeframe, feature_name);

-- Retention: Keep features for 3 months (can be adjusted)
SELECT add_retention_policy('features', INTERVAL '90 days', if_not_exists => TRUE);
