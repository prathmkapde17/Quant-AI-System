"""Quant Trading System — Configuration Loader.

Loads configuration from config/settings.yaml with environment variable overrides.
Secrets (API keys, passwords) are ALWAYS loaded from environment variables.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# =============================================================================
# Configuration Sub-Models
# =============================================================================


class DatabaseConfig(BaseModel):
    """TimescaleDB connection configuration."""

    host: str = "localhost"
    port: int = 5432
    name: str = "quantdb"
    user: str = "quant"
    password: str = "quantpass"
    min_connections: int = 5
    max_connections: int = 20
    command_timeout: int = 60

    @property
    def dsn(self) -> str:
        """PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    @property
    def async_dsn(self) -> str:
        """Async PostgreSQL connection string for asyncpg."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class RedisConfig(BaseModel):
    """Redis connection configuration."""

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    max_connections: int = 20
    stream_max_len: int = 10000
    decode_responses: bool = True

    @property
    def url(self) -> str:
        """Redis connection URL."""
        return f"redis://{self.host}:{self.port}/{self.db}"


class AngelOneConfig(BaseModel):
    """Angel One SmartAPI configuration."""

    client_id: str = ""
    password: str = ""
    api_key: str = ""
    totp_secret: str = ""
    rate_limit_per_second: int = 3
    historical_candles_per_request: int = 8000
    websocket_reconnect_delay: int = 5
    websocket_max_reconnect: int = 50
    master_contract_url: str = (
        "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    )

    @property
    def is_configured(self) -> bool:
        """Check if Angel One credentials are set."""
        return bool(self.client_id and self.password and self.api_key)


class BinanceConfig(BaseModel):
    """Binance Futures API configuration."""

    api_key: str = ""
    api_secret: str = ""
    futures_base_url: str = "https://fapi.binance.com"
    rate_limit_weight_per_minute: int = 1200
    klines_per_request: int = 1500
    websocket_reconnect_delay: int = 3
    websocket_max_reconnect: int = 100
    testnet: bool = False

    @property
    def is_configured(self) -> bool:
        """Check if Binance credentials are set."""
        return bool(self.api_key and self.api_secret)


class HistoricalIngestionConfig(BaseModel):
    """Historical data fetching configuration."""

    default_lookback_days_intraday: int = 730
    default_lookback_days_daily: int = 3650
    batch_size: int = 500
    max_concurrent_fetches: int = 3
    retry_max_attempts: int = 5
    retry_base_delay: int = 2


class LiveIngestionConfig(BaseModel):
    """Live data streaming configuration."""

    db_flush_interval: int = 5
    db_flush_batch_size: int = 1000
    heartbeat_interval: int = 30
    stale_data_threshold: int = 120
    max_tick_buffer_size: int = 10000
    max_candle_buffer_size: int = 1000


class SchedulerConfig(BaseModel):
    """Task scheduler configuration."""

    daily_backfill_time: str = "15:45"
    instrument_refresh_hours: int = 6
    health_check_seconds: int = 60
    compression_day: str = "sunday"
    compression_time: str = "02:00"


class IngestionConfig(BaseModel):
    """Combined ingestion configuration."""

    historical: HistoricalIngestionConfig = Field(default_factory=HistoricalIngestionConfig)
    live: LiveIngestionConfig = Field(default_factory=LiveIngestionConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)


class CleaningConfig(BaseModel):
    """Data cleaning configuration."""

    max_gap_candles_to_fill: int = 5
    outlier_zscore_threshold: float = 4.0
    equity_max_single_candle_pct: float = 0.10
    crypto_max_single_candle_pct: float = 0.20


class AppConfig(BaseModel):
    """Top-level application metadata."""

    name: str = "quant-trading-system"
    version: str = "0.1.0"
    log_level: str = "INFO"
    timezone: str = "UTC"


# =============================================================================
# Main Settings Class
# =============================================================================


class Settings(BaseSettings):
    """Root settings — loads from YAML file + environment variables.

    Priority order:
    1. Environment variables (highest — for secrets)
    2. YAML config file
    3. Default values (lowest)
    """

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app: AppConfig = Field(default_factory=AppConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    angel_one: AngelOneConfig = Field(default_factory=AngelOneConfig)
    binance: BinanceConfig = Field(default_factory=BinanceConfig)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    cleaning: CleaningConfig = Field(default_factory=CleaningConfig)


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override dict into base dict."""
    merged = base.copy()
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_settings(
    config_path: str | Path | None = None,
    env_overrides: dict[str, Any] | None = None,
) -> Settings:
    """Load settings from YAML config file with environment variable overrides.

    Args:
        config_path: Path to settings.yaml. Defaults to config/settings.yaml
                     relative to project root.
        env_overrides: Additional overrides (useful for testing).

    Returns:
        Fully resolved Settings instance.
    """
    if config_path is None:
        # Find config relative to this file's location
        project_root = Path(__file__).resolve().parent.parent.parent
        config_path = project_root / "config" / "settings.yaml"
    else:
        config_path = Path(config_path)

    # Load YAML
    yaml_data: dict[str, Any] = {}
    if config_path.exists():
        with open(config_path, encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f) or {}

    # Apply env_overrides
    if env_overrides:
        yaml_data = _deep_merge(yaml_data, env_overrides)

    # Inject secrets from environment variables
    import os

    # Database password
    db_password = os.environ.get("DB_PASSWORD")
    if db_password:
        yaml_data.setdefault("database", {})["password"] = db_password

    # Angel One credentials
    for env_key, config_key in [
        ("ANGEL_ONE_CLIENT_ID", "client_id"),
        ("ANGEL_ONE_PASSWORD", "password"),
        ("ANGEL_ONE_API_KEY", "api_key"),
        ("ANGEL_ONE_TOTP_SECRET", "totp_secret"),
    ]:
        env_val = os.environ.get(env_key)
        if env_val:
            yaml_data.setdefault("angel_one", {})[config_key] = env_val

    # Binance credentials
    for env_key, config_key in [
        ("BINANCE_API_KEY", "api_key"),
        ("BINANCE_API_SECRET", "api_secret"),
    ]:
        env_val = os.environ.get(env_key)
        if env_val:
            yaml_data.setdefault("binance", {})[config_key] = env_val

    return Settings(**yaml_data)


# Module-level singleton (lazy loaded)
_settings: Settings | None = None


def get_settings() -> Settings:
    """Get the global settings singleton.

    Call load_settings() first if you need custom config path.
    Otherwise, this loads from the default location.
    """
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings


def reset_settings() -> None:
    """Reset the settings singleton (useful for testing)."""
    global _settings
    _settings = None
