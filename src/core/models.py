"""Quant Trading System — Pydantic Data Models.

Defines the canonical data structures used across the entire pipeline.
All exchange-specific formats are normalized into these models.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator

from src.core.enums import AssetClass, ConnectionStatus, DataQuality, Exchange, Timeframe


# =============================================================================
# Market Data Models
# =============================================================================


class OHLCV(BaseModel):
    """Normalized OHLCV candlestick — the core data unit.

    Every candle from every exchange gets converted to this format.
    Timestamps are always in UTC.
    """

    timestamp: datetime = Field(description="Candle open time in UTC")
    symbol: str = Field(description="Instrument symbol (e.g., RELIANCE, BTCUSDT)")
    exchange: Exchange = Field(description="Source exchange")
    timeframe: Timeframe = Field(description="Candle timeframe")
    open: float = Field(ge=0, description="Open price")
    high: float = Field(ge=0, description="High price")
    low: float = Field(ge=0, description="Low price")
    close: float = Field(ge=0, description="Close price")
    volume: float = Field(ge=0, description="Trading volume")
    turnover: float | None = Field(default=None, description="Turnover / quote volume")
    num_trades: int | None = Field(default=None, ge=0, description="Number of trades")
    quality: DataQuality = Field(default=DataQuality.RAW, description="Data quality flag")

    model_config = {"frozen": True}

    @field_validator("high")
    @classmethod
    def high_gte_low(cls, v: float, info) -> float:
        """Ensure high >= low when both are available."""
        if "low" in info.data and info.data["low"] is not None and v < info.data["low"]:
            msg = f"high ({v}) must be >= low ({info.data['low']})"
            raise ValueError(msg)
        return v

    @property
    def mid(self) -> float:
        """Mid price: (high + low) / 2."""
        return (self.high + self.low) / 2

    @property
    def typical_price(self) -> float:
        """Typical price: (high + low + close) / 3."""
        return (self.high + self.low + self.close) / 3

    @property
    def bar_range(self) -> float:
        """Bar range: high - low."""
        return self.high - self.low

    def to_db_tuple(self) -> tuple:
        """Convert to a tuple matching the DB insert column order."""
        return (
            self.timestamp, self.symbol, self.exchange.value, self.timeframe.value,
            self.open, self.high, self.low, self.close, self.volume,
            self.turnover, self.num_trades, self.quality.value,
        )


class Tick(BaseModel):
    """Real-time tick / quote data from a live feed.

    Represents a single price update from an exchange WebSocket.
    """

    timestamp: datetime = Field(description="Tick time in UTC")
    symbol: str = Field(description="Instrument symbol")
    exchange: Exchange = Field(description="Source exchange")
    ltp: float = Field(ge=0, description="Last traded price")
    volume: float = Field(default=0, ge=0, description="Traded volume at this tick")
    bid: float | None = Field(default=None, ge=0, description="Best bid price")
    ask: float | None = Field(default=None, ge=0, description="Best ask price")
    oi: float | None = Field(default=None, ge=0, description="Open interest (futures)")
    turnover: float | None = Field(default=None, description="Cumulative turnover")

    model_config = {"frozen": True}

    @property
    def spread(self) -> float | None:
        """Bid-ask spread, if both are available."""
        if self.bid is not None and self.ask is not None:
            return self.ask - self.bid
        return None

    def to_db_tuple(self) -> tuple:
        """Convert to a tuple matching the DB insert column order."""
        return (
            self.timestamp, self.symbol, self.exchange.value,
            self.ltp, self.volume, self.bid, self.ask, self.oi,
        )


# =============================================================================
# Instrument Metadata
# =============================================================================


class Instrument(BaseModel):
    """Instrument metadata — describes a tradeable asset.

    Each exchange has its own identifier (token) for the same logical instrument.
    """

    symbol: str = Field(description="Canonical symbol (e.g., RELIANCE, BTCUSDT)")
    exchange: Exchange = Field(description="Exchange this instrument belongs to")
    asset_class: AssetClass = Field(description="Asset class classification")
    name: str = Field(default="", description="Human-readable name")
    lot_size: float = Field(default=1.0, ge=0, description="Minimum tradeable lot size")
    tick_size: float = Field(default=0.01, ge=0, description="Minimum price increment")
    expiry: datetime | None = Field(default=None, description="Expiry date for derivatives")
    is_active: bool = Field(default=True, description="Whether this instrument is currently tracked")
    exchange_token: str | None = Field(default=None, description="Exchange-specific instrument token/ID")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Exchange-specific extra fields")

    model_config = {"frozen": True}

    @property
    def unique_key(self) -> str:
        """Unique identifier: exchange:symbol."""
        return f"{self.exchange.value}:{self.symbol}"


# =============================================================================
# Health & Status Models
# =============================================================================


class ComponentHealth(BaseModel):
    """Health status of a single system component."""

    name: str
    status: str = Field(description="healthy | degraded | unhealthy")
    latency_ms: float | None = Field(default=None, description="Response latency in ms")
    last_check: datetime | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class ConnectionHealth(BaseModel):
    """WebSocket connection health for an exchange."""

    exchange: Exchange
    status: ConnectionStatus
    connected_since: datetime | None = None
    last_message_at: datetime | None = None
    reconnect_count: int = 0
    subscribed_symbols: list[str] = Field(default_factory=list)


class HealthReport(BaseModel):
    """Overall system health report."""

    timestamp: datetime
    status: str = Field(description="healthy | degraded | unhealthy")
    components: list[ComponentHealth] = Field(default_factory=list)
    connections: list[ConnectionHealth] = Field(default_factory=list)


# =============================================================================
# Validation Result
# =============================================================================


class ValidationResult(BaseModel):
    """Result of validating a set of OHLCV records."""

    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    violations: list[dict[str, Any]] = Field(default_factory=list)

    @property
    def pass_rate(self) -> float:
        """Percentage of records that passed validation."""
        if self.total_records == 0:
            return 0.0
        return (self.valid_records / self.total_records) * 100

    @property
    def is_acceptable(self) -> bool:
        """Check if the data quality is acceptable (> 95% pass rate)."""
        return self.pass_rate >= 95.0
