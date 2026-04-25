"""Quant Trading System — Core Enumerations.

Defines all enums used across the system for type safety and consistency.
"""

from enum import StrEnum


class Exchange(StrEnum):
    """Supported exchange / data source identifiers."""

    ANGEL_ONE = "angel_one"
    BINANCE = "binance"
    YFINANCE = "yfinance"


class AssetClass(StrEnum):
    """Supported asset classes."""

    EQUITY = "equity"
    EQUITY_FNO = "equity_fno"
    CRYPTO_FUTURES = "crypto_futures"
    FOREX = "forex"  # Reserved for future use


class Timeframe(StrEnum):
    """Supported candlestick timeframes.

    The value is the standard short-form used across APIs and storage.
    """

    M1 = "1m"
    M3 = "3m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"
    W1 = "1w"

    @property
    def minutes(self) -> int:
        """Return the timeframe duration in minutes."""
        mapping = {
            "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "4h": 240, "1d": 1440, "1w": 10080,
        }
        return mapping[self.value]

    @property
    def is_intraday(self) -> bool:
        """Check if this is an intraday timeframe."""
        return self.minutes < 1440


class DataQuality(StrEnum):
    """Data quality classification for each record."""

    RAW = "raw"                  # Unprocessed, straight from source
    CLEAN = "clean"              # Passed all validation checks
    INTERPOLATED = "interpolated"  # Gap-filled via forward-fill or interpolation
    SUSPICIOUS = "suspicious"    # Flagged by anomaly detection, needs review


class MarketStatus(StrEnum):
    """Current market trading status."""

    PRE_OPEN = "pre_open"
    OPEN = "open"
    CLOSED = "closed"
    HOLIDAY = "holiday"


class ConnectionStatus(StrEnum):
    """WebSocket / API connection status."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"
