"""Quant Trading System — Abstract Connector Interface.

Defines the contract that all exchange connectors must implement.
This ensures a uniform API regardless of the data source.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Coroutine

from src.core.enums import ConnectionStatus, Exchange, Timeframe
from src.core.models import OHLCV, Instrument, Tick


# Type aliases for callback signatures
OnTickCallback = Callable[[Tick], Coroutine[Any, Any, None]]
OnCandleCallback = Callable[[OHLCV], Coroutine[Any, Any, None]]


class AbstractConnector(ABC):
    """Base class for all exchange data connectors.

    Every connector (Angel One, Binance, yfinance) implements this interface
    so the ingestion engine can treat them uniformly.
    """

    def __init__(self, exchange: Exchange):
        self._exchange = exchange
        self._status = ConnectionStatus.DISCONNECTED

    @property
    def exchange(self) -> Exchange:
        """The exchange this connector is for."""
        return self._exchange

    @property
    def status(self) -> ConnectionStatus:
        """Current connection status."""
        return self._status

    @property
    def is_connected(self) -> bool:
        """Whether the connector is currently connected and ready."""
        return self._status == ConnectionStatus.CONNECTED

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection / authenticate with the exchange.

        Should set self._status to CONNECTED on success.
        Raises ConnectorAuthError on authentication failure.
        """

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully close all connections.

        Should set self._status to DISCONNECTED.
        """

    # -------------------------------------------------------------------------
    # Historical Data
    # -------------------------------------------------------------------------

    @abstractmethod
    async def fetch_historical(
        self,
        symbol: str,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> list[OHLCV]:
        """Fetch historical candlestick data for a symbol.

        Implementations handle auto-pagination (e.g., Angel One 8000/request,
        Binance 1500/request) and rate limiting internally.

        Args:
            symbol: Instrument symbol (e.g., "RELIANCE", "BTCUSDT").
            timeframe: Candle timeframe.
            start: Start time (inclusive, UTC).
            end: End time (inclusive, UTC).

        Returns:
            List of OHLCV objects, sorted by timestamp ascending.

        Raises:
            ConnectorError: On fetch failure.
            ConnectorRateLimitError: If rate limit is hit.
        """

    # -------------------------------------------------------------------------
    # Live Streaming
    # -------------------------------------------------------------------------

    @abstractmethod
    async def subscribe_live(
        self,
        symbols: list[str],
        on_tick: OnTickCallback | None = None,
        on_candle: OnCandleCallback | None = None,
    ) -> None:
        """Subscribe to live market data via WebSocket.

        Implementations handle auto-reconnect with exponential backoff.

        Args:
            symbols: List of symbols to subscribe to.
            on_tick: Async callback for each tick update.
            on_candle: Async callback for each completed candle.

        Raises:
            WebSocketError: On connection failure.
        """

    @abstractmethod
    async def unsubscribe_live(self, symbols: list[str] | None = None) -> None:
        """Unsubscribe from live data.

        Args:
            symbols: Specific symbols to unsubscribe. None = unsubscribe all.
        """

    # -------------------------------------------------------------------------
    # Instrument Discovery
    # -------------------------------------------------------------------------

    @abstractmethod
    async def get_instruments(self) -> list[Instrument]:
        """Fetch the list of available instruments from the exchange.

        For Angel One: downloads and parses the master contract file.
        For Binance: calls the exchange info endpoint.

        Returns:
            List of Instrument objects with exchange_token populated.
        """

    # -------------------------------------------------------------------------
    # Utilities
    # -------------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} exchange={self._exchange.value} status={self._status.value}>"
