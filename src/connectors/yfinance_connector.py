"""Quant Trading System — yfinance Fallback Connector.

Provides historical daily/weekly data for backtesting depth using the
yfinance library. No live streaming — this is a historical-only source.
Runs synchronously under the hood (yfinance is not async-native).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import yfinance as yf

from src.core.enums import AssetClass, ConnectionStatus, Exchange, Timeframe
from src.core.exceptions import ConnectorDataError
from src.core.logging import get_logger
from src.core.models import OHLCV, Instrument, Tick
from src.connectors.base import AbstractConnector, OnCandleCallback, OnTickCallback

log = get_logger(__name__)


# yfinance interval mapping (only a subset is supported)
_TIMEFRAME_TO_YF_INTERVAL = {
    Timeframe.M1: "1m",
    Timeframe.M5: "5m",
    Timeframe.M15: "15m",
    Timeframe.M30: "30m",
    Timeframe.H1: "1h",
    Timeframe.D1: "1d",
    Timeframe.W1: "1wk",
}

# yfinance max period for intraday data
_INTRADAY_MAX_DAYS = 7


class YFinanceConnector(AbstractConnector):
    """yfinance fallback connector for deep historical data.

    Capabilities:
    - Daily data: 10+ years depth
    - Intraday: last 7 days only (yfinance limitation)
    - No live streaming (use Angel One / Binance for that)

    Note: yfinance is synchronous. We run it in a thread executor
    to avoid blocking the event loop.
    """

    def __init__(self):
        super().__init__(Exchange.YFINANCE)

    async def connect(self) -> None:
        """No auth needed for yfinance — just mark as connected."""
        self._status = ConnectionStatus.CONNECTED
        log.info("yfinance_connected")

    async def disconnect(self) -> None:
        """Nothing to close for yfinance."""
        self._status = ConnectionStatus.DISCONNECTED
        log.info("yfinance_disconnected")

    # -------------------------------------------------------------------------
    # Historical Data
    # -------------------------------------------------------------------------

    async def fetch_historical(
        self,
        symbol: str,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> list[OHLCV]:
        """Fetch historical data from yfinance.

        Runs the yfinance download in a thread pool to avoid blocking.

        Args:
            symbol: Ticker symbol (for NSE stocks, append ".NS", e.g., "RELIANCE.NS").
            timeframe: Candle timeframe.
            start: Start time (UTC).
            end: End time (UTC).

        Returns:
            List of OHLCV objects sorted by timestamp ascending.
        """
        interval = _TIMEFRAME_TO_YF_INTERVAL.get(timeframe)
        if not interval:
            raise ConnectorDataError(
                f"Timeframe {timeframe} not supported by yfinance",
            )

        # Run yfinance in a thread (it's synchronous / uses requests internally)
        loop = asyncio.get_event_loop()
        candles = await loop.run_in_executor(
            None,
            self._fetch_sync,
            symbol,
            interval,
            timeframe,
            start,
            end,
        )

        log.info(
            "yfinance_historical_fetched",
            symbol=symbol,
            timeframe=timeframe.value,
            count=len(candles),
            start=str(start),
            end=str(end),
        )

        return candles

    def _fetch_sync(
        self,
        symbol: str,
        interval: str,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> list[OHLCV]:
        """Synchronous yfinance download (runs in thread pool).

        Args:
            symbol: Ticker symbol.
            interval: yfinance interval string.
            timeframe: Our Timeframe enum.
            start: Start time.
            end: End time.

        Returns:
            List of OHLCV objects.
        """
        try:
            ticker = yf.Ticker(symbol)

            # yfinance uses period-based fetch for intraday, date-based for daily+
            df = ticker.history(
                interval=interval,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                auto_adjust=True,   # Adjusted prices by default
                actions=False,       # Don't include dividends/splits columns
            )

            if df.empty:
                log.warning("yfinance_no_data", symbol=symbol, interval=interval)
                return []

            candles: list[OHLCV] = []

            for idx, row in df.iterrows():
                try:
                    # Convert pandas Timestamp to UTC datetime
                    ts = idx.to_pydatetime()
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    else:
                        ts = ts.astimezone(timezone.utc)

                    candle = OHLCV(
                        timestamp=ts,
                        symbol=symbol,
                        exchange=Exchange.YFINANCE,
                        timeframe=timeframe,
                        open=float(row["Open"]),
                        high=float(row["High"]),
                        low=float(row["Low"]),
                        close=float(row["Close"]),
                        volume=float(row.get("Volume", 0)),
                    )
                    candles.append(candle)
                except (KeyError, ValueError, TypeError) as e:
                    log.warning(
                        "yfinance_row_parse_error",
                        symbol=symbol,
                        error=str(e),
                    )

            return sorted(candles, key=lambda c: c.timestamp)

        except Exception as e:
            log.error(
                "yfinance_fetch_error",
                symbol=symbol,
                error=str(e),
            )
            raise ConnectorDataError(
                f"yfinance fetch failed for {symbol}: {e}",
            ) from e

    # -------------------------------------------------------------------------
    # Live Streaming (Not Supported)
    # -------------------------------------------------------------------------

    async def subscribe_live(
        self,
        symbols: list[str],
        on_tick: OnTickCallback | None = None,
        on_candle: OnCandleCallback | None = None,
    ) -> None:
        """Not supported by yfinance — raises NotImplementedError."""
        raise NotImplementedError(
            "yfinance does not support live streaming. "
            "Use Angel One or Binance connectors for live data."
        )

    async def unsubscribe_live(self, symbols: list[str] | None = None) -> None:
        """Not supported by yfinance."""
        pass  # No-op

    # -------------------------------------------------------------------------
    # Instrument Discovery
    # -------------------------------------------------------------------------

    async def get_instruments(self) -> list[Instrument]:
        """Return instruments for our tracked Indian equities via yfinance.

        yfinance doesn't have a proper instrument discovery API.
        We return our configured instruments with ".NS" suffix for NSE.

        Returns:
            List of Instrument objects.
        """
        # Load from config to know which symbols to look up
        from src.storage.metadata import InstrumentMetadataManager

        config_instruments = InstrumentMetadataManager.load_from_yaml()

        instruments: list[Instrument] = []
        for inst in config_instruments:
            if inst.asset_class in (AssetClass.EQUITY, AssetClass.EQUITY_FNO):
                # For NSE stocks, yfinance uses "SYMBOL.NS" format
                yf_symbol = f"{inst.symbol}.NS"
                instruments.append(
                    Instrument(
                        symbol=yf_symbol,
                        exchange=Exchange.YFINANCE,
                        asset_class=inst.asset_class,
                        name=inst.name,
                        exchange_token=yf_symbol,
                    )
                )

        log.info("yfinance_instruments_loaded", count=len(instruments))
        return instruments
