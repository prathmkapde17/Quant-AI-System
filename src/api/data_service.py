"""Quant Trading System — Data Service.

High-level query interface for accessing historical and real-time market data.
Wraps the repository layer with DataFrame conversion, multi-symbol queries,
and an in-memory LRU cache for hot-path performance.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any

import pandas as pd

from src.core.enums import DataQuality, Exchange, Timeframe
from src.core.logging import get_logger
from src.core.models import OHLCV
from src.storage.database import Database
from src.storage.repository import OHLCVRepository, TickRepository

log = get_logger(__name__)

# Maximum entries in the DataFrame cache
_CACHE_MAX_SIZE = 128


class DataService:
    """Unified data access layer for the pipeline.

    This is the primary interface downstream consumers (feature engine,
    signal generator, notebooks) use to access market data.

    Usage:
        svc = DataService(db)
        df = await svc.get_historical("RELIANCE", Exchange.ANGEL_ONE, Timeframe.D1,
                                       start, end)
        latest = await svc.get_latest_bar("BTCUSDT", Exchange.BINANCE, Timeframe.M1)
        multi = await svc.get_multiple(["BTCUSDT", "ETHUSDT"], Exchange.BINANCE,
                                        Timeframe.D1, start, end)
    """

    def __init__(self, db: Database):
        self._db = db
        self._ohlcv_repo = OHLCVRepository(db)
        self._tick_repo = TickRepository(db)
        self._cache: dict[str, pd.DataFrame] = {}

    # -------------------------------------------------------------------------
    # Historical Queries
    # -------------------------------------------------------------------------

    async def get_historical(
        self,
        symbol: str,
        exchange: Exchange,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
        quality: DataQuality | None = None,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """Fetch historical OHLCV data as a pandas DataFrame.

        Args:
            symbol: Instrument symbol (e.g., "RELIANCE", "BTCUSDT").
            exchange: Exchange enum.
            timeframe: Candle timeframe.
            start: Start time (inclusive, UTC).
            end: End time (inclusive, UTC).
            quality: Optional filter by data quality flag.
            use_cache: Whether to check/populate the in-memory cache.

        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume,
            turnover, num_trades, quality. Indexed by timestamp.
        """
        # Cache key
        cache_key = f"{symbol}:{exchange.value}:{timeframe.value}:{start.isoformat()}:{end.isoformat()}"

        if use_cache and cache_key in self._cache:
            log.debug("data_service_cache_hit", key=cache_key)
            return self._cache[cache_key]

        rows = await self._ohlcv_repo.get_ohlcv(
            symbol=symbol,
            exchange=exchange,
            timeframe=timeframe,
            start=start,
            end=end,
            quality=quality,
        )

        df = self._rows_to_dataframe(rows)

        # Cache the result (evict oldest if full)
        if use_cache and not df.empty:
            if len(self._cache) >= _CACHE_MAX_SIZE:
                oldest_key = next(iter(self._cache))
                del self._cache[oldest_key]
            self._cache[cache_key] = df

        log.debug(
            "data_service_historical_fetched",
            symbol=symbol,
            timeframe=timeframe.value,
            rows=len(df),
        )

        return df

    async def get_latest_bar(
        self,
        symbol: str,
        exchange: Exchange,
        timeframe: Timeframe,
    ) -> dict[str, Any] | None:
        """Get the single most recent candle for a symbol.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.
            timeframe: Candle timeframe.

        Returns:
            Dict with OHLCV fields, or None if no data.
        """
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=7)  # Look back 7 days max

        rows = await self._ohlcv_repo.get_ohlcv(
            symbol=symbol,
            exchange=exchange,
            timeframe=timeframe,
            start=start,
            end=end,
            limit=1,
        )

        return rows[0] if rows else None

    async def get_multiple(
        self,
        symbols: list[str],
        exchange: Exchange,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> dict[str, pd.DataFrame]:
        """Fetch historical data for multiple symbols concurrently.

        Args:
            symbols: List of instrument symbols.
            exchange: Exchange enum.
            timeframe: Candle timeframe.
            start: Start time (UTC).
            end: End time (UTC).

        Returns:
            Dict of {symbol: DataFrame}.
        """
        tasks = [
            self.get_historical(symbol, exchange, timeframe, start, end)
            for symbol in symbols
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        output: dict[str, pd.DataFrame] = {}
        for symbol, result in zip(symbols, results):
            if isinstance(result, Exception):
                log.warning(
                    "data_service_multi_fetch_error",
                    symbol=symbol,
                    error=str(result),
                )
                output[symbol] = pd.DataFrame()
            else:
                output[symbol] = result

        return output

    # -------------------------------------------------------------------------
    # Real-time Queries
    # -------------------------------------------------------------------------

    async def get_latest_tick(
        self,
        symbol: str,
        exchange: Exchange,
    ) -> dict[str, Any] | None:
        """Get the most recent tick for a symbol.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.

        Returns:
            Tick dict or None.
        """
        return await self._tick_repo.get_latest_tick(symbol, exchange)

    async def get_recent_ticks(
        self,
        symbol: str,
        exchange: Exchange,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Get recent ticks as a DataFrame.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.
            limit: Max ticks to return.

        Returns:
            DataFrame with tick data.
        """
        rows = await self._tick_repo.get_latest_ticks(symbol, exchange, limit)
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        if "timestamp" in df.columns:
            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)
        return df

    # -------------------------------------------------------------------------
    # Statistics
    # -------------------------------------------------------------------------

    async def get_data_summary(
        self,
        exchange: Exchange | None = None,
    ) -> list[dict[str, Any]]:
        """Get a summary of all available data in the database.

        Returns:
            List of dicts with symbol, exchange, timeframe, record_count,
            earliest, latest timestamps.
        """
        query = """
            SELECT
                symbol,
                exchange,
                timeframe,
                COUNT(*) as record_count,
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest
            FROM ohlcv
        """
        args: list[Any] = []

        if exchange:
            query += " WHERE exchange = $1"
            args.append(exchange.value)

        query += " GROUP BY symbol, exchange, timeframe ORDER BY symbol, timeframe"

        rows = await self._db.fetch(query, *args)
        return [dict(row) for row in rows]

    # -------------------------------------------------------------------------
    # Cache Management
    # -------------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear the in-memory DataFrame cache."""
        self._cache.clear()
        log.debug("data_service_cache_cleared")

    def invalidate_cache(self, symbol: str | None = None) -> int:
        """Invalidate cache entries, optionally for a specific symbol.

        Args:
            symbol: If provided, only invalidate entries for this symbol.

        Returns:
            Number of cache entries removed.
        """
        if symbol is None:
            count = len(self._cache)
            self._cache.clear()
            return count

        keys_to_remove = [k for k in self._cache if k.startswith(f"{symbol}:")]
        for k in keys_to_remove:
            del self._cache[k]
        return len(keys_to_remove)

    @property
    def cache_size(self) -> int:
        """Current number of cached DataFrames."""
        return len(self._cache)

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _rows_to_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
        """Convert a list of DB row dicts into a properly indexed DataFrame."""
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        # Set timestamp as index
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)

        # Ensure numeric types
        numeric_cols = ["open", "high", "low", "close", "volume", "turnover", "num_trades"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Drop metadata columns that downstream consumers don't need
        drop_cols = ["exchange", "timeframe", "symbol"]
        df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True, errors="ignore")

        return df
