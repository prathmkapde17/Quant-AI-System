"""Quant Trading System — Historical Data Ingester.

Batch backfill engine that fetches historical candle data from exchanges,
with incremental sync (only fetches missing data), parallel fetching with
rate limiting, exponential backoff on errors, and progress tracking.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from src.core.config import get_settings
from src.core.enums import Exchange, Timeframe
from src.core.exceptions import ConnectorRateLimitError, HistoricalFetchError
from src.core.logging import get_logger
from src.connectors.base import AbstractConnector
from src.storage.database import Database
from src.storage.raw_store import RawDataStore
from src.storage.repository import OHLCVRepository
from src.api.latency import LatencyTracker

log = get_logger(__name__)


class HistoricalIngester:
    """Orchestrates historical data backfill from exchange connectors.

    Features:
    - Incremental sync: checks DB for latest timestamp and fetches only new data
    - Parallel fetching: uses asyncio.Semaphore to respect per-exchange rate limits
    - Exponential backoff: retries on transient failures
    - Batch DB writes: inserts in configurable batch sizes
    - Progress tracking: logs progress per symbol/timeframe
    """

    def __init__(
        self,
        db: Database,
        connectors: dict[Exchange, AbstractConnector],
        raw_store: RawDataStore | None = None,
        latency_tracker: LatencyTracker | None = None,
    ):
        """Initialize the historical ingester.

        Args:
            db: Database connection instance.
            connectors: Map of Exchange → AbstractConnector.
            raw_store: Optional raw data lake for immutable storage.
            latency_tracker: Optional latency tracker for pipeline metrics.
        """
        self._db = db
        self._connectors = connectors
        self._repo = OHLCVRepository(db)
        self._raw_store = raw_store or RawDataStore()
        self._latency_tracker = latency_tracker or LatencyTracker(db)
        self._settings = get_settings()
        self._cfg = self._settings.ingestion.historical

    async def backfill(
        self,
        exchange: Exchange,
        symbols: list[str],
        timeframe: Timeframe,
        lookback_days: int | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dict[str, int]:
        """Backfill historical data for multiple symbols.

        Uses asyncio.Semaphore to limit concurrent API calls per exchange.

        Args:
            exchange: Exchange to fetch from.
            symbols: List of symbols to backfill.
            timeframe: Candle timeframe.
            lookback_days: Days to look back from now. Overrides start.
            start: Explicit start time (UTC). If None, uses lookback_days.
            end: End time (UTC). Defaults to now.

        Returns:
            Dict of {symbol: records_inserted}.
        """
        connector = self._connectors.get(exchange)
        if not connector:
            raise HistoricalFetchError(
                f"No connector configured for {exchange.value}",
            )

        if not connector.is_connected:
            raise HistoricalFetchError(
                f"Connector for {exchange.value} is not connected",
            )

        # Determine time range
        if end is None:
            end = datetime.now(timezone.utc)

        if start is None:
            if lookback_days is None:
                lookback_days = (
                    self._cfg.default_lookback_days_intraday
                    if timeframe.is_intraday
                    else self._cfg.default_lookback_days_daily
                )
            start = end - timedelta(days=lookback_days)

        log.info(
            "backfill_started",
            exchange=exchange.value,
            symbols=symbols,
            timeframe=timeframe.value,
            start=str(start),
            end=str(end),
        )

        # Parallel fetch with semaphore
        semaphore = asyncio.Semaphore(self._cfg.max_concurrent_fetches)
        results: dict[str, int] = {}

        tasks = [
            self._backfill_symbol(
                connector=connector,
                symbol=symbol,
                timeframe=timeframe,
                start=start,
                end=end,
                semaphore=semaphore,
            )
            for symbol in symbols
        ]

        completed = await asyncio.gather(*tasks, return_exceptions=True)

        for symbol, result in zip(symbols, completed):
            if isinstance(result, Exception):
                log.error(
                    "backfill_symbol_failed",
                    symbol=symbol,
                    exchange=exchange.value,
                    error=str(result),
                )
                results[symbol] = 0
            else:
                results[symbol] = result

        total = sum(results.values())
        log.info(
            "backfill_complete",
            exchange=exchange.value,
            timeframe=timeframe.value,
            total_records=total,
            symbols_succeeded=sum(1 for v in results.values() if v > 0),
            symbols_failed=sum(1 for v in results.values() if v == 0),
        )

        return results

    async def _backfill_symbol(
        self,
        connector: AbstractConnector,
        symbol: str,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
        semaphore: asyncio.Semaphore,
    ) -> int:
        """Backfill a single symbol with incremental sync and retries.

        Args:
            connector: Exchange connector instance.
            symbol: Symbol to backfill.
            timeframe: Candle timeframe.
            start: Requested start time.
            end: Requested end time.
            semaphore: Concurrency limiter.

        Returns:
            Number of records inserted.
        """
        async with semaphore:
            # Check DB for existing data → incremental sync
            latest = await self._repo.get_latest_timestamp(
                symbol=symbol,
                exchange=connector.exchange,
                timeframe=timeframe,
            )

            if latest is not None:
                # Start from just after the last candle we have
                actual_start = latest + timedelta(minutes=timeframe.minutes)
                if actual_start >= end:
                    log.info(
                        "backfill_symbol_up_to_date",
                        symbol=symbol,
                        latest=str(latest),
                    )
                    return 0
            else:
                actual_start = start

            log.info(
                "backfill_symbol_started",
                symbol=symbol,
                exchange=connector.exchange.value,
                timeframe=timeframe.value,
                start=str(actual_start),
                end=str(end),
            )

            # Fetch with retry
            candles = await self._fetch_with_retry(
                connector=connector,
                symbol=symbol,
                timeframe=timeframe,
                start=actual_start,
                end=end,
            )

            if not candles:
                log.info("backfill_symbol_no_new_data", symbol=symbol)
                return 0

            # Save raw data to Parquet (immutable source of truth)
            try:
                raw_data = [c.to_db_tuple() for c in candles]
                self._raw_store.save_historical_candles(
                    raw_data=raw_data,
                    symbol=symbol,
                    exchange=connector.exchange,
                    timeframe=timeframe,
                )
            except Exception as e:
                log.warning("raw_store_save_error", symbol=symbol, error=str(e))

            # Track latency (sampled)
            latency_rec = self._latency_tracker.start(
                symbol=symbol,
                exchange=connector.exchange.value,
                data_type="candle",
                exchange_ts=candles[0].timestamp if candles else None,
            )

            # Insert in batches
            total_inserted = 0
            batch_size = self._cfg.batch_size

            for i in range(0, len(candles), batch_size):
                batch = candles[i : i + batch_size]
                inserted = await self._repo.upsert_many(batch)
                total_inserted += inserted

            # Stamp storage latency
            if latency_rec:
                latency_rec.stamp_stored()

            # Flush latency records periodically
            if self._latency_tracker.buffer_size >= 50:
                await self._latency_tracker.flush()

            log.info(
                "backfill_symbol_complete",
                symbol=symbol,
                exchange=connector.exchange.value,
                records_inserted=total_inserted,
                time_range=f"{candles[0].timestamp} → {candles[-1].timestamp}",
            )

            return total_inserted

    async def _fetch_with_retry(
        self,
        connector: AbstractConnector,
        symbol: str,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> list:
        """Fetch historical data with exponential backoff on failure.

        Args:
            connector: Exchange connector.
            symbol: Symbol to fetch.
            timeframe: Candle timeframe.
            start: Start time.
            end: End time.

        Returns:
            List of OHLCV objects.

        Raises:
            HistoricalFetchError: After max retries exhausted.
        """
        last_error: Exception | None = None

        for attempt in range(1, self._cfg.retry_max_attempts + 1):
            try:
                return await connector.fetch_historical(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=start,
                    end=end,
                )
            except ConnectorRateLimitError as e:
                delay = e.retry_after or (self._cfg.retry_base_delay * attempt)
                log.warning(
                    "backfill_rate_limited",
                    symbol=symbol,
                    attempt=attempt,
                    delay=delay,
                )
                await asyncio.sleep(delay)
                last_error = e
            except Exception as e:
                delay = self._cfg.retry_base_delay * (2 ** (attempt - 1))
                log.warning(
                    "backfill_fetch_retry",
                    symbol=symbol,
                    attempt=attempt,
                    max_attempts=self._cfg.retry_max_attempts,
                    delay=delay,
                    error=str(e),
                )
                await asyncio.sleep(delay)
                last_error = e

        raise HistoricalFetchError(
            f"Failed to fetch {symbol} after {self._cfg.retry_max_attempts} attempts: {last_error}",
            details={"symbol": symbol, "last_error": str(last_error)},
        )

    async def backfill_all(
        self,
        exchange: Exchange,
        timeframes: list[Timeframe] | None = None,
        lookback_days: int | None = None,
    ) -> dict[str, dict[str, int]]:
        """Backfill all configured instruments for an exchange across timeframes.

        Args:
            exchange: Exchange to backfill.
            timeframes: List of timeframes. Defaults to [M1, D1].
            lookback_days: Override lookback days.

        Returns:
            Nested dict: {timeframe: {symbol: records_inserted}}.
        """
        from src.storage.metadata import InstrumentMetadataManager

        metadata = InstrumentMetadataManager(self._db)
        instruments = metadata.get_all(exchange)

        if not instruments:
            log.warning("backfill_no_instruments", exchange=exchange.value)
            return {}

        symbols = [inst.symbol for inst in instruments]

        if timeframes is None:
            timeframes = [Timeframe.M1, Timeframe.D1]

        results: dict[str, dict[str, int]] = {}

        for tf in timeframes:
            log.info(
                "backfill_timeframe_started",
                exchange=exchange.value,
                timeframe=tf.value,
                num_symbols=len(symbols),
            )
            results[tf.value] = await self.backfill(
                exchange=exchange,
                symbols=symbols,
                timeframe=tf,
                lookback_days=lookback_days,
            )

        return results
