"""Quant Trading System — Live Data Ingester.

Manages concurrent WebSocket connections for real-time market data,
buffers ticks/candles for efficient batch DB inserts, publishes to
Redis Streams, and handles graceful shutdown.
"""

from __future__ import annotations

import asyncio
import signal
from datetime import datetime, timezone
from typing import Any

from src.core.config import get_settings
from src.core.enums import Exchange
from src.core.logging import get_logger
from src.core.models import OHLCV, Tick
from src.connectors.base import AbstractConnector
from src.storage.database import Database
from src.storage.repository import OHLCVRepository, TickRepository
from src.api.latency import LatencyTracker
from src.feature_engineering import FeatureManager

log = get_logger(__name__)


class LiveIngester:
    """Manages live data ingestion from multiple exchanges.

    Architecture:
    1. Exchange WebSockets → on_tick / on_candle callbacks
    2. Callbacks → buffer ticks and candles in memory
    3. Periodic flush → batch insert buffered data to DB
    4. (Optional) Publish to Redis Streams for downstream consumers

    Features:
    - Concurrent WebSocket connections (Angel One + Binance)
    - Configurable flush interval and batch size
    - Heartbeat monitoring for stale data detection
    - Graceful shutdown on SIGTERM/SIGINT
    """

    def __init__(
        self,
        db: Database,
        connectors: dict[Exchange, AbstractConnector],
        publisher: Any | None = None,  # StreamPublisher
        latency_tracker: LatencyTracker | None = None,
    ):
        """Initialize the live ingester.

        Args:
            db: Database connection instance.
            connectors: Map of Exchange → AbstractConnector.
            publisher: Optional Redis StreamPublisher for real-time bus.
            latency_tracker: Optional latency tracker for pipeline metrics.
        """
        self._db = db
        self._connectors = connectors
        self._publisher = publisher
        self._latency_tracker = latency_tracker or LatencyTracker(db)

        self._ohlcv_repo = OHLCVRepository(db)
        self._tick_repo = TickRepository(db)
        self._feature_manager = FeatureManager(db)
        self._settings = get_settings()
        self._cfg = self._settings.ingestion.live

        # Buffers
        self._tick_buffer: list[Tick] = []
        self._candle_buffer: list[OHLCV] = []
        self._buffer_lock = asyncio.Lock()

        # State
        self._running = False
        self._flush_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._last_tick_times: dict[str, datetime] = {}  # symbol → last tick time

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    async def start(self, symbols_per_exchange: dict[Exchange, list[str]]) -> None:
        """Start live ingestion for all configured exchanges.

        Args:
            symbols_per_exchange: Map of Exchange → list of symbols to subscribe.
        """
        self._running = True

        log.info(
            "live_ingester_starting",
            exchanges=list(symbols_per_exchange.keys()),
        )

        # Start periodic flush task
        self._flush_task = asyncio.create_task(self._flush_loop())

        # Start heartbeat monitor
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # Subscribe to each exchange
        for exchange, symbols in symbols_per_exchange.items():
            connector = self._connectors.get(exchange)
            if not connector or not connector.is_connected:
                log.warning(
                    "live_ingester_skip_exchange",
                    exchange=exchange.value,
                    reason="not connected",
                )
                continue

            try:
                await connector.subscribe_live(
                    symbols=symbols,
                    on_tick=self._on_tick,
                    on_candle=self._on_candle,
                )
                log.info(
                    "live_ingester_subscribed",
                    exchange=exchange.value,
                    symbols=symbols,
                )
            except Exception as e:
                log.error(
                    "live_ingester_subscribe_failed",
                    exchange=exchange.value,
                    error=str(e),
                )

    async def stop(self) -> None:
        """Gracefully stop live ingestion.

        1. Signal all loops to stop
        2. Unsubscribe from exchanges
        3. Flush remaining buffer
        4. Cancel background tasks
        """
        log.info("live_ingester_stopping")
        self._running = False

        # Unsubscribe from all exchanges
        for exchange, connector in self._connectors.items():
            try:
                await connector.unsubscribe_live()
            except Exception as e:
                log.warning(
                    "live_ingester_unsubscribe_error",
                    exchange=exchange.value,
                    error=str(e),
                )

        # Flush remaining data
        await self._flush_buffers()

        # Cancel background tasks
        for task in [self._flush_task, self._heartbeat_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        log.info("live_ingester_stopped")

    # -------------------------------------------------------------------------
    # Callbacks (invoked by exchange connectors)
    # -------------------------------------------------------------------------

    async def _on_tick(self, tick: Tick) -> None:
        """Handle an incoming tick from any exchange.

        1. Buffer the tick for batch DB insert
        2. Publish to Redis Streams (if publisher configured)
        3. Update last tick time for heartbeat monitoring
        """
        async with self._buffer_lock:
            # Circuit Breaker: drop oldest if buffer is full
            if len(self._tick_buffer) >= self._cfg.max_tick_buffer_size:
                log.warning(
                    "live_ingester_tick_buffer_full",
                    limit=self._cfg.max_tick_buffer_size,
                    dropping=1,
                )
                self._tick_buffer.pop(0)
            self._tick_buffer.append(tick)

        # Track last tick time
        self._last_tick_times[f"{tick.exchange.value}:{tick.symbol}"] = tick.timestamp

        # Track latency (sampled)
        latency_rec = self._latency_tracker.start(
            symbol=tick.symbol,
            exchange=tick.exchange.value,
            data_type="tick",
            exchange_ts=tick.timestamp,
        )

        # Publish to Redis (fire-and-forget)
        if self._publisher:
            try:
                await self._publisher.publish_tick(tick)
                if latency_rec:
                    latency_rec.stamp_published()
            except Exception as e:
                log.warning(
                    "live_ingester_publish_tick_error",
                    symbol=tick.symbol,
                    error=str(e),
                )

    async def _on_candle(self, candle: OHLCV) -> None:
        """Handle a completed candle from any exchange.

        1. Buffer for batch DB insert
        2. Publish to Redis Streams
        """
        async with self._buffer_lock:
            # Circuit Breaker: drop oldest if buffer is full
            if len(self._candle_buffer) >= self._cfg.max_candle_buffer_size:
                log.warning(
                    "live_ingester_candle_buffer_full",
                    limit=self._cfg.max_candle_buffer_size,
                    dropping=1,
                )
                self._candle_buffer.pop(0)
            self._candle_buffer.append(candle)

        # Track latency (sampled)
        latency_rec = self._latency_tracker.start(
            symbol=candle.symbol,
            exchange=candle.exchange.value,
            data_type="candle",
            exchange_ts=candle.timestamp,
        )

        if self._publisher:
            try:
                await self._publisher.publish_candle(candle)
                if latency_rec:
                    latency_rec.stamp_published()
            except Exception as e:
                log.warning(
                    "live_ingester_publish_candle_error",
                    symbol=candle.symbol,
                    error=str(e),
                )

    # -------------------------------------------------------------------------
    # Background Loops
    # -------------------------------------------------------------------------

    async def _flush_loop(self) -> None:
        """Periodically flush buffered data to the database."""
        while self._running:
            try:
                await asyncio.sleep(self._cfg.db_flush_interval)
                await self._flush_buffers()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("live_ingester_flush_error", error=str(e))

    async def _flush_buffers(self) -> None:
        """Drain the tick and candle buffers and insert into DB."""
        async with self._buffer_lock:
            ticks = self._tick_buffer.copy()
            candles = self._candle_buffer.copy()
            self._tick_buffer.clear()
            self._candle_buffer.clear()

        # Insert ticks
        if ticks:
            try:
                count = await self._tick_repo.bulk_insert(ticks)
                log.debug("live_ingester_ticks_flushed", count=count)
            except Exception as e:
                log.error(
                    "live_ingester_tick_flush_failed",
                    count=len(ticks),
                    error=str(e),
                )
                # Re-buffer failed ticks (up to max batch size)
                async with self._buffer_lock:
                    self._tick_buffer = ticks[-self._cfg.db_flush_batch_size :] + self._tick_buffer

        # Insert candles
        if candles:
            try:
                count = await self._ohlcv_repo.upsert_many(candles)
                log.debug("live_ingester_candles_flushed", count=count)
                
                # Update features for unique symbol/exchange/timeframe combos
                unique_combos = set((c.symbol, c.exchange, c.timeframe) for c in candles)
                for symbol, exchange, timeframe in unique_combos:
                    try:
                        # Fetch context (last 100 bars) for indicator calculation
                        bars = await self._ohlcv_repo.get_recent_bars(symbol, exchange, timeframe, limit=100)
                        if len(bars) >= 30:
                            import pandas as pd
                            df = pd.DataFrame(bars)
                            await self._feature_manager.process_symbol(symbol, exchange, timeframe, df)
                    except Exception as fe_err:
                        log.warning("feature_calculation_error", symbol=symbol, error=str(fe_err))

                # Flush latency records
                await self._latency_tracker.flush()
            except Exception as e:
                log.error(
                    "live_ingester_candle_flush_failed",
                    count=len(candles),
                    error=str(e),
                )

    async def _heartbeat_loop(self) -> None:
        """Monitor data freshness and log warnings for stale symbols."""
        while self._running:
            try:
                await asyncio.sleep(self._cfg.heartbeat_interval)

                now = datetime.now(timezone.utc)
                threshold = self._cfg.stale_data_threshold

                for key, last_time in self._last_tick_times.items():
                    age = (now - last_time).total_seconds()
                    if age > threshold:
                        log.warning(
                            "live_ingester_stale_data",
                            symbol_key=key,
                            age_seconds=round(age, 1),
                            threshold=threshold,
                        )

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("live_ingester_heartbeat_error", error=str(e))

    # -------------------------------------------------------------------------
    # Stats
    # -------------------------------------------------------------------------

    @property
    def buffer_stats(self) -> dict[str, int]:
        """Get current buffer sizes."""
        return {
            "tick_buffer_size": len(self._tick_buffer),
            "candle_buffer_size": len(self._candle_buffer),
            "tracked_symbols": len(self._last_tick_times),
        }

    @property
    def last_tick_times(self) -> dict[str, datetime]:
        """Get last tick times per symbol (for health monitoring)."""
        return dict(self._last_tick_times)
