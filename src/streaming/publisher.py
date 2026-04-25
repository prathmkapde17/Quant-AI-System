"""Quant Trading System — Redis Stream Publisher.

Publishes normalized OHLCV candles and ticks to Redis Streams
for real-time downstream consumption by signal generators,
dashboards, and other consumers.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as redis

from src.core.config import get_settings
from src.core.exceptions import PublishError
from src.core.logging import get_logger
from src.core.models import OHLCV, Tick

log = get_logger(__name__)


class StreamPublisher:
    """Publishes market data to Redis Streams.

    Stream naming convention:
        market:{exchange}:{symbol}:tick   — real-time ticks
        market:{exchange}:{symbol}:candle — completed candles

    Features:
    - Automatic stream trimming (MAXLEN ~N)
    - JSON serialization of Pydantic models
    - Batch publishing for efficiency
    - Connection health checking
    """

    def __init__(self, redis_client: redis.Redis | None = None):
        """Initialize the publisher.

        Args:
            redis_client: Optional pre-configured Redis client.
                          If None, creates one from settings.
        """
        self._settings = get_settings()
        self._cfg = self._settings.redis
        self._redis = redis_client
        self._max_len = self._cfg.stream_max_len

    async def connect(self) -> None:
        """Connect to Redis if not already connected."""
        if self._redis is None:
            self._redis = redis.Redis(
                host=self._cfg.host,
                port=self._cfg.port,
                db=self._cfg.db,
                decode_responses=True,
                max_connections=self._cfg.max_connections,
            )

        # Test connection
        try:
            await self._redis.ping()
            log.info("redis_publisher_connected")
        except redis.RedisError as e:
            raise PublishError(f"Redis connection failed: {e}") from e

    async def disconnect(self) -> None:
        """Close the Redis connection."""
        if self._redis:
            await self._redis.aclose()
            self._redis = None
            log.info("redis_publisher_disconnected")

    # -------------------------------------------------------------------------
    # Publishing
    # -------------------------------------------------------------------------

    async def publish_tick(self, tick: Tick) -> str:
        """Publish a tick to its Redis Stream.

        Args:
            tick: Tick model instance.

        Returns:
            Stream message ID.
        """
        stream_key = f"market:{tick.exchange.value}:{tick.symbol}:tick"

        data = {
            "timestamp": tick.timestamp.isoformat(),
            "symbol": tick.symbol,
            "exchange": tick.exchange.value,
            "ltp": str(tick.ltp),
            "volume": str(tick.volume),
        }

        if tick.bid is not None:
            data["bid"] = str(tick.bid)
        if tick.ask is not None:
            data["ask"] = str(tick.ask)
        if tick.oi is not None:
            data["oi"] = str(tick.oi)

        try:
            msg_id = await self._redis.xadd(
                stream_key,
                data,
                maxlen=self._max_len,
                approximate=True,
            )
            return msg_id
        except redis.RedisError as e:
            raise PublishError(
                f"Failed to publish tick to {stream_key}: {e}",
            ) from e

    async def publish_candle(self, candle: OHLCV) -> str:
        """Publish a completed candle to its Redis Stream.

        Args:
            candle: OHLCV model instance.

        Returns:
            Stream message ID.
        """
        stream_key = f"market:{candle.exchange.value}:{candle.symbol}:candle"

        data = {
            "timestamp": candle.timestamp.isoformat(),
            "symbol": candle.symbol,
            "exchange": candle.exchange.value,
            "timeframe": candle.timeframe.value,
            "open": str(candle.open),
            "high": str(candle.high),
            "low": str(candle.low),
            "close": str(candle.close),
            "volume": str(candle.volume),
        }

        if candle.turnover is not None:
            data["turnover"] = str(candle.turnover)
        if candle.num_trades is not None:
            data["num_trades"] = str(candle.num_trades)

        try:
            msg_id = await self._redis.xadd(
                stream_key,
                data,
                maxlen=self._max_len,
                approximate=True,
            )
            return msg_id
        except redis.RedisError as e:
            raise PublishError(
                f"Failed to publish candle to {stream_key}: {e}",
            ) from e

    async def publish_batch(
        self,
        ticks: list[Tick] | None = None,
        candles: list[OHLCV] | None = None,
    ) -> dict[str, int]:
        """Publish multiple ticks and candles in a pipeline.

        Uses Redis pipeline for efficiency.

        Args:
            ticks: List of ticks to publish.
            candles: List of candles to publish.

        Returns:
            Dict with counts: {"ticks": N, "candles": M}.
        """
        tick_count = 0
        candle_count = 0

        async with self._redis.pipeline(transaction=False) as pipe:
            if ticks:
                for tick in ticks:
                    stream_key = f"market:{tick.exchange.value}:{tick.symbol}:tick"
                    data = {
                        "timestamp": tick.timestamp.isoformat(),
                        "symbol": tick.symbol,
                        "exchange": tick.exchange.value,
                        "ltp": str(tick.ltp),
                        "volume": str(tick.volume),
                    }
                    pipe.xadd(stream_key, data, maxlen=self._max_len, approximate=True)
                    tick_count += 1

            if candles:
                for candle in candles:
                    stream_key = f"market:{candle.exchange.value}:{candle.symbol}:candle"
                    data = {
                        "timestamp": candle.timestamp.isoformat(),
                        "symbol": candle.symbol,
                        "exchange": candle.exchange.value,
                        "timeframe": candle.timeframe.value,
                        "open": str(candle.open),
                        "high": str(candle.high),
                        "low": str(candle.low),
                        "close": str(candle.close),
                        "volume": str(candle.volume),
                    }
                    pipe.xadd(stream_key, data, maxlen=self._max_len, approximate=True)
                    candle_count += 1

            try:
                await pipe.execute()
            except redis.RedisError as e:
                raise PublishError(f"Batch publish failed: {e}") from e

        return {"ticks": tick_count, "candles": candle_count}

    # -------------------------------------------------------------------------
    # Utilities
    # -------------------------------------------------------------------------

    async def get_stream_info(self, stream_key: str) -> dict[str, Any]:
        """Get info about a Redis Stream.

        Args:
            stream_key: Full stream key.

        Returns:
            Dict with length, first/last entry IDs, etc.
        """
        try:
            info = await self._redis.xinfo_stream(stream_key)
            return {
                "length": info.get("length", 0),
                "first_entry": info.get("first-entry"),
                "last_entry": info.get("last-entry"),
            }
        except redis.RedisError:
            return {"length": 0, "error": "stream not found"}

    async def get_all_stream_lengths(self) -> dict[str, int]:
        """Get lengths of all market data streams.

        Returns:
            Dict of {stream_key: message_count}.
        """
        result = {}
        try:
            # Scan for all market:* streams
            async for key in self._redis.scan_iter("market:*"):
                length = await self._redis.xlen(key)
                result[key] = length
        except redis.RedisError as e:
            log.warning("stream_length_scan_error", error=str(e))

        return result
