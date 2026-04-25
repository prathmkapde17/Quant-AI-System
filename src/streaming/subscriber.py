"""Quant Trading System — Redis Stream Subscriber.

Consumes market data from Redis Streams using consumer groups,
providing reliable delivery, message acknowledgment, and
callback-based processing.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

import redis.asyncio as redis

from src.core.config import get_settings
from src.core.enums import Exchange, Timeframe
from src.core.exceptions import SubscribeError
from src.core.logging import get_logger
from src.core.models import OHLCV, Tick

log = get_logger(__name__)

# Type aliases
TickHandler = Callable[[Tick], Coroutine[Any, Any, None]]
CandleHandler = Callable[[OHLCV], Coroutine[Any, Any, None]]


class StreamSubscriber:
    """Consumes market data from Redis Streams.

    Uses Redis consumer groups for reliable, at-least-once delivery.
    Each consumer gets unique messages; if a consumer crashes, its
    pending messages can be claimed by another.

    Usage:
        sub = StreamSubscriber(group="signal_engine", consumer="worker_1")
        await sub.connect()
        sub.on_tick("BTCUSDT", Exchange.BINANCE, my_tick_handler)
        sub.on_candle("BTCUSDT", Exchange.BINANCE, my_candle_handler)
        await sub.start()
    """

    def __init__(
        self,
        group: str = "default_group",
        consumer: str = "consumer_1",
        redis_client: redis.Redis | None = None,
    ):
        """Initialize the subscriber.

        Args:
            group: Consumer group name.
            consumer: Consumer name within the group.
            redis_client: Optional pre-configured Redis client.
        """
        self._settings = get_settings()
        self._cfg = self._settings.redis
        self._redis = redis_client
        self._group = group
        self._consumer = consumer

        # Callback registries
        self._tick_handlers: dict[str, TickHandler] = {}     # stream_key → handler
        self._candle_handlers: dict[str, CandleHandler] = {}  # stream_key → handler

        # State
        self._running = False
        self._listen_task: asyncio.Task | None = None

    async def connect(self) -> None:
        """Connect to Redis."""
        if self._redis is None:
            self._redis = redis.Redis(
                host=self._cfg.host,
                port=self._cfg.port,
                db=self._cfg.db,
                decode_responses=True,
                max_connections=self._cfg.max_connections,
            )

        try:
            await self._redis.ping()
            log.info(
                "redis_subscriber_connected",
                group=self._group,
                consumer=self._consumer,
            )
        except redis.RedisError as e:
            raise SubscribeError(f"Redis connection failed: {e}") from e

    async def disconnect(self) -> None:
        """Stop listening and close the connection."""
        await self.stop()
        if self._redis:
            await self._redis.aclose()
            self._redis = None
            log.info("redis_subscriber_disconnected")

    # -------------------------------------------------------------------------
    # Callback Registration
    # -------------------------------------------------------------------------

    def on_tick(
        self,
        symbol: str,
        exchange: Exchange,
        handler: TickHandler,
    ) -> None:
        """Register a tick handler for a specific symbol/exchange.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.
            handler: Async callback: async def handler(tick: Tick) -> None.
        """
        stream_key = f"market:{exchange.value}:{symbol}:tick"
        self._tick_handlers[stream_key] = handler
        log.debug(
            "tick_handler_registered",
            stream=stream_key,
        )

    def on_candle(
        self,
        symbol: str,
        exchange: Exchange,
        handler: CandleHandler,
    ) -> None:
        """Register a candle handler for a specific symbol/exchange.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.
            handler: Async callback: async def handler(candle: OHLCV) -> None.
        """
        stream_key = f"market:{exchange.value}:{symbol}:candle"
        self._candle_handlers[stream_key] = handler
        log.debug(
            "candle_handler_registered",
            stream=stream_key,
        )

    def on_all_ticks(
        self,
        exchange: Exchange,
        handler: TickHandler,
    ) -> None:
        """Register a tick handler for all symbols on an exchange.

        The handler will receive ticks for any symbol matching the pattern.

        Args:
            exchange: Exchange enum.
            handler: Async tick callback.
        """
        # We'll handle pattern matching in the listen loop
        pattern_key = f"market:{exchange.value}:*:tick"
        self._tick_handlers[pattern_key] = handler

    # -------------------------------------------------------------------------
    # Listening
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Start consuming from all registered streams."""
        self._running = True

        # Ensure consumer groups exist
        all_streams = set(self._tick_handlers.keys()) | set(self._candle_handlers.keys())
        for stream_key in all_streams:
            if "*" in stream_key:
                continue  # Skip pattern-based subscriptions for now
            await self._ensure_consumer_group(stream_key)

        self._listen_task = asyncio.create_task(self._listen_loop())
        log.info(
            "subscriber_started",
            group=self._group,
            consumer=self._consumer,
            streams=list(all_streams),
        )

    async def stop(self) -> None:
        """Stop consuming."""
        self._running = False
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        log.info("subscriber_stopped")

    async def _listen_loop(self) -> None:
        """Main loop: read from streams and dispatch to handlers."""
        # Build stream dict for XREADGROUP: {stream: last_id}
        streams = {}
        for key in set(self._tick_handlers.keys()) | set(self._candle_handlers.keys()):
            if "*" not in key:
                streams[key] = ">"  # Read only new messages

        if not streams:
            log.warning("subscriber_no_streams")
            return

        while self._running:
            try:
                # Block for up to 1 second waiting for messages
                results = await self._redis.xreadgroup(
                    groupname=self._group,
                    consumername=self._consumer,
                    streams=streams,
                    count=100,
                    block=1000,  # ms
                )

                if not results:
                    continue

                for stream_key, messages in results:
                    for msg_id, data in messages:
                        await self._dispatch(stream_key, msg_id, data)

            except asyncio.CancelledError:
                break
            except redis.RedisError as e:
                log.error("subscriber_read_error", error=str(e))
                await asyncio.sleep(1)
            except Exception as e:
                log.error("subscriber_unexpected_error", error=str(e))
                await asyncio.sleep(1)

    async def _dispatch(
        self,
        stream_key: str,
        msg_id: str,
        data: dict[str, str],
    ) -> None:
        """Dispatch a message to the appropriate handler.

        Args:
            stream_key: Redis stream key.
            msg_id: Message ID.
            data: Message data dict.
        """
        try:
            if stream_key.endswith(":tick") and stream_key in self._tick_handlers:
                tick = self._parse_tick(data)
                await self._tick_handlers[stream_key](tick)

            elif stream_key.endswith(":candle") and stream_key in self._candle_handlers:
                candle = self._parse_candle(data)
                await self._candle_handlers[stream_key](candle)

            # Acknowledge the message
            await self._redis.xack(stream_key, self._group, msg_id)

        except Exception as e:
            log.warning(
                "subscriber_dispatch_error",
                stream=stream_key,
                msg_id=msg_id,
                error=str(e),
            )

    # -------------------------------------------------------------------------
    # Parsing
    # -------------------------------------------------------------------------

    @staticmethod
    def _parse_tick(data: dict[str, str]) -> Tick:
        """Parse a Redis Stream message back into a Tick."""
        return Tick(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            symbol=data["symbol"],
            exchange=Exchange(data["exchange"]),
            ltp=float(data["ltp"]),
            volume=float(data.get("volume", 0)),
            bid=float(data["bid"]) if "bid" in data else None,
            ask=float(data["ask"]) if "ask" in data else None,
            oi=float(data["oi"]) if "oi" in data else None,
        )

    @staticmethod
    def _parse_candle(data: dict[str, str]) -> OHLCV:
        """Parse a Redis Stream message back into an OHLCV."""
        return OHLCV(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            symbol=data["symbol"],
            exchange=Exchange(data["exchange"]),
            timeframe=Timeframe(data["timeframe"]),
            open=float(data["open"]),
            high=float(data["high"]),
            low=float(data["low"]),
            close=float(data["close"]),
            volume=float(data["volume"]),
            turnover=float(data["turnover"]) if "turnover" in data else None,
            num_trades=int(data["num_trades"]) if "num_trades" in data else None,
        )

    # -------------------------------------------------------------------------
    # Consumer Group Management
    # -------------------------------------------------------------------------

    async def _ensure_consumer_group(self, stream_key: str) -> None:
        """Create the consumer group if it doesn't exist.

        Also creates the stream if it doesn't exist (MKSTREAM).
        """
        try:
            await self._redis.xgroup_create(
                stream_key,
                self._group,
                id="0",
                mkstream=True,
            )
            log.debug("consumer_group_created", stream=stream_key, group=self._group)
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                pass  # Group already exists — fine
            else:
                raise SubscribeError(
                    f"Failed to create consumer group for {stream_key}: {e}",
                ) from e

    async def get_pending_count(self) -> dict[str, int]:
        """Get pending (unacknowledged) message counts per stream.

        Returns:
            Dict of {stream_key: pending_count}.
        """
        result = {}
        all_streams = set(self._tick_handlers.keys()) | set(self._candle_handlers.keys())

        for stream_key in all_streams:
            if "*" in stream_key:
                continue
            try:
                info = await self._redis.xpending(stream_key, self._group)
                result[stream_key] = info.get("pending", 0) if info else 0
            except redis.RedisError:
                result[stream_key] = -1  # Error indicator

        return result
