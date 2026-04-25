"""Quant Trading System — Binance Futures Connector.

Handles USDT-M futures historical kline fetching (1500/request with auto-pagination),
live WebSocket streaming via combined streams, and exchange info for instrument discovery.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import aiohttp

from src.core.config import get_settings
from src.core.enums import AssetClass, ConnectionStatus, Exchange, Timeframe
from src.core.exceptions import (
    ConnectorAuthError,
    ConnectorDataError,
    ConnectorRateLimitError,
    ConnectorTimeoutError,
    WebSocketError,
)
from src.core.logging import get_logger
from src.core.models import OHLCV, Instrument, Tick
from src.connectors.base import AbstractConnector, OnCandleCallback, OnTickCallback

log = get_logger(__name__)


# Binance kline interval mapping
_TIMEFRAME_TO_BN_INTERVAL = {
    Timeframe.M1: "1m",
    Timeframe.M3: "3m",
    Timeframe.M5: "5m",
    Timeframe.M15: "15m",
    Timeframe.M30: "30m",
    Timeframe.H1: "1h",
    Timeframe.H4: "4h",
    Timeframe.D1: "1d",
    Timeframe.W1: "1w",
}

# Max klines per request (Binance limit)
_MAX_KLINES_PER_REQUEST = 1500

# Binance WebSocket base URLs
_WS_BASE = "wss://fstream.binance.com"
_WS_TESTNET = "wss://stream.binancefuture.com"


class BinanceFuturesConnector(AbstractConnector):
    """Binance USDT-M Futures connector.

    Features:
    - Historical kline fetching with auto-pagination (1500/request)
    - Rate limit tracking (1200 weight/minute)
    - Combined WebSocket streams for multiple symbols
    - Auto-reconnect with exponential backoff
    - Support for testnet
    """

    def __init__(self):
        super().__init__(Exchange.BINANCE)
        settings = get_settings()
        self._config = settings.binance

        # API credentials
        self._api_key = self._config.api_key
        self._api_secret = self._config.api_secret

        # Session
        self._session: aiohttp.ClientSession | None = None

        # Rate limiting
        self._weight_used: int = 0
        self._weight_reset_time: float = 0
        self._rate_lock = asyncio.Lock()

        # WebSocket state
        self._ws_task: asyncio.Task | None = None
        self._ws_connection: Any = None
        self._subscribed_symbols: list[str] = []
        self._on_tick: OnTickCallback | None = None
        self._on_candle: OnCandleCallback | None = None

        # Base URLs
        if self._config.testnet:
            self._base_url = "https://testnet.binancefuture.com"
            self._ws_base = _WS_TESTNET
        else:
            self._base_url = self._config.futures_base_url
            self._ws_base = _WS_BASE

    # -------------------------------------------------------------------------
    # Connection
    # -------------------------------------------------------------------------

    async def connect(self) -> None:
        """Initialize the Binance Futures session.

        No explicit authentication needed for public data endpoints.
        For private endpoints (orders), API key/secret are sent per request.
        """
        self._status = ConnectionStatus.CONNECTING
        self._session = aiohttp.ClientSession(
            headers={"X-MBX-APIKEY": self._api_key} if self._api_key else {},
        )

        # Test connectivity
        try:
            async with self._session.get(
                f"{self._base_url}/fapi/v1/ping",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    raise ConnectorDataError(f"Binance ping failed: HTTP {resp.status}")

            self._status = ConnectionStatus.CONNECTED
            log.info(
                "binance_futures_connected",
                testnet=self._config.testnet,
            )

        except aiohttp.ClientError as e:
            self._status = ConnectionStatus.ERROR
            raise ConnectorAuthError(
                f"Binance connection failed: {e}",
            ) from e

    async def disconnect(self) -> None:
        """Close the Binance session and WebSocket connections."""
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

        if self._session and not self._session.closed:
            await self._session.close()

        self._session = None
        self._status = ConnectionStatus.DISCONNECTED
        log.info("binance_futures_disconnected")

    # -------------------------------------------------------------------------
    # Rate Limiting
    # -------------------------------------------------------------------------

    async def _check_rate_limit(self, weight: int = 1) -> None:
        """Track and enforce Binance API weight limits.

        Binance Futures allows 1200 weight per minute.
        """
        import time

        async with self._rate_lock:
            now = time.monotonic()

            # Reset counter if a minute has passed
            if now - self._weight_reset_time >= 60:
                self._weight_used = 0
                self._weight_reset_time = now

            # Check if we'd exceed the limit
            if self._weight_used + weight >= self._config.rate_limit_weight_per_minute:
                wait_time = 60 - (now - self._weight_reset_time)
                if wait_time > 0:
                    log.warning(
                        "binance_rate_limit_approaching",
                        weight_used=self._weight_used,
                        waiting=round(wait_time, 1),
                    )
                    await asyncio.sleep(wait_time)
                    self._weight_used = 0
                    self._weight_reset_time = time.monotonic()

            self._weight_used += weight

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
        """Fetch historical klines from Binance Futures.

        Auto-paginates for large date ranges (max 1500 klines/request).

        Args:
            symbol: Trading pair (e.g., "BTCUSDT").
            timeframe: Candle timeframe.
            start: Start time (UTC).
            end: End time (UTC).

        Returns:
            List of OHLCV objects sorted by timestamp ascending.
        """
        if not self._session:
            raise ConnectorAuthError("Not connected. Call connect() first.")

        interval = _TIMEFRAME_TO_BN_INTERVAL.get(timeframe)
        if not interval:
            raise ConnectorDataError(
                f"Timeframe {timeframe} not supported by Binance",
            )

        # Convert to millisecond timestamps
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)

        all_candles: list[OHLCV] = []
        current_start_ms = start_ms

        while current_start_ms < end_ms:
            await self._check_rate_limit(weight=5)  # klines cost 5 weight

            params = {
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": current_start_ms,
                "endTime": end_ms,
                "limit": _MAX_KLINES_PER_REQUEST,
            }

            try:
                async with self._session.get(
                    f"{self._base_url}/fapi/v1/klines",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    # Check rate limit headers
                    used_weight = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                    if used_weight:
                        self._weight_used = int(used_weight)

                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", 60))
                        raise ConnectorRateLimitError(
                            "Binance rate limited",
                            retry_after=retry_after,
                        )

                    if resp.status != 200:
                        text = await resp.text()
                        raise ConnectorDataError(
                            f"Binance klines failed: HTTP {resp.status} — {text}",
                        )

                    klines = await resp.json()

            except asyncio.TimeoutError as e:
                raise ConnectorTimeoutError(
                    f"Binance klines timed out for {symbol}",
                ) from e
            except aiohttp.ClientError as e:
                raise ConnectorDataError(
                    f"Binance klines failed for {symbol}: {e}",
                ) from e

            if not klines:
                break

            # Parse klines: [open_time, open, high, low, close, volume,
            #                 close_time, quote_vol, num_trades, ...]
            for k in klines:
                try:
                    ts = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)

                    candle = OHLCV(
                        timestamp=ts,
                        symbol=symbol.upper(),
                        exchange=Exchange.BINANCE,
                        timeframe=timeframe,
                        open=float(k[1]),
                        high=float(k[2]),
                        low=float(k[3]),
                        close=float(k[4]),
                        volume=float(k[5]),
                        turnover=float(k[7]),       # Quote asset volume
                        num_trades=int(k[8]),
                    )
                    all_candles.append(candle)
                except (IndexError, ValueError, TypeError) as e:
                    log.warning(
                        "binance_kline_parse_error",
                        symbol=symbol,
                        error=str(e),
                    )

            # Pagination: move start to after the last kline
            if len(klines) < _MAX_KLINES_PER_REQUEST:
                break

            # Next start = last kline's close_time + 1ms
            current_start_ms = klines[-1][6] + 1

        log.info(
            "binance_historical_fetched",
            symbol=symbol,
            timeframe=timeframe.value,
            count=len(all_candles),
            start=str(start),
            end=str(end),
        )

        return sorted(all_candles, key=lambda c: c.timestamp)

    # -------------------------------------------------------------------------
    # Live Streaming
    # -------------------------------------------------------------------------

    async def subscribe_live(
        self,
        symbols: list[str],
        on_tick: OnTickCallback | None = None,
        on_candle: OnCandleCallback | None = None,
    ) -> None:
        """Subscribe to live data via Binance Futures combined WebSocket streams.

        Subscribes to both mini-ticker (for ticks) and kline streams (for candles).

        Args:
            symbols: List of trading pairs (e.g., ["BTCUSDT", "ETHUSDT"]).
            on_tick: Async callback for tick updates.
            on_candle: Async callback for completed candles (1m).
        """
        self._on_tick = on_tick
        self._on_candle = on_candle
        self._subscribed_symbols = symbols

        # Build combined stream names
        streams = []
        for sym in symbols:
            s = sym.lower()
            streams.append(f"{s}@miniTicker")    # Real-time price
            streams.append(f"{s}@kline_1m")      # 1-minute klines

        stream_path = "/".join(streams)
        ws_url = f"{self._ws_base}/stream?streams={stream_path}"

        self._ws_task = asyncio.create_task(self._ws_loop(ws_url))

    async def _ws_loop(self, ws_url: str) -> None:
        """WebSocket connection loop with auto-reconnect."""
        import websockets

        reconnect_count = 0

        while reconnect_count < self._config.websocket_max_reconnect:
            try:
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self._ws_connection = ws
                    log.info(
                        "binance_ws_connected",
                        symbols=self._subscribed_symbols,
                        reconnect_count=reconnect_count,
                    )

                    async for message in ws:
                        await self._handle_ws_message(message)

            except Exception as e:
                log.warning(
                    "binance_ws_disconnected",
                    error=str(e),
                    reconnect_count=reconnect_count,
                )

            reconnect_count += 1
            delay = min(
                self._config.websocket_reconnect_delay * (2 ** min(reconnect_count, 5)),
                60,
            )
            log.info("binance_ws_reconnecting", delay=delay)
            await asyncio.sleep(delay)

        self._status = ConnectionStatus.ERROR
        raise WebSocketError(
            f"Binance WebSocket max reconnects ({self._config.websocket_max_reconnect}) exceeded"
        )

    async def _handle_ws_message(self, message: str) -> None:
        """Parse and dispatch a Binance WebSocket message."""
        try:
            data = json.loads(message)
            stream = data.get("stream", "")
            payload = data.get("data", {})

            if "@miniTicker" in stream:
                await self._handle_mini_ticker(payload)
            elif "@kline" in stream:
                await self._handle_kline(payload)

        except (json.JSONDecodeError, KeyError) as e:
            log.warning("binance_ws_parse_error", error=str(e))

    async def _handle_mini_ticker(self, data: dict) -> None:
        """Process a mini-ticker message into a Tick."""
        if not self._on_tick:
            return

        try:
            tick = Tick(
                timestamp=datetime.fromtimestamp(
                    data["E"] / 1000, tz=timezone.utc
                ),
                symbol=data["s"],
                exchange=Exchange.BINANCE,
                ltp=float(data["c"]),       # Close price = last traded
                volume=float(data["v"]),     # Total volume
                turnover=float(data["q"]),   # Quote volume
            )
            await self._on_tick(tick)
        except (KeyError, ValueError) as e:
            log.warning("binance_ticker_parse_error", error=str(e))

    async def _handle_kline(self, data: dict) -> None:
        """Process a kline message into an OHLCV (only when candle is closed)."""
        if not self._on_candle:
            return

        try:
            k = data["k"]
            is_closed = k["x"]  # True when the candle is final

            if not is_closed:
                return  # Skip interim candle updates

            candle = OHLCV(
                timestamp=datetime.fromtimestamp(
                    k["t"] / 1000, tz=timezone.utc
                ),
                symbol=k["s"],
                exchange=Exchange.BINANCE,
                timeframe=Timeframe.M1,  # We subscribe to 1m klines
                open=float(k["o"]),
                high=float(k["h"]),
                low=float(k["l"]),
                close=float(k["c"]),
                volume=float(k["v"]),
                turnover=float(k["q"]),
                num_trades=int(k["n"]),
            )
            await self._on_candle(candle)
        except (KeyError, ValueError) as e:
            log.warning("binance_kline_parse_error", error=str(e))

    async def unsubscribe_live(self, symbols: list[str] | None = None) -> None:
        """Unsubscribe from live data."""
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        self._subscribed_symbols = []

    # -------------------------------------------------------------------------
    # Instrument Discovery
    # -------------------------------------------------------------------------

    async def get_instruments(self) -> list[Instrument]:
        """Fetch available futures instruments from Binance exchange info.

        Returns:
            List of Instrument objects for USDT-M perpetual futures.
        """
        if not self._session:
            self._session = aiohttp.ClientSession()

        await self._check_rate_limit(weight=1)

        try:
            async with self._session.get(
                f"{self._base_url}/fapi/v1/exchangeInfo",
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status != 200:
                    raise ConnectorDataError(
                        f"Binance exchangeInfo failed: HTTP {resp.status}"
                    )
                data = await resp.json()

        except aiohttp.ClientError as e:
            raise ConnectorDataError(
                f"Failed to fetch Binance exchange info: {e}",
            ) from e

        instruments: list[Instrument] = []

        for sym_info in data.get("symbols", []):
            try:
                # Only USDT-margined perpetuals
                if sym_info.get("contractType") != "PERPETUAL":
                    continue
                if sym_info.get("quoteAsset") != "USDT":
                    continue

                symbol = sym_info["symbol"]

                # Find tick size from price filter
                tick_size = 0.01
                for f in sym_info.get("filters", []):
                    if f["filterType"] == "PRICE_FILTER":
                        tick_size = float(f.get("tickSize", 0.01))
                        break

                # Find lot size from lot size filter
                lot_size = 1.0
                for f in sym_info.get("filters", []):
                    if f["filterType"] == "LOT_SIZE":
                        lot_size = float(f.get("stepSize", 1.0))
                        break

                inst = Instrument(
                    symbol=symbol,
                    exchange=Exchange.BINANCE,
                    asset_class=AssetClass.CRYPTO_FUTURES,
                    name=f"{sym_info.get('baseAsset', '')} / {sym_info.get('quoteAsset', '')} Perpetual",
                    lot_size=lot_size,
                    tick_size=tick_size,
                    exchange_token=symbol,  # Binance uses the symbol itself as the token
                    is_active=sym_info.get("status") == "TRADING",
                    metadata={
                        "contractType": sym_info.get("contractType"),
                        "marginAsset": sym_info.get("marginAsset"),
                        "baseAsset": sym_info.get("baseAsset"),
                        "quoteAsset": sym_info.get("quoteAsset"),
                    },
                )
                instruments.append(inst)

            except (KeyError, ValueError, TypeError):
                continue

        log.info("binance_instruments_loaded", count=len(instruments))
        return instruments
