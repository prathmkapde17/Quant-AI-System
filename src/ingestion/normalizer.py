"""Quant Trading System — Data Normalizer.

Converts exchange-specific data formats into our unified OHLCV/Tick models.
All timestamps are normalized to UTC.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from src.core.enums import Exchange, Timeframe
from src.core.exceptions import NormalizationError
from src.core.logging import get_logger
from src.core.models import OHLCV, Tick

log = get_logger(__name__)

# IST timezone offset
_IST = timezone(timedelta(hours=5, minutes=30))


class Normalizer:
    """Converts raw exchange data into normalized OHLCV/Tick objects.

    Each exchange has its own data format. The normalizer provides a
    unified interface so the rest of the pipeline doesn't care about
    exchange-specific quirks.
    """

    # -------------------------------------------------------------------------
    # Angel One
    # -------------------------------------------------------------------------

    @staticmethod
    def angel_one_candle(
        raw: list,
        symbol: str,
        timeframe: Timeframe,
    ) -> OHLCV:
        """Normalize an Angel One historical candle.

        Angel One format: [timestamp_str, open, high, low, close, volume]
        Timestamps are in IST with format "2024-01-15T09:15:00+05:30".

        Args:
            raw: Raw candle list from Angel One API.
            symbol: Instrument symbol.
            timeframe: Candle timeframe.

        Returns:
            Normalized OHLCV object with UTC timestamp.
        """
        try:
            # Parse IST timestamp → UTC
            ts_ist = datetime.strptime(raw[0], "%Y-%m-%dT%H:%M:%S%z")
            ts_utc = ts_ist.astimezone(timezone.utc)

            return OHLCV(
                timestamp=ts_utc,
                symbol=symbol,
                exchange=Exchange.ANGEL_ONE,
                timeframe=timeframe,
                open=float(raw[1]),
                high=float(raw[2]),
                low=float(raw[3]),
                close=float(raw[4]),
                volume=float(raw[5]),
            )
        except (IndexError, ValueError, TypeError) as e:
            raise NormalizationError(
                f"Failed to normalize Angel One candle for {symbol}: {e}",
                details={"raw": str(raw), "error": str(e)},
            ) from e

    @staticmethod
    def angel_one_candles(
        raw_list: list[list],
        symbol: str,
        timeframe: Timeframe,
    ) -> list[OHLCV]:
        """Normalize a batch of Angel One candles.

        Skips individual candles that fail to parse (logs warning).

        Args:
            raw_list: List of raw candle arrays.
            symbol: Instrument symbol.
            timeframe: Candle timeframe.

        Returns:
            List of normalized OHLCV objects.
        """
        candles: list[OHLCV] = []
        errors = 0

        for raw in raw_list:
            try:
                candles.append(
                    Normalizer.angel_one_candle(raw, symbol, timeframe)
                )
            except NormalizationError:
                errors += 1

        if errors > 0:
            log.warning(
                "angel_one_normalization_errors",
                symbol=symbol,
                total=len(raw_list),
                errors=errors,
            )

        return candles

    @staticmethod
    def angel_one_tick(raw: dict, symbol: str) -> Tick:
        """Normalize an Angel One WebSocket tick message.

        Args:
            raw: Parsed tick data dict from WebSocket.
            symbol: Instrument symbol.

        Returns:
            Normalized Tick object.
        """
        try:
            # Angel One WebSocket sends epoch ms for exchange timestamp
            ts = datetime.fromtimestamp(
                raw.get("exchange_timestamp", 0) / 1000,
                tz=timezone.utc,
            )

            return Tick(
                timestamp=ts,
                symbol=symbol,
                exchange=Exchange.ANGEL_ONE,
                ltp=float(raw.get("ltp", 0)),
                volume=float(raw.get("volume", 0)),
                bid=float(raw["best_bid_price"]) if "best_bid_price" in raw else None,
                ask=float(raw["best_ask_price"]) if "best_ask_price" in raw else None,
                oi=float(raw["open_interest"]) if "open_interest" in raw else None,
            )
        except (KeyError, ValueError, TypeError) as e:
            raise NormalizationError(
                f"Failed to normalize Angel One tick for {symbol}: {e}",
                details={"raw": str(raw)},
            ) from e

    # -------------------------------------------------------------------------
    # Binance Futures
    # -------------------------------------------------------------------------

    @staticmethod
    def binance_kline(
        raw: list,
        symbol: str,
        timeframe: Timeframe,
    ) -> OHLCV:
        """Normalize a Binance Futures kline.

        Binance format: [open_time_ms, open, high, low, close, volume,
                          close_time_ms, quote_vol, num_trades, ...]

        Args:
            raw: Raw kline list from Binance API.
            symbol: Trading pair symbol.
            timeframe: Candle timeframe.

        Returns:
            Normalized OHLCV object with UTC timestamp.
        """
        try:
            ts = datetime.fromtimestamp(raw[0] / 1000, tz=timezone.utc)

            return OHLCV(
                timestamp=ts,
                symbol=symbol.upper(),
                exchange=Exchange.BINANCE,
                timeframe=timeframe,
                open=float(raw[1]),
                high=float(raw[2]),
                low=float(raw[3]),
                close=float(raw[4]),
                volume=float(raw[5]),
                turnover=float(raw[7]),     # Quote asset volume
                num_trades=int(raw[8]),
            )
        except (IndexError, ValueError, TypeError) as e:
            raise NormalizationError(
                f"Failed to normalize Binance kline for {symbol}: {e}",
                details={"raw": str(raw), "error": str(e)},
            ) from e

    @staticmethod
    def binance_klines(
        raw_list: list[list],
        symbol: str,
        timeframe: Timeframe,
    ) -> list[OHLCV]:
        """Normalize a batch of Binance klines.

        Args:
            raw_list: List of raw kline arrays.
            symbol: Trading pair symbol.
            timeframe: Candle timeframe.

        Returns:
            List of normalized OHLCV objects.
        """
        candles: list[OHLCV] = []
        errors = 0

        for raw in raw_list:
            try:
                candles.append(
                    Normalizer.binance_kline(raw, symbol, timeframe)
                )
            except NormalizationError:
                errors += 1

        if errors > 0:
            log.warning(
                "binance_normalization_errors",
                symbol=symbol,
                total=len(raw_list),
                errors=errors,
            )

        return candles

    @staticmethod
    def binance_ws_ticker(raw: dict) -> Tick:
        """Normalize a Binance Futures WebSocket mini-ticker.

        Args:
            raw: Mini-ticker data from WebSocket.

        Returns:
            Normalized Tick object.
        """
        try:
            return Tick(
                timestamp=datetime.fromtimestamp(
                    raw["E"] / 1000, tz=timezone.utc
                ),
                symbol=raw["s"],
                exchange=Exchange.BINANCE,
                ltp=float(raw["c"]),
                volume=float(raw["v"]),
                turnover=float(raw.get("q", 0)),
            )
        except (KeyError, ValueError, TypeError) as e:
            raise NormalizationError(
                f"Failed to normalize Binance ticker: {e}",
                details={"raw": str(raw)},
            ) from e

    @staticmethod
    def binance_ws_kline(raw: dict) -> OHLCV | None:
        """Normalize a Binance Futures WebSocket kline.

        Only returns a value when the kline is closed (final).

        Args:
            raw: Kline data from WebSocket.

        Returns:
            Normalized OHLCV if candle is closed, else None.
        """
        k = raw.get("k", {})
        if not k.get("x", False):  # Not closed yet
            return None

        try:
            return OHLCV(
                timestamp=datetime.fromtimestamp(
                    k["t"] / 1000, tz=timezone.utc
                ),
                symbol=k["s"],
                exchange=Exchange.BINANCE,
                timeframe=Timeframe.M1,
                open=float(k["o"]),
                high=float(k["h"]),
                low=float(k["l"]),
                close=float(k["c"]),
                volume=float(k["v"]),
                turnover=float(k["q"]),
                num_trades=int(k["n"]),
            )
        except (KeyError, ValueError, TypeError) as e:
            raise NormalizationError(
                f"Failed to normalize Binance WS kline: {e}",
            ) from e

    # -------------------------------------------------------------------------
    # yfinance
    # -------------------------------------------------------------------------

    @staticmethod
    def yfinance_row(
        idx: Any,
        row: Any,
        symbol: str,
        timeframe: Timeframe,
    ) -> OHLCV:
        """Normalize a yfinance DataFrame row into an OHLCV.

        Args:
            idx: Pandas Timestamp index.
            row: DataFrame row.
            symbol: Ticker symbol.
            timeframe: Candle timeframe.

        Returns:
            Normalized OHLCV object.
        """
        try:
            ts = idx.to_pydatetime()
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            else:
                ts = ts.astimezone(timezone.utc)

            return OHLCV(
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
        except (KeyError, ValueError, TypeError) as e:
            raise NormalizationError(
                f"Failed to normalize yfinance row for {symbol}: {e}",
            ) from e
