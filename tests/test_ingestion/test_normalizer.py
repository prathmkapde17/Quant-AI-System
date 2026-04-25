"""Tests for data normalizer — exchange-specific format conversion."""

from datetime import datetime, timezone

import pytest

from src.core.enums import Exchange, Timeframe
from src.core.exceptions import NormalizationError
from src.ingestion.normalizer import Normalizer


class TestAngelOneNormalization:
    """Tests for Angel One data normalization."""

    def test_valid_candle(self):
        raw = ["2024-06-10T09:15:00+05:30", 2500.0, 2510.0, 2495.0, 2505.0, 10000]
        candle = Normalizer.angel_one_candle(raw, "RELIANCE", Timeframe.M1)
        assert candle.symbol == "RELIANCE"
        assert candle.exchange == Exchange.ANGEL_ONE
        # Should be converted from IST to UTC (IST = UTC+5:30)
        assert candle.timestamp.tzinfo == timezone.utc
        assert candle.timestamp.hour == 3  # 9:15 IST = 3:45 UTC
        assert candle.timestamp.minute == 45

    def test_invalid_candle_raises(self):
        with pytest.raises(NormalizationError):
            Normalizer.angel_one_candle(["bad_data"], "RELIANCE", Timeframe.M1)

    def test_batch_with_errors(self):
        raw_list = [
            ["2024-06-10T09:15:00+05:30", 2500.0, 2510.0, 2495.0, 2505.0, 10000],
            ["bad"],
            ["2024-06-10T09:16:00+05:30", 2505.0, 2515.0, 2500.0, 2510.0, 8000],
        ]
        candles = Normalizer.angel_one_candles(raw_list, "RELIANCE", Timeframe.M1)
        assert len(candles) == 2  # Skips the bad one


class TestBinanceNormalization:
    """Tests for Binance data normalization."""

    def test_valid_kline(self):
        # Binance kline: [open_time_ms, o, h, l, c, vol, close_time, quote_vol, trades, ...]
        ts_ms = int(datetime(2024, 6, 10, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
        raw = [ts_ms, "67000.0", "67500.0", "66800.0", "67200.0", "150.5",
               ts_ms + 60000, "10000000.0", 5000, "0", "0", "0"]
        candle = Normalizer.binance_kline(raw, "BTCUSDT", Timeframe.M1)
        assert candle.symbol == "BTCUSDT"
        assert candle.exchange == Exchange.BINANCE
        assert candle.close == 67200.0
        assert candle.turnover == 10000000.0
        assert candle.num_trades == 5000

    def test_ws_kline_closed(self):
        data = {
            "k": {
                "t": int(datetime(2024, 6, 10, tzinfo=timezone.utc).timestamp() * 1000),
                "s": "BTCUSDT",
                "x": True,  # Closed
                "o": "67000.0", "h": "67500.0", "l": "66800.0", "c": "67200.0",
                "v": "150.5", "q": "10000000.0", "n": 5000,
            }
        }
        candle = Normalizer.binance_ws_kline(data)
        assert candle is not None
        assert candle.close == 67200.0

    def test_ws_kline_not_closed_returns_none(self):
        data = {
            "k": {
                "t": 1000, "s": "BTCUSDT", "x": False,
                "o": "67000", "h": "67500", "l": "66800", "c": "67200",
                "v": "150", "q": "10000000", "n": 5000,
            }
        }
        assert Normalizer.binance_ws_kline(data) is None

    def test_ws_ticker(self):
        data = {
            "E": int(datetime(2024, 6, 10, tzinfo=timezone.utc).timestamp() * 1000),
            "s": "ETHUSDT",
            "c": "3500.0",
            "v": "50000.0",
            "q": "175000000.0",
        }
        tick = Normalizer.binance_ws_ticker(data)
        assert tick.symbol == "ETHUSDT"
        assert tick.ltp == 3500.0
