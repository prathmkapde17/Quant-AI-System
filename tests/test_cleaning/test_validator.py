"""Tests for OHLCV Validator — data integrity and anomaly detection."""

from datetime import datetime, timedelta, timezone

import pytest

from src.core.enums import AssetClass, DataQuality, Exchange, Timeframe
from src.core.models import OHLCV
from src.cleaning.validator import OHLCVValidator


def _make_candle(ts_offset_min: int = 0, **overrides) -> OHLCV:
    """Helper to create candles with offset timestamps."""
    base = datetime(2024, 6, 10, 9, 15, tzinfo=timezone.utc)
    defaults = {
        "timestamp": base + timedelta(minutes=ts_offset_min),
        "symbol": "RELIANCE",
        "exchange": Exchange.ANGEL_ONE,
        "timeframe": Timeframe.M1,
        "open": 2500.0,
        "high": 2510.0,
        "low": 2495.0,
        "close": 2505.0,
        "volume": 10000.0,
    }
    defaults.update(overrides)
    return OHLCV(**defaults)


class TestOHLCVValidator:
    """Tests for the OHLCVValidator."""

    def setup_method(self):
        self.validator = OHLCVValidator()

    def test_valid_data_passes(self):
        """Clean sequential data should pass validation."""
        candles = [_make_candle(i) for i in range(10)]
        result = self.validator.validate(candles)
        assert result.valid_records == 10
        assert result.invalid_records == 0
        assert result.is_acceptable

    def test_detects_high_below_open(self):
        """Should flag when high < open (logically impossible)."""
        candles = [
            _make_candle(0),
            _make_candle(1, open=2510.0, high=2505.0, low=2490.0, close=2500.0),
        ]
        result = self.validator.validate(candles)
        assert result.invalid_records >= 1

    def test_detects_duplicate_timestamps(self):
        """Should flag duplicate timestamps."""
        candles = [_make_candle(0), _make_candle(0)]
        result = self.validator.validate(candles)
        assert result.invalid_records >= 1

    def test_detects_non_monotonic_timestamps(self):
        """Should flag timestamps that go backward."""
        candles = [_make_candle(1), _make_candle(0)]
        result = self.validator.validate(candles)
        assert result.invalid_records >= 1

    def test_detects_large_equity_move(self):
        """Should flag >10% single-candle move for equities."""
        candles = [
            _make_candle(0, close=1000.0),
            _make_candle(1, open=1150.0, high=1160.0, low=1140.0, close=1150.0),
        ]
        result = self.validator.validate(candles, asset_class=AssetClass.EQUITY)
        violations = [v for v in result.violations if any("large move" in issue for issue in v["issues"])]
        assert len(violations) >= 1

    def test_crypto_threshold_is_higher(self):
        """15% move should pass for crypto but fail for equity."""
        candles = [
            _make_candle(0, close=1000.0),
            _make_candle(1, open=1150.0, high=1160.0, low=1140.0, close=1150.0),
        ]
        crypto_result = self.validator.validate(candles, asset_class=AssetClass.CRYPTO_FUTURES)
        equity_result = self.validator.validate(candles, asset_class=AssetClass.EQUITY)
        # 15% is below 20% crypto threshold but above 10% equity threshold
        crypto_violations = [v for v in crypto_result.violations if any("large move" in i for i in v["issues"])]
        equity_violations = [v for v in equity_result.violations if any("large move" in i for i in v["issues"])]
        assert len(crypto_violations) < len(equity_violations)

    def test_empty_list(self):
        """Empty input should return zero counts."""
        result = self.validator.validate([])
        assert result.total_records == 0
        assert result.pass_rate == 0.0

    def test_find_gaps(self):
        """Should detect a 5-minute gap in 1-minute data."""
        candles = [_make_candle(0), _make_candle(1), _make_candle(7)]  # Gap: 2-6
        gaps = self.validator.find_gaps(candles, Timeframe.M1, market_hours_only=False)
        assert len(gaps) >= 1
        assert gaps[0]["missing_candles"] >= 4
