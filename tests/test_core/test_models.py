"""Tests for core data models."""

from datetime import datetime, timezone

import pytest

from src.core.enums import AssetClass, DataQuality, Exchange, Timeframe
from src.core.models import OHLCV, Instrument, Tick, ValidationResult


class TestOHLCV:
    """Tests for the OHLCV model."""

    def _make_ohlcv(self, **overrides) -> OHLCV:
        """Helper to create an OHLCV with defaults."""
        defaults = {
            "timestamp": datetime(2026, 4, 23, 10, 0, 0, tzinfo=timezone.utc),
            "symbol": "RELIANCE",
            "exchange": Exchange.ANGEL_ONE,
            "timeframe": Timeframe.M1,
            "open": 2500.0,
            "high": 2520.0,
            "low": 2490.0,
            "close": 2510.0,
            "volume": 10000.0,
        }
        defaults.update(overrides)
        return OHLCV(**defaults)

    def test_create_valid_ohlcv(self):
        """Valid OHLCV should create without errors."""
        ohlcv = self._make_ohlcv()
        assert ohlcv.symbol == "RELIANCE"
        assert ohlcv.exchange == Exchange.ANGEL_ONE
        assert ohlcv.timeframe == Timeframe.M1
        assert ohlcv.quality == DataQuality.RAW

    def test_computed_properties(self):
        """Mid, typical price, and bar range should compute correctly."""
        ohlcv = self._make_ohlcv(high=100.0, low=90.0, close=95.0)
        assert ohlcv.mid == 95.0
        assert ohlcv.typical_price == pytest.approx(95.0)
        assert ohlcv.bar_range == 10.0

    def test_high_less_than_low_raises(self):
        """High < low should raise a validation error."""
        with pytest.raises(ValueError, match="high.*must be >= low"):
            self._make_ohlcv(high=90.0, low=100.0)

    def test_negative_volume_raises(self):
        """Negative volume should raise a validation error."""
        with pytest.raises(ValueError):
            self._make_ohlcv(volume=-1.0)

    def test_to_db_tuple(self):
        """to_db_tuple should return correct column order."""
        ohlcv = self._make_ohlcv()
        t = ohlcv.to_db_tuple()
        assert len(t) == 12
        assert t[0] == ohlcv.timestamp
        assert t[1] == "RELIANCE"
        assert t[2] == "angel_one"
        assert t[3] == "1m"

    def test_frozen_model(self):
        """OHLCV should be immutable (frozen)."""
        ohlcv = self._make_ohlcv()
        with pytest.raises(Exception):  # ValidationError from Pydantic
            ohlcv.close = 9999.0


class TestTick:
    """Tests for the Tick model."""

    def test_create_valid_tick(self):
        """Valid tick should create without errors."""
        tick = Tick(
            timestamp=datetime(2026, 4, 23, 10, 0, 0, tzinfo=timezone.utc),
            symbol="BTCUSDT",
            exchange=Exchange.BINANCE,
            ltp=67000.0,
            volume=1.5,
            bid=66999.0,
            ask=67001.0,
        )
        assert tick.ltp == 67000.0
        assert tick.spread == 2.0

    def test_spread_none_when_missing(self):
        """Spread should be None if bid or ask is missing."""
        tick = Tick(
            timestamp=datetime(2026, 4, 23, 10, 0, 0, tzinfo=timezone.utc),
            symbol="BTCUSDT",
            exchange=Exchange.BINANCE,
            ltp=67000.0,
        )
        assert tick.spread is None


class TestInstrument:
    """Tests for the Instrument model."""

    def test_unique_key(self):
        """Unique key should be exchange:symbol."""
        inst = Instrument(
            symbol="RELIANCE",
            exchange=Exchange.ANGEL_ONE,
            asset_class=AssetClass.EQUITY,
            name="Reliance Industries",
        )
        assert inst.unique_key == "angel_one:RELIANCE"


class TestTimeframe:
    """Tests for Timeframe enum."""

    def test_minutes(self):
        """Minutes property should return correct duration."""
        assert Timeframe.M1.minutes == 1
        assert Timeframe.H1.minutes == 60
        assert Timeframe.D1.minutes == 1440

    def test_is_intraday(self):
        """is_intraday should distinguish intraday from daily/weekly."""
        assert Timeframe.M1.is_intraday is True
        assert Timeframe.H4.is_intraday is True
        assert Timeframe.D1.is_intraday is False
        assert Timeframe.W1.is_intraday is False


class TestValidationResult:
    """Tests for ValidationResult model."""

    def test_pass_rate(self):
        """Pass rate should compute correctly."""
        result = ValidationResult(total_records=100, valid_records=97, invalid_records=3)
        assert result.pass_rate == 97.0
        assert result.is_acceptable is True

    def test_unacceptable(self):
        """Below 95% pass rate should be unacceptable."""
        result = ValidationResult(total_records=100, valid_records=90, invalid_records=10)
        assert result.is_acceptable is False
