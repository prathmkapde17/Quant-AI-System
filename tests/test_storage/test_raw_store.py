"""Tests for Raw Data Store — Parquet-based immutable data lake."""

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from src.core.enums import Exchange, Timeframe
from src.storage.raw_store import RawDataStore


@pytest.fixture
def raw_store(tmp_path):
    """Create a RawDataStore with a temp directory."""
    store = RawDataStore(raw_dir=tmp_path / "raw")
    yield store
    # Cleanup
    if (tmp_path / "raw").exists():
        shutil.rmtree(tmp_path / "raw")


class TestRawDataStore:
    """Tests for RawDataStore."""

    def test_save_and_load_binance(self, raw_store):
        """Save Binance klines and load them back."""
        ts_ms = int(datetime(2024, 6, 10, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

        raw_data = [
            [ts_ms, "67000", "67500", "66800", "67200", "150.5",
             ts_ms + 60000, "10000000", 5000, "0", "0", "0"],
            [ts_ms + 60000, "67200", "67300", "67100", "67250", "120.0",
             ts_ms + 120000, "8000000", 4200, "0", "0", "0"],
        ]

        path = raw_store.save_historical_candles(
            raw_data=raw_data,
            symbol="BTCUSDT",
            exchange=Exchange.BINANCE,
            timeframe=Timeframe.M1,
        )

        assert path.exists()
        assert path.suffix == ".parquet"

        # Load back
        df = raw_store.load_historical(
            symbol="BTCUSDT",
            exchange=Exchange.BINANCE,
            timeframe=Timeframe.M1,
        )
        assert len(df) == 2
        assert "_exchange" in df.columns
        assert "_symbol" in df.columns

    def test_save_and_load_angel_one(self, raw_store):
        """Save Angel One candles and load them back."""
        raw_data = [
            ["2024-06-10T09:15:00+05:30", 2500.0, 2510.0, 2495.0, 2505.0, 10000],
            ["2024-06-10T09:16:00+05:30", 2505.0, 2515.0, 2500.0, 2510.0, 8000],
        ]

        raw_store.save_historical_candles(
            raw_data=raw_data,
            symbol="RELIANCE",
            exchange=Exchange.ANGEL_ONE,
            timeframe=Timeframe.M1,
        )

        df = raw_store.load_historical(
            symbol="RELIANCE",
            exchange=Exchange.ANGEL_ONE,
            timeframe=Timeframe.M1,
        )
        assert len(df) == 2

    def test_append_deduplicates(self, raw_store):
        """Saving twice with same data should deduplicate."""
        raw_data = [
            ["2024-06-10T09:15:00+05:30", 2500.0, 2510.0, 2495.0, 2505.0, 10000],
        ]

        raw_store.save_historical_candles(
            raw_data=raw_data,
            symbol="TCS",
            exchange=Exchange.ANGEL_ONE,
            timeframe=Timeframe.M1,
        )
        raw_store.save_historical_candles(
            raw_data=raw_data,
            symbol="TCS",
            exchange=Exchange.ANGEL_ONE,
            timeframe=Timeframe.M1,
        )

        df = raw_store.load_historical("TCS", Exchange.ANGEL_ONE, Timeframe.M1)
        assert len(df) == 1  # Deduplicated

    def test_inventory(self, raw_store):
        """Inventory should list saved files."""
        raw_store.save_historical_candles(
            raw_data=[["2024-06-10T09:15:00+05:30", 100, 110, 90, 105, 5000]],
            symbol="INFY",
            exchange=Exchange.ANGEL_ONE,
            timeframe=Timeframe.D1,
        )

        inventory = raw_store.get_inventory()
        assert len(inventory) >= 1
        assert inventory[0]["exchange"] == "angel_one"
        assert inventory[0]["records"] == 1

    def test_disk_usage(self, raw_store):
        """Disk usage should report size by exchange."""
        raw_store.save_historical_candles(
            raw_data=[["2024-06-10T09:15:00+05:30", 100, 110, 90, 105, 5000]],
            symbol="INFY",
            exchange=Exchange.ANGEL_ONE,
            timeframe=Timeframe.D1,
        )

        usage = raw_store.get_disk_usage()
        assert "total_mb" in usage
        assert usage["total_mb"] > 0

    def test_empty_data_returns_empty_path(self, raw_store):
        """Empty data should return empty path."""
        path = raw_store.save_historical_candles(
            raw_data=[],
            symbol="EMPTY",
            exchange=Exchange.BINANCE,
            timeframe=Timeframe.D1,
        )
        assert path == Path()

    def test_load_nonexistent_returns_empty(self, raw_store):
        """Loading nonexistent data should return empty DataFrame."""
        df = raw_store.load_historical("FAKE", Exchange.BINANCE, Timeframe.D1)
        assert df.empty
