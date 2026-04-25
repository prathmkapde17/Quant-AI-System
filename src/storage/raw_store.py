"""Quant Trading System — Raw Data Store (Immutable Data Lake).

Stores raw API responses as Parquet files before any normalization or cleaning.
This is the immutable source of truth — if processing logic changes, we can
always reprocess from raw data.

Directory structure:
    data/raw/{exchange}/{date}/{symbol}_{timeframe}.parquet
    data/raw/angel_one/2026-04-24/RELIANCE_1m.parquet
    data/raw/binance/2026-04-24/BTCUSDT_1m.parquet
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.enums import Exchange, Timeframe
from src.core.logging import get_logger

log = get_logger(__name__)

# Default raw data root (relative to project root)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_RAW_DIR = _PROJECT_ROOT / "data" / "raw"


class RawDataStore:
    """Append-only Parquet-based raw data lake.

    Principles:
    - NEVER overwrite existing files (append new partitions only)
    - Store data exactly as received from the exchange
    - Partition by exchange / date / symbol for efficient access
    - Snappy compression for speed + reasonable size

    This enables:
    - Full reprocessing with updated cleaning logic
    - Audit trail of what data was received and when
    - Reproducible dataset builds (raw + git-versioned code = deterministic output)
    """

    def __init__(self, raw_dir: Path | None = None):
        self._raw_dir = raw_dir or _DEFAULT_RAW_DIR
        self._raw_dir.mkdir(parents=True, exist_ok=True)

    @property
    def root_dir(self) -> Path:
        return self._raw_dir

    # -------------------------------------------------------------------------
    # Writing Raw Data
    # -------------------------------------------------------------------------

    def save_historical_candles(
        self,
        raw_data: list[list | dict],
        symbol: str,
        exchange: Exchange,
        timeframe: Timeframe,
        fetch_timestamp: datetime | None = None,
    ) -> Path:
        """Save raw historical candle data from an exchange API response.

        Args:
            raw_data: Raw candle data exactly as received (list of lists or dicts).
            symbol: Instrument symbol.
            exchange: Exchange enum.
            timeframe: Candle timeframe.
            fetch_timestamp: When this data was fetched (defaults to now).

        Returns:
            Path to the saved Parquet file.
        """
        if not raw_data:
            return Path()

        fetch_ts = fetch_timestamp or datetime.now(timezone.utc)
        date_str = fetch_ts.strftime("%Y-%m-%d")

        # Build output path
        out_dir = self._raw_dir / exchange.value / date_str
        out_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{symbol}_{timeframe.value}.parquet"
        out_path = out_dir / filename

        # Convert to DataFrame for Parquet storage
        # For list-of-lists (Angel One, Binance), wrap in a dict
        if raw_data and isinstance(raw_data[0], list):
            df = pd.DataFrame(raw_data)
            # Add column names based on exchange format
            if exchange == Exchange.ANGEL_ONE:
                cols = ["timestamp", "open", "high", "low", "close", "volume"]
                if len(df.columns) >= len(cols):
                    df.columns = cols[:len(df.columns)]
            elif exchange == Exchange.BINANCE:
                cols = [
                    "open_time", "open", "high", "low", "close", "volume",
                    "close_time", "quote_volume", "num_trades",
                    "taker_buy_base", "taker_buy_quote", "ignore",
                ]
                if len(df.columns) >= len(cols):
                    df.columns = cols[:len(df.columns)]
        elif raw_data and isinstance(raw_data[0], dict):
            df = pd.DataFrame(raw_data)
        else:
            # Fallback: ensure every element is a string to avoid Parquet type inference failures
            df = pd.DataFrame({"raw": [str(x) for x in raw_data]})

        # Add metadata columns
        df["_fetch_timestamp"] = fetch_ts.isoformat()
        df["_exchange"] = exchange.value
        df["_symbol"] = symbol
        df["_timeframe"] = timeframe.value

        # Append mode: if file exists, read + concat + rewrite
        # (Parquet doesn't support native append, but files are small per partition)
        if out_path.exists():
            existing = pd.read_parquet(out_path)
            df = pd.concat([existing, df], ignore_index=True)
            # Deduplicate by first column (timestamp) if possible
            if len(df.columns) > 0:
                first_col = df.columns[0]
                df = df.drop_duplicates(subset=[first_col], keep="last")

        df.to_parquet(out_path, compression="snappy", index=False)

        log.info(
            "raw_data_saved",
            path=str(out_path),
            records=len(df),
            exchange=exchange.value,
            symbol=symbol,
        )

        return out_path

    def save_raw_ticks(
        self,
        ticks: list[dict],
        symbol: str,
        exchange: Exchange,
    ) -> Path:
        """Save raw tick data.

        Args:
            ticks: List of tick dicts as received from WebSocket.
            symbol: Instrument symbol.
            exchange: Exchange enum.

        Returns:
            Path to saved file.
        """
        if not ticks:
            return Path()

        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")

        # Partition ticks by hour (they're high volume)
        out_dir = self._raw_dir / exchange.value / date_str / "ticks"
        out_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{symbol}_ticks_{hour_str}.parquet"
        out_path = out_dir / filename

        df = pd.DataFrame(ticks)
        df["_save_timestamp"] = now.isoformat()

        if out_path.exists():
            existing = pd.read_parquet(out_path)
            df = pd.concat([existing, df], ignore_index=True)

        df.to_parquet(out_path, compression="snappy", index=False)
        return out_path

    # -------------------------------------------------------------------------
    # Reading Raw Data
    # -------------------------------------------------------------------------

    def load_historical(
        self,
        symbol: str,
        exchange: Exchange,
        timeframe: Timeframe,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """Load raw historical data from Parquet files.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.
            timeframe: Candle timeframe.
            start_date: Start date string "YYYY-MM-DD" (inclusive).
            end_date: End date string "YYYY-MM-DD" (inclusive).

        Returns:
            Combined DataFrame from all matching partitions.
        """
        exchange_dir = self._raw_dir / exchange.value

        if not exchange_dir.exists():
            return pd.DataFrame()

        target_filename = f"{symbol}_{timeframe.value}.parquet"
        dfs: list[pd.DataFrame] = []

        # Scan date directories
        for date_dir in sorted(exchange_dir.iterdir()):
            if not date_dir.is_dir():
                continue

            date_str = date_dir.name

            # Filter by date range
            if start_date and date_str < start_date:
                continue
            if end_date and date_str > end_date:
                continue

            parquet_path = date_dir / target_filename
            if parquet_path.exists():
                df = pd.read_parquet(parquet_path)
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        combined = pd.concat(dfs, ignore_index=True)
        log.debug(
            "raw_data_loaded",
            symbol=symbol,
            exchange=exchange.value,
            partitions=len(dfs),
            total_records=len(combined),
        )
        return combined

    # -------------------------------------------------------------------------
    # Inventory
    # -------------------------------------------------------------------------

    def get_inventory(self) -> list[dict[str, Any]]:
        """Get an inventory of all raw data files.

        Returns:
            List of dicts with exchange, date, file, size_mb, records.
        """
        inventory: list[dict[str, Any]] = []

        if not self._raw_dir.exists():
            return inventory

        for exchange_dir in sorted(self._raw_dir.iterdir()):
            if not exchange_dir.is_dir():
                continue

            for date_dir in sorted(exchange_dir.iterdir()):
                if not date_dir.is_dir():
                    continue

                for parquet_file in sorted(date_dir.rglob("*.parquet")):
                    size_mb = parquet_file.stat().st_size / (1024 * 1024)
                    try:
                        df = pd.read_parquet(parquet_file)
                        records = len(df)
                    except Exception:
                        records = -1

                    inventory.append({
                        "exchange": exchange_dir.name,
                        "date": date_dir.name,
                        "file": parquet_file.name,
                        "size_mb": round(size_mb, 3),
                        "records": records,
                        "path": str(parquet_file),
                    })

        return inventory

    def get_disk_usage(self) -> dict[str, float]:
        """Get total disk usage by exchange.

        Returns:
            Dict of {exchange: size_mb}.
        """
        usage: dict[str, float] = {}
        total = 0.0

        if not self._raw_dir.exists():
            return {"total_mb": 0.0}

        for exchange_dir in self._raw_dir.iterdir():
            if not exchange_dir.is_dir():
                continue

            exchange_size = sum(
                f.stat().st_size for f in exchange_dir.rglob("*.parquet")
            )
            size_mb = exchange_size / (1024 * 1024)
            usage[exchange_dir.name] = round(size_mb, 2)
            total += size_mb

        usage["total_mb"] = round(total, 2)
        return usage
