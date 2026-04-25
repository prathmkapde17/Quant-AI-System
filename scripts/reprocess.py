"""Reprocess — Rebuild processed data from raw Parquet files.

When cleaning logic changes, run this to reprocess all raw data
through the updated pipeline without re-fetching from exchanges.

Usage:
    python scripts/reprocess.py --exchange binance --timeframe 1m
    python scripts/reprocess.py --exchange angel_one --timeframe 1d --start 2025-01-01
    python scripts/reprocess.py --all  # Reprocess everything
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.core.config import load_settings
from src.core.enums import Exchange, Timeframe
from src.core.logging import setup_logging
from src.ingestion.normalizer import Normalizer
from src.cleaning.validator import OHLCVValidator
from src.cleaning.cleaner import OHLCVCleaner
from src.cleaning.advanced_validator import AdvancedValidator
from src.storage.database import Database
from src.storage.raw_store import RawDataStore
from src.storage.repository import OHLCVRepository

console = Console()


async def reprocess_symbol(
    db: Database,
    raw_store: RawDataStore,
    symbol: str,
    exchange: Exchange,
    timeframe: Timeframe,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, int]:
    """Reprocess a single symbol from raw data.

    Pipeline: Load raw Parquet → Normalize → Validate → Clean → Upsert to DB.

    Returns:
        Dict with stats: raw_records, valid, invalid, stored.
    """
    # 1. Load raw data
    raw_df = raw_store.load_historical(
        symbol=symbol,
        exchange=exchange,
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
    )

    if raw_df.empty:
        return {"raw_records": 0, "valid": 0, "invalid": 0, "stored": 0}

    # 2. Normalize (convert raw rows to OHLCV objects)
    candles = []
    for _, row in raw_df.iterrows():
        try:
            if exchange == Exchange.ANGEL_ONE:
                raw_list = [
                    row.get("timestamp", ""),
                    row.get("open", 0),
                    row.get("high", 0),
                    row.get("low", 0),
                    row.get("close", 0),
                    row.get("volume", 0),
                ]
                candle = Normalizer.angel_one_candle(raw_list, symbol, timeframe)
                candles.append(candle)
            elif exchange == Exchange.BINANCE:
                raw_list = [
                    row.get("open_time", 0),
                    row.get("open", 0),
                    row.get("high", 0),
                    row.get("low", 0),
                    row.get("close", 0),
                    row.get("volume", 0),
                    row.get("close_time", 0),
                    row.get("quote_volume", 0),
                    row.get("num_trades", 0),
                ]
                candle = Normalizer.binance_kline(raw_list, symbol, timeframe)
                candles.append(candle)
        except Exception:
            pass  # Skip unparseable records

    if not candles:
        return {"raw_records": len(raw_df), "valid": 0, "invalid": 0, "stored": 0}

    # 3. Validate
    validator = OHLCVValidator()
    validation = validator.validate(candles)

    # 4. Advanced validation
    adv_validator = AdvancedValidator()
    adv_results = adv_validator.validate_all(candles)

    # 5. Clean
    cleaner = OHLCVCleaner()
    cleaned = cleaner.clean(candles, timeframe)

    # 6. Store (upsert — idempotent)
    repo = OHLCVRepository(db)
    stored = await repo.upsert_many(cleaned)

    return {
        "raw_records": len(raw_df),
        "normalized": len(candles),
        "valid": validation.valid_records,
        "invalid": validation.invalid_records,
        "advanced_flags": adv_results.get("total_flags", 0),
        "cleaned": len(cleaned),
        "stored": stored,
    }


async def main(
    exchange_str: str | None = None,
    timeframe_str: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    reprocess_all: bool = False,
) -> None:
    console.print(Panel.fit(
        "[bold cyan]Quant Trading System — Reprocess from Raw Data[/bold cyan]",
        border_style="cyan",
    ))

    settings = load_settings()
    setup_logging(settings.app.log_level)

    db = Database()
    await db.connect()

    raw_store = RawDataStore()

    try:
        # Get inventory of raw data
        inventory = raw_store.get_inventory()
        if not inventory:
            console.print("[yellow]No raw data found. Run backfill first.[/yellow]")
            return

        # Determine what to reprocess
        if reprocess_all:
            targets = inventory
        else:
            targets = inventory
            if exchange_str:
                targets = [t for t in targets if t["exchange"] == exchange_str]
            if timeframe_str:
                targets = [t for t in targets if timeframe_str in t["file"]]

        # Group by exchange + symbol
        seen: set[str] = set()
        unique_targets: list[dict] = []
        for t in targets:
            # Parse symbol from filename: BTCUSDT_1m.parquet
            filename = t["file"]
            if filename.endswith(".parquet") and "_" in filename:
                parts = filename.replace(".parquet", "").rsplit("_", 1)
                sym, tf = parts[0], parts[1]
                key = f"{t['exchange']}:{sym}:{tf}"
                if key not in seen:
                    seen.add(key)
                    unique_targets.append({
                        "exchange": t["exchange"],
                        "symbol": sym,
                        "timeframe": tf,
                    })

        console.print(f"\n[cyan]Reprocessing {len(unique_targets)} symbol/timeframe combinations[/cyan]\n")

        start_time = time.monotonic()
        results_table = Table(title="Reprocess Results", border_style="cyan")
        results_table.add_column("Symbol", style="cyan")
        results_table.add_column("Exchange")
        results_table.add_column("TF")
        results_table.add_column("Raw", justify="right")
        results_table.add_column("Valid", justify="right")
        results_table.add_column("Flags", justify="right")
        results_table.add_column("Stored", justify="right")

        total_stored = 0

        for target in unique_targets:
            try:
                exchange = Exchange(target["exchange"])
                timeframe = Timeframe(target["timeframe"])
            except ValueError:
                continue

            stats = await reprocess_symbol(
                db=db,
                raw_store=raw_store,
                symbol=target["symbol"],
                exchange=exchange,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
            )

            flag_str = (
                f"[red]{stats['advanced_flags']}[/red]"
                if stats.get("advanced_flags", 0) > 0
                else "[green]0[/green]"
            )

            results_table.add_row(
                target["symbol"],
                target["exchange"],
                target["timeframe"],
                str(stats["raw_records"]),
                str(stats.get("valid", 0)),
                flag_str,
                str(stats["stored"]),
            )
            total_stored += stats["stored"]

        console.print(results_table)
        elapsed = time.monotonic() - start_time
        console.print(f"\n[green]✅ Reprocessed {total_stored:,} records in {elapsed:.1f}s[/green]")

    finally:
        await db.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reprocess data from raw Parquet files")
    parser.add_argument("--exchange", "-e", choices=["angel_one", "binance", "yfinance"])
    parser.add_argument("--timeframe", "-t", choices=["1m", "5m", "15m", "1h", "1d"])
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    parser.add_argument("--all", action="store_true", help="Reprocess everything")
    args = parser.parse_args()
    asyncio.run(main(args.exchange, args.timeframe, args.start, args.end, getattr(args, "all", False)))
