"""Backfill Historical Data — Fetch and store historical candles.

Usage:
    python scripts/backfill.py --exchange binance --timeframe 1d --days 365
    python scripts/backfill.py --exchange angel_one --timeframe 1m --days 30
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
from src.connectors.angel_one import AngelOneConnector
from src.connectors.binance_futures import BinanceFuturesConnector
from src.connectors.yfinance_connector import YFinanceConnector
from src.ingestion.historical import HistoricalIngester
from src.storage.database import Database
from src.storage.metadata import InstrumentMetadataManager

console = Console()


def get_connector(exchange: Exchange):
    if exchange == Exchange.ANGEL_ONE:
        return AngelOneConnector()
    elif exchange == Exchange.BINANCE:
        return BinanceFuturesConnector()
    elif exchange == Exchange.YFINANCE:
        return YFinanceConnector()
    raise ValueError(f"Unknown exchange: {exchange}")


async def main(exchange_str: str, timeframe_str: str, days: int, symbols: list[str] | None) -> None:
    console.print(Panel.fit("[bold cyan]Quant Trading System — Historical Backfill[/bold cyan]"))

    settings = load_settings()
    setup_logging(settings.app.log_level)

    exchange = Exchange(exchange_str)
    timeframe = Timeframe(timeframe_str)
    console.print(f"Exchange: {exchange.value} | Timeframe: {timeframe.value} | Lookback: {days}d")

    db = Database()
    await db.connect()
    console.print("[green]✅ Database connected[/green]")

    try:
        connector = get_connector(exchange)
        await connector.connect()
        console.print(f"[green]✅ {exchange.value} connected[/green]")

        if not symbols:
            metadata = InstrumentMetadataManager(db)
            await metadata.refresh_cache()
            instruments = metadata.get_all(exchange)
            if not instruments:
                instruments = [i for i in InstrumentMetadataManager.load_from_yaml() if i.exchange == exchange]
            symbols = [i.symbol for i in instruments]

        if not symbols:
            console.print("[red]❌ No symbols found[/red]")
            return

        console.print(f"Symbols: {', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}")

        if exchange == Exchange.ANGEL_ONE:
            console.print("Loading Angel One master contracts...")
            try:
                await connector.get_instruments()
            except Exception as e:
                console.print(f"[yellow]⚠️ {e}[/yellow]")

        start_time = time.monotonic()
        ingester = HistoricalIngester(db=db, connectors={exchange: connector})
        results = await ingester.backfill(exchange=exchange, symbols=symbols, timeframe=timeframe, lookback_days=days)
        elapsed = time.monotonic() - start_time

        table = Table(title="Backfill Results", border_style="cyan")
        table.add_column("Symbol", style="cyan")
        table.add_column("Records", justify="right")
        table.add_column("Status", justify="center")

        total = 0
        for sym, count in results.items():
            status = "[green]✅" if count > 0 else "[yellow]⚡ Up to date"
            table.add_row(sym, str(count), status)
            total += count

        console.print(table)
        console.print(f"\n[green]✅ Done! {total:,} records in {elapsed:.1f}s[/green]")

    finally:
        try:
            await connector.disconnect()
        except Exception:
            pass
        await db.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill historical market data")
    parser.add_argument("--exchange", "-e", required=True, choices=["angel_one", "binance", "yfinance"])
    parser.add_argument("--timeframe", "-t", required=True, choices=["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"])
    parser.add_argument("--days", "-d", type=int, default=730)
    parser.add_argument("--symbols", "-s", nargs="*")
    args = parser.parse_args()
    asyncio.run(main(args.exchange, args.timeframe, args.days, args.symbols))
