"""Run Pipeline — Main entry point for the quant data pipeline.

Orchestrates: DB connect → instrument sync → historical gap-fill →
live WebSocket streaming → scheduler → health monitor.

Usage:
    python scripts/run_pipeline.py
    python scripts/run_pipeline.py --no-backfill   # Skip historical gap-fill
    python scripts/run_pipeline.py --no-live       # Skip live streaming
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from rich.console import Console
from rich.panel import Panel

from src.core.config import load_settings
from src.core.enums import Exchange, Timeframe
from src.core.logging import setup_logging, get_logger
from src.connectors.angel_one import AngelOneConnector
from src.connectors.binance_futures import BinanceFuturesConnector
from src.ingestion.historical import HistoricalIngester
from src.ingestion.live import LiveIngester
from src.ingestion.scheduler import PipelineScheduler
from src.storage.database import Database
from src.storage.metadata import InstrumentMetadataManager
from src.streaming.publisher import StreamPublisher
from src.api.health import HealthChecker

console = Console()
log = get_logger(__name__)

# Graceful shutdown flag
_shutdown_event = asyncio.Event()


def _handle_signal(sig, frame):
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    console.print(f"\n[yellow]Received {signal.Signals(sig).name} — shutting down...[/yellow]")
    _shutdown_event.set()


async def main(skip_backfill: bool = False, skip_live: bool = False) -> None:
    console.print(Panel.fit(
        "[bold cyan]Quant Trading System — Data Pipeline[/bold cyan]\n"
        "Press Ctrl+C to stop",
        border_style="cyan",
    ))

    settings = load_settings()
    setup_logging(settings.app.log_level)

    # Register signal handlers
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # --- Step 1: Connect Database ---
    console.print("\n[bold]1. Connecting to TimescaleDB...[/bold]")
    db = Database()
    await db.connect()
    health = await db.health_check()
    console.print(f"[green]✅ DB connected — {health.get('latency_ms', '?')}ms latency[/green]")

    # --- Step 2: Connect Redis ---
    console.print("\n[bold]2. Connecting to Redis...[/bold]")
    publisher = StreamPublisher()
    await publisher.connect()
    console.print("[green]✅ Redis connected[/green]")

    # --- Step 3: Sync instruments ---
    console.print("\n[bold]3. Syncing instrument metadata...[/bold]")
    metadata = InstrumentMetadataManager(db)
    count = await metadata.sync_to_db()
    console.print(f"[green]✅ {count} instruments synced[/green]")

    # --- Step 4: Create connectors ---
    connectors = {}

    console.print("\n[bold]4. Connecting to exchanges...[/bold]")

    # Binance (always try — free, no auth needed for public data)
    try:
        bn = BinanceFuturesConnector()
        await bn.connect()
        connectors[Exchange.BINANCE] = bn
        console.print("[green]✅ Binance Futures connected[/green]")
    except Exception as e:
        console.print(f"[yellow]⚠️ Binance: {e}[/yellow]")

    # Angel One (only if configured)
    if settings.angel_one.is_configured:
        try:
            ao = AngelOneConnector()
            await ao.connect()
            connectors[Exchange.ANGEL_ONE] = ao
            console.print("[green]✅ Angel One connected[/green]")

            # Load instrument tokens
            ao_instruments = await ao.get_instruments()
            console.print(f"   Loaded {len(ao_instruments)} instrument tokens")
        except Exception as e:
            console.print(f"[yellow]⚠️ Angel One: {e}[/yellow]")
    else:
        console.print("[dim]⏭ Angel One not configured — skipping[/dim]")

    # --- Step 5: Historical gap-fill ---
    if not skip_backfill and connectors:
        console.print("\n[bold]5. Checking for historical data gaps...[/bold]")
        ingester = HistoricalIngester(db=db, connectors=connectors)

        for exchange, connector in connectors.items():
            symbols = metadata.get_symbols(exchange)
            if symbols:
                console.print(f"   Backfilling {exchange.value}: {len(symbols)} symbols...")
                try:
                    results = await ingester.backfill(
                        exchange=exchange,
                        symbols=symbols,
                        timeframe=Timeframe.M1,
                        lookback_days=7,  # Quick gap-fill (last 7 days)
                    )
                    total = sum(results.values())
                    console.print(f"[green]   ✅ {total:,} records filled[/green]")
                except Exception as e:
                    console.print(f"[yellow]   ⚠️ Backfill error: {e}[/yellow]")
    else:
        console.print("\n[dim]5. Skipping historical gap-fill[/dim]")

    # --- Step 6: Start live streaming ---
    live_ingester = None
    if not skip_live and connectors:
        console.print("\n[bold]6. Starting live data streams...[/bold]")
        live_ingester = LiveIngester(db=db, connectors=connectors, publisher=publisher)

        symbols_per_exchange = {}
        for exchange in connectors:
            syms = metadata.get_symbols(exchange)
            if syms:
                symbols_per_exchange[exchange] = syms

        await live_ingester.start(symbols_per_exchange)
        console.print(f"[green]✅ Live streaming active — {sum(len(s) for s in symbols_per_exchange.values())} symbols[/green]")
    else:
        console.print("\n[dim]6. Skipping live streaming[/dim]")

    # --- Step 7: Start scheduler ---
    console.print("\n[bold]7. Starting scheduler...[/bold]")
    scheduler = PipelineScheduler()

    if connectors:
        async def daily_backfill_job():
            ing = HistoricalIngester(db=db, connectors=connectors)
            for ex, conn in connectors.items():
                syms = metadata.get_symbols(ex)
                if syms:
                    await ing.backfill(exchange=ex, symbols=syms, timeframe=Timeframe.M1, lookback_days=2)

        scheduler.register_daily_backfill(daily_backfill_job)

    import redis.asyncio as redis_lib
    redis_client = redis_lib.Redis(host=settings.redis.host, port=settings.redis.port, decode_responses=True)

    health_checker = HealthChecker(
        db=db,
        redis_client=redis_client,
        connectors=connectors,
        last_tick_times=live_ingester.last_tick_times if live_ingester else {},
    )

    async def health_check_job():
        summary = await health_checker.get_summary()
        log.info("pipeline_health", **summary)

    scheduler.register_health_check(health_check_job)
    scheduler.start()
    console.print("[green]✅ Scheduler running[/green]")

    # --- Step 8: Wait for shutdown ---
    console.print(Panel.fit(
        "[bold green]🟢 Pipeline is running![/bold green]\n"
        "All systems operational — press Ctrl+C to stop",
        border_style="green",
    ))

    await _shutdown_event.wait()

    # --- Graceful shutdown ---
    console.print("\n[bold yellow]Shutting down...[/bold yellow]")
    scheduler.stop()

    if live_ingester:
        await live_ingester.stop()

    for conn in connectors.values():
        try:
            await conn.disconnect()
        except Exception:
            pass

    await publisher.disconnect()
    await redis_client.aclose()
    await db.disconnect()

    console.print("[green]✅ Pipeline stopped cleanly[/green]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the quant data pipeline")
    parser.add_argument("--no-backfill", action="store_true", help="Skip historical gap-fill")
    parser.add_argument("--no-live", action="store_true", help="Skip live streaming")
    args = parser.parse_args()
    asyncio.run(main(skip_backfill=args.no_backfill, skip_live=args.no_live))
