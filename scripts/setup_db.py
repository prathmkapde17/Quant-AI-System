"""Setup Database — Initialize TimescaleDB schema and seed instruments.

Usage:
    python scripts/setup_db.py
    python scripts/setup_db.py --reset   # Drop and recreate all tables
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from rich.console import Console
from rich.panel import Panel

from src.core.config import load_settings
from src.core.logging import setup_logging
from src.storage.database import Database
from src.storage.metadata import InstrumentMetadataManager

console = Console()


async def check_docker() -> bool:
    """Verify Docker containers are running."""
    import subprocess

    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True,
            text=True,
            cwd=str(project_root),
            timeout=10,
        )
        if result.returncode != 0:
            console.print("[red]❌ Docker Compose is not running.[/red]")
            console.print("[yellow]Run: docker compose up -d[/yellow]")
            return False

        console.print("[green]✅ Docker containers detected[/green]")
        return True
    except FileNotFoundError:
        console.print("[red]❌ Docker not found. Install Docker Desktop.[/red]")
        return False
    except subprocess.TimeoutExpired:
        console.print("[yellow]⚠️ Docker check timed out — proceeding anyway[/yellow]")
        return True


async def run_migrations(db: Database, reset: bool = False) -> None:
    """Run SQL migrations."""
    if reset:
        console.print("[yellow]⚠️  Resetting database — dropping all tables...[/yellow]")
        # Drop views first (dependencies)
        await db.execute("DROP MATERIALIZED VIEW IF EXISTS ohlcv_4h CASCADE")
        await db.execute("DROP MATERIALIZED VIEW IF EXISTS ohlcv_1h CASCADE")
        await db.execute("DROP MATERIALIZED VIEW IF EXISTS ohlcv_15m CASCADE")
        await db.execute("DROP MATERIALIZED VIEW IF EXISTS ohlcv_5m CASCADE")
        await db.execute("DROP MATERIALIZED VIEW IF EXISTS ohlcv_daily CASCADE")
        
        # Drop tables
        await db.execute("DROP TABLE IF EXISTS pipeline_latency CASCADE")
        await db.execute("DROP TABLE IF EXISTS features CASCADE")
        await db.execute("DROP TABLE IF EXISTS ohlcv CASCADE")
        await db.execute("DROP TABLE IF EXISTS ticks CASCADE")
        await db.execute("DROP TABLE IF EXISTS instruments CASCADE")
        await db.execute("DROP TABLE IF EXISTS data_quality_log CASCADE")
        await db.execute("DROP TABLE IF EXISTS ingestion_progress CASCADE")
        console.print("[green]✅ All tables dropped[/green]")

    console.print("[cyan]Running migrations...[/cyan]")
    await db.run_migrations()
    console.print("[green]✅ Migrations complete[/green]")


async def seed_instruments(db: Database) -> None:
    """Load instruments from config and sync to database."""
    console.print("[cyan]Seeding instruments from config/instruments.yaml...[/cyan]")
    manager = InstrumentMetadataManager(db)
    count = await manager.sync_to_db()
    console.print(f"[green]✅ {count} instruments synced to database[/green]")


async def verify_tables(db: Database) -> None:
    """Verify all expected tables exist."""
    tables = await db.fetch("""
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename
    """)
    table_names = [row["tablename"] for row in tables]

    expected = [
        "data_quality_log", "ingestion_progress", "instruments", 
        "ohlcv", "ticks", "pipeline_latency", "features"
    ]
    missing = [t for t in expected if t not in table_names]

    if missing:
        console.print(f"[red]❌ Missing tables: {missing}[/red]")
    else:
        console.print(f"[green]✅ All {len(expected)} tables verified[/green]")

    # Verify Views
    views = await db.fetch("""
        SELECT matviewname FROM pg_matviews
        WHERE schemaname = 'public'
    """)
    view_names = [row["matviewname"] for row in views]
    expected_views = ["ohlcv_5m", "ohlcv_15m", "ohlcv_1h", "ohlcv_4h", "ohlcv_daily"]
    missing_views = [v for v in expected_views if v not in view_names]
    
    if missing_views:
        console.print(f"[red]❌ Missing views: {missing_views}[/red]")
    else:
        console.print(f"[green]✅ All {len(expected_views)} continuous aggregates verified[/green]")

    # Check hypertables
    hypertables = await db.fetch("""
        SELECT hypertable_name FROM timescaledb_information.hypertables
        ORDER BY hypertable_name
    """)
    ht_names = [row["hypertable_name"] for row in hypertables]
    console.print(f"[green]✅ Hypertables: {', '.join(ht_names) if ht_names else 'none'}[/green]")


async def main(reset: bool = False) -> None:
    """Main setup flow."""
    console.print(Panel.fit(
        "[bold cyan]Quant Trading System — Database Setup[/bold cyan]",
        border_style="cyan",
    ))

    settings = load_settings()
    setup_logging(settings.app.log_level)

    # Step 1: Check Docker
    console.print("\n[bold]Step 1: Checking Docker...[/bold]")
    docker_ok = await check_docker()
    if not docker_ok:
        console.print("[red]Fix Docker first, then re-run this script.[/red]")
        return

    # Step 2: Connect to database
    console.print("\n[bold]Step 2: Connecting to TimescaleDB...[/bold]")
    db = Database()
    try:
        await db.connect(retries=5, retry_delay=3.0)
        health = await db.health_check()
        console.print(f"[green]✅ Connected — latency: {health.get('latency_ms', '?')}ms[/green]")
    except Exception as e:
        console.print(f"[red]❌ Connection failed: {e}[/red]")
        console.print("[yellow]Make sure Docker is running: docker compose up -d[/yellow]")
        return

    try:
        # Step 3: Run migrations
        console.print("\n[bold]Step 3: Running migrations...[/bold]")
        await run_migrations(db, reset=reset)

        # Step 4: Seed instruments
        console.print("\n[bold]Step 4: Seeding instruments...[/bold]")
        await seed_instruments(db)

        # Step 5: Verify
        console.print("\n[bold]Step 5: Verifying setup...[/bold]")
        await verify_tables(db)

        console.print(Panel.fit(
            "[bold green]✅ Database setup complete![/bold green]\n"
            "Next: python scripts/backfill.py --exchange binance --timeframe 1d --days 365",
            border_style="green",
        ))

    finally:
        await db.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup Quant Trading System database")
    parser.add_argument("--reset", action="store_true", help="Drop and recreate all tables")
    args = parser.parse_args()

    asyncio.run(main(reset=args.reset))
