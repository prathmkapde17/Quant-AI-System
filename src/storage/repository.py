"""Quant Trading System — OHLCV & Tick Repository.

Provides CRUD operations for market data with idempotent upserts,
bulk inserts, and flexible querying.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import asyncpg

from src.core.enums import DataQuality, Exchange, Timeframe
from src.core.exceptions import DatabaseReadError, DatabaseWriteError
from src.core.logging import get_logger
from src.core.models import OHLCV, Tick
from src.storage.database import Database

log = get_logger(__name__)


# =============================================================================
# OHLCV Columns — matches the DB table and model's to_db_tuple() order
# =============================================================================

OHLCV_COLUMNS = [
    "timestamp", "symbol", "exchange", "timeframe",
    "open", "high", "low", "close", "volume",
    "turnover", "num_trades", "quality",
]

TICK_COLUMNS = [
    "timestamp", "symbol", "exchange",
    "ltp", "volume", "bid", "ask", "oi",
]


class OHLCVRepository:
    """Repository for OHLCV candlestick data.

    Handles bulk upserts, time-range queries, and latest-timestamp lookups
    used by the ingestion engine for incremental sync.
    """

    def __init__(self, db: Database):
        self._db = db

    # -------------------------------------------------------------------------
    # Writes
    # -------------------------------------------------------------------------

    async def upsert_many(self, candles: list[OHLCV]) -> int:
        """Bulk upsert OHLCV records (idempotent — ON CONFLICT UPDATE).

        Args:
            candles: List of OHLCV model instances.

        Returns:
            Number of records upserted.
        """
        if not candles:
            return 0

        query = """
            INSERT INTO ohlcv (
                timestamp, symbol, exchange, timeframe,
                open, high, low, close, volume,
                turnover, num_trades, quality
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (timestamp, symbol, exchange, timeframe)
            DO UPDATE SET
                open       = EXCLUDED.open,
                high       = EXCLUDED.high,
                low        = EXCLUDED.low,
                close      = EXCLUDED.close,
                volume     = EXCLUDED.volume,
                turnover   = EXCLUDED.turnover,
                num_trades = EXCLUDED.num_trades,
                quality    = EXCLUDED.quality
        """

        args_list = [candle.to_db_tuple() for candle in candles]

        try:
            await self._db.execute_many(query, args_list)
            log.debug(
                "ohlcv_upserted",
                count=len(candles),
                symbol=candles[0].symbol,
                exchange=candles[0].exchange.value,
            )
            return len(candles)
        except asyncpg.PostgresError as e:
            raise DatabaseWriteError(
                f"Failed to upsert {len(candles)} OHLCV records: {e}",
                details={"symbol": candles[0].symbol, "error": str(e)},
            ) from e

    async def bulk_insert(self, candles: list[OHLCV]) -> int:
        """Fast bulk insert using COPY protocol (no conflict handling).

        Use this for initial backfill when you're sure there are no duplicates.
        Falls back to upsert_many if COPY fails (e.g., duplicates exist).

        Args:
            candles: List of OHLCV model instances.

        Returns:
            Number of records inserted.
        """
        if not candles:
            return 0

        records = [candle.to_db_tuple() for candle in candles]

        try:
            await self._db.copy_records(
                table_name="ohlcv",
                records=records,
                columns=OHLCV_COLUMNS,
            )
            log.debug("ohlcv_bulk_inserted", count=len(candles))
            return len(candles)
        except asyncpg.UniqueViolationError:
            log.warning(
                "ohlcv_bulk_insert_conflict_fallback_to_upsert",
                count=len(candles),
            )
            return await self.upsert_many(candles)
        except asyncpg.PostgresError as e:
            raise DatabaseWriteError(
                f"Failed to bulk insert {len(candles)} OHLCV records: {e}",
                details={"error": str(e)},
            ) from e

    # -------------------------------------------------------------------------
    # Reads
    # -------------------------------------------------------------------------

    async def get_ohlcv(
        self,
        symbol: str,
        exchange: Exchange,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
        quality: DataQuality | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch OHLCV records for a symbol over a time range.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.
            timeframe: Candle timeframe.
            start: Start time (inclusive).
            end: End time (inclusive).
            quality: Optional quality filter.
            limit: Max records to return.

        Returns:
            List of dicts with OHLCV data.
        """
        query = """
            SELECT timestamp, symbol, exchange, timeframe,
                   open, high, low, close, volume,
                   turnover, num_trades, quality
            FROM ohlcv
            WHERE symbol = $1
              AND exchange = $2
              AND timeframe = $3
              AND timestamp >= $4
              AND timestamp <= $5
        """
        args: list[Any] = [symbol, exchange.value, timeframe.value, start, end]

        if quality is not None:
            query += " AND quality = $6"
            args.append(quality.value)

        query += " ORDER BY timestamp ASC"

        if limit is not None:
            query += f" LIMIT {int(limit)}"

        try:
            rows = await self._db.fetch(query, *args)
            return [dict(row) for row in rows]
        except asyncpg.PostgresError as e:
            raise DatabaseReadError(
                f"Failed to query OHLCV for {symbol}: {e}",
                details={"symbol": symbol, "exchange": exchange.value, "error": str(e)},
            ) from e

    async def get_latest_timestamp(
        self,
        symbol: str,
        exchange: Exchange,
        timeframe: Timeframe,
    ) -> datetime | None:
        """Get the most recent candle timestamp for incremental sync.

        This is called before fetching historical data to determine
        where to resume from.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.
            timeframe: Candle timeframe.

        Returns:
            Latest timestamp or None if no data exists.
        """
        query = """
            SELECT MAX(timestamp) FROM ohlcv
            WHERE symbol = $1 AND exchange = $2 AND timeframe = $3
        """
        try:
            return await self._db.fetchval(
                query, symbol, exchange.value, timeframe.value
            )
        except asyncpg.PostgresError as e:
            raise DatabaseReadError(
                f"Failed to get latest timestamp for {symbol}: {e}",
            ) from e

    async def get_record_count(
        self,
        symbol: str | None = None,
        exchange: Exchange | None = None,
        timeframe: Timeframe | None = None,
    ) -> int:
        """Get total record count with optional filters.

        Args:
            symbol: Optional symbol filter.
            exchange: Optional exchange filter.
            timeframe: Optional timeframe filter.

        Returns:
            Number of matching records.
        """
        conditions = []
        args: list[Any] = []
        idx = 1

        if symbol:
            conditions.append(f"symbol = ${idx}")
            args.append(symbol)
            idx += 1
        if exchange:
            conditions.append(f"exchange = ${idx}")
            args.append(exchange.value)
            idx += 1
        if timeframe:
            conditions.append(f"timeframe = ${idx}")
            args.append(timeframe.value)
            idx += 1

        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        query = f"SELECT COUNT(*) FROM ohlcv WHERE {where_clause}"

        try:
            return await self._db.fetchval(query, *args)
        except asyncpg.PostgresError as e:
            raise DatabaseReadError(
                f"Failed to get record count: {e}",
            ) from e

    async def get_symbols(
        self,
        exchange: Exchange | None = None,
    ) -> list[str]:
        """Get all distinct symbols in the database.

        Args:
            exchange: Optional exchange filter.

        Returns:
            List of symbol strings.
        """
        if exchange:
            query = "SELECT DISTINCT symbol FROM ohlcv WHERE exchange = $1 ORDER BY symbol"
            rows = await self._db.fetch(query, exchange.value)
        else:
            query = "SELECT DISTINCT symbol FROM ohlcv ORDER BY symbol"
            rows = await self._db.fetch(query)

        return [row["symbol"] for row in rows]

    async def get_time_range(
        self,
        symbol: str,
        exchange: Exchange,
        timeframe: Timeframe,
    ) -> tuple[datetime | None, datetime | None]:
        """Get the earliest and latest timestamps for a symbol."""
        query = """
            SELECT MIN(timestamp), MAX(timestamp) FROM ohlcv
            WHERE symbol = $1 AND exchange = $2 AND timeframe = $3
        """
        try:
            row = await self._db.fetchrow(
                query, symbol, exchange.value, timeframe.value
            )
            if row:
                return row[0], row[1]
            return None, None
        except asyncpg.PostgresError as e:
            raise DatabaseReadError(
                f"Failed to get time range for {symbol}: {e}",
            ) from e

    async def get_recent_bars(
        self,
        symbol: str,
        exchange: Exchange,
        timeframe: Timeframe,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Fetch the most recent N bars for a symbol.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.
            timeframe: Timeframe enum.
            limit: Number of bars to fetch.

        Returns:
            List of bar dicts, sorted by timestamp ASCENDING.
        """
        query = """
            SELECT timestamp, open, high, low, close, volume
            FROM (
                SELECT timestamp, open, high, low, close, volume
                FROM ohlcv
                WHERE symbol = $1 AND exchange = $2 AND timeframe = $3
                ORDER BY timestamp DESC
                LIMIT $4
            ) sub
            ORDER BY timestamp ASC
        """
        try:
            rows = await self._db.fetch(query, symbol, exchange.value, timeframe.value, limit)
            return [dict(row) for row in rows]
        except asyncpg.PostgresError as e:
            raise DatabaseReadError(
                f"Failed to fetch recent bars for {symbol}: {e}",
            ) from e

    # -------------------------------------------------------------------------
    # Deletes
    # -------------------------------------------------------------------------

    async def delete_range(
        self,
        symbol: str,
        exchange: Exchange,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> int:
        """Delete OHLCV records in a time range (used for re-ingestion).

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.
            timeframe: Candle timeframe.
            start: Start time (inclusive).
            end: End time (inclusive).

        Returns:
            Number of records deleted.
        """
        query = """
            DELETE FROM ohlcv
            WHERE symbol = $1
              AND exchange = $2
              AND timeframe = $3
              AND timestamp >= $4
              AND timestamp <= $5
        """
        try:
            result = await self._db.execute(
                query, symbol, exchange.value, timeframe.value, start, end
            )
            count = int(result.split()[-1])
            log.info(
                "ohlcv_deleted",
                count=count,
                symbol=symbol,
                exchange=exchange.value,
                start=str(start),
                end=str(end),
            )
            return count
        except asyncpg.PostgresError as e:
            raise DatabaseWriteError(
                f"Failed to delete OHLCV range for {symbol}: {e}",
            ) from e


class TickRepository:
    """Repository for real-time tick data.

    Optimized for high-throughput writes from live WebSocket feeds.
    """

    def __init__(self, db: Database):
        self._db = db

    async def bulk_insert(self, ticks: list[Tick]) -> int:
        """Fast bulk insert ticks using COPY protocol.

        Args:
            ticks: List of Tick model instances.

        Returns:
            Number of records inserted.
        """
        if not ticks:
            return 0

        records = [tick.to_db_tuple() for tick in ticks]

        try:
            await self._db.copy_records(
                table_name="ticks",
                records=records,
                columns=TICK_COLUMNS,
            )
            log.debug("ticks_bulk_inserted", count=len(ticks))
            return len(ticks)
        except asyncpg.PostgresError as e:
            raise DatabaseWriteError(
                f"Failed to bulk insert {len(ticks)} ticks: {e}",
                details={"error": str(e)},
            ) from e

    async def insert_many(self, ticks: list[Tick]) -> int:
        """Insert ticks using executemany (slower but handles errors better).

        Args:
            ticks: List of Tick model instances.

        Returns:
            Number of records inserted.
        """
        if not ticks:
            return 0

        query = """
            INSERT INTO ticks (timestamp, symbol, exchange, ltp, volume, bid, ask, oi)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
        args_list = [tick.to_db_tuple() for tick in ticks]

        try:
            await self._db.execute_many(query, args_list)
            return len(ticks)
        except asyncpg.PostgresError as e:
            raise DatabaseWriteError(
                f"Failed to insert {len(ticks)} ticks: {e}",
            ) from e

    async def get_latest_ticks(
        self,
        symbol: str,
        exchange: Exchange,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get the most recent ticks for a symbol.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.
            limit: Max ticks to return.

        Returns:
            List of tick dicts, most recent first.
        """
        query = """
            SELECT timestamp, symbol, exchange, ltp, volume, bid, ask, oi
            FROM ticks
            WHERE symbol = $1 AND exchange = $2
            ORDER BY timestamp DESC
            LIMIT $3
        """
        try:
            rows = await self._db.fetch(query, symbol, exchange.value, limit)
            return [dict(row) for row in rows]
        except asyncpg.PostgresError as e:
            raise DatabaseReadError(
                f"Failed to get ticks for {symbol}: {e}",
            ) from e

    async def get_latest_tick(
        self,
        symbol: str,
        exchange: Exchange,
    ) -> dict[str, Any] | None:
        """Get the single most recent tick for a symbol.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.

        Returns:
            Tick dict or None.
        """
        query = """
            SELECT timestamp, symbol, exchange, ltp, volume, bid, ask, oi
            FROM ticks
            WHERE symbol = $1 AND exchange = $2
            ORDER BY timestamp DESC
            LIMIT 1
        """
        try:
            row = await self._db.fetchrow(query, symbol, exchange.value)
            return dict(row) if row else None
        except asyncpg.PostgresError as e:
            raise DatabaseReadError(
                f"Failed to get latest tick for {symbol}: {e}",
            ) from e
