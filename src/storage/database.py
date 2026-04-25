"""Quant Trading System — Database Connection Pool.

Manages async connections to TimescaleDB using asyncpg.
Provides connection pooling, health checks, and migration execution.
"""

from __future__ import annotations

import asyncio
import functools
from pathlib import Path
from typing import Any, Callable, TypeVar

import asyncpg

from src.core.config import get_settings
from src.core.exceptions import DatabaseConnectionError, MigrationError
from src.core.logging import get_logger

log = get_logger(__name__)

T = TypeVar("T")

def with_retry(retries: int = 3, base_delay: float = 0.5):
    """Decorator to retry async database operations with exponential backoff."""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            last_err = None
            for attempt in range(1, retries + 1):
                try:
                    return await func(self, *args, **kwargs)
                except (asyncpg.PostgresError, OSError) as e:
                    last_err = e
                    if attempt < retries:
                        delay = base_delay * (2 ** (attempt - 1))
                        log.warning(
                            "database_operation_retry",
                            method=func.__name__,
                            attempt=attempt,
                            next_retry_in=f"{delay:.2f}s",
                            error=str(e),
                        )
                        await asyncio.sleep(delay)
            log.error("database_operation_failed", method=func.__name__, error=str(last_err))
            raise last_err
        return wrapper
    return decorator

# Path to SQL migrations
MIGRATIONS_DIR = Path(__file__).parent / "migrations"


class Database:
    """Async TimescaleDB connection pool manager.

    Usage:
        db = Database()
        await db.connect()
        async with db.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM ohlcv LIMIT 10")
        await db.disconnect()
    """

    def __init__(
        self,
        dsn: str | None = None,
        min_connections: int | None = None,
        max_connections: int | None = None,
        command_timeout: int | None = None,
    ):
        settings = get_settings()
        db_cfg = settings.database

        self._dsn = dsn or db_cfg.async_dsn
        self._min_connections = min_connections or db_cfg.min_connections
        self._max_connections = max_connections or db_cfg.max_connections
        self._command_timeout = command_timeout or db_cfg.command_timeout
        self._pool: asyncpg.Pool | None = None

    @property
    def pool(self) -> asyncpg.Pool:
        """Get the connection pool. Raises if not connected."""
        if self._pool is None:
            raise DatabaseConnectionError(
                "Database pool is not initialized. Call connect() first."
            )
        return self._pool

    @property
    def is_connected(self) -> bool:
        """Check if the pool is initialized and has available connections."""
        return self._pool is not None

    async def connect(self, retries: int = 3, retry_delay: float = 2.0) -> None:
        """Initialize the connection pool with retry logic.

        Args:
            retries: Number of connection attempts before giving up.
            retry_delay: Seconds to wait between retries (doubles each attempt).
        """
        for attempt in range(1, retries + 1):
            try:
                self._pool = await asyncpg.create_pool(
                    dsn=self._dsn,
                    min_size=self._min_connections,
                    max_size=self._max_connections,
                    command_timeout=self._command_timeout,
                )
                log.info(
                    "database_connected",
                    min_connections=self._min_connections,
                    max_connections=self._max_connections,
                )
                return
            except (OSError, asyncpg.PostgresError) as e:
                log.warning(
                    "database_connection_failed",
                    attempt=attempt,
                    max_retries=retries,
                    error=str(e),
                )
                if attempt == retries:
                    raise DatabaseConnectionError(
                        f"Failed to connect to database after {retries} attempts: {e}",
                        details={"dsn": self._dsn, "error": str(e)},
                    ) from e
                await asyncio.sleep(retry_delay * attempt)

    async def disconnect(self) -> None:
        """Close the connection pool gracefully."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            log.info("database_disconnected")

    async def health_check(self) -> dict[str, Any]:
        """Run a health check query and return pool stats.

        Returns:
            Dict with status, latency, pool size, and connection counts.
        """
        import time

        try:
            pool = self.pool
            start = time.monotonic()
            async with pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
            latency_ms = (time.monotonic() - start) * 1000

            return {
                "status": "healthy" if result == 1 else "unhealthy",
                "latency_ms": round(latency_ms, 2),
                "pool_size": pool.get_size(),
                "pool_free": pool.get_idle_size(),
                "pool_min": pool.get_min_size(),
                "pool_max": pool.get_max_size(),
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
            }

    @with_retry()
    async def execute(self, query: str, *args: Any) -> str:
        """Execute a single SQL statement.

        Args:
            query: SQL statement.
            *args: Query parameters.

        Returns:
            Status string (e.g., 'INSERT 0 1').
        """
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    @with_retry()
    async def fetch(self, query: str, *args: Any) -> list[asyncpg.Record]:
        """Execute a query and return all rows.

        Args:
            query: SQL query.
            *args: Query parameters.

        Returns:
            List of Record objects.
        """
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @with_retry()
    async def fetchrow(self, query: str, *args: Any) -> asyncpg.Record | None:
        """Execute a query and return a single row.

        Args:
            query: SQL query.
            *args: Query parameters.

        Returns:
            A single Record or None.
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    @with_retry()
    async def fetchval(self, query: str, *args: Any) -> Any:
        """Execute a query and return a single value.

        Args:
            query: SQL query.
            *args: Query parameters.

        Returns:
            The first column of the first row.
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    @with_retry()
    async def execute_many(self, query: str, args_list: list[tuple]) -> None:
        """Execute a statement for many sets of arguments (batch insert).

        Uses a transaction for atomicity and `executemany` for performance.

        Args:
            query: SQL statement with $1, $2, ... placeholders.
            args_list: List of tuples, each matching the query's parameters.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(query, args_list)

    async def copy_records(
        self,
        table_name: str,
        records: list[tuple],
        columns: list[str],
    ) -> None:
        """Bulk-insert records using PostgreSQL COPY protocol (fastest method).

        Args:
            table_name: Target table name.
            records: List of tuples to insert.
            columns: Column names matching the tuple positions.
        """
        async with self.pool.acquire() as conn:
            await conn.copy_records_to_table(
                table_name,
                records=records,
                columns=columns,
            )

    async def run_migrations(self, migrations_dir: Path | None = None) -> None:
        """Execute all SQL migration files in order.

        Files are sorted alphabetically (001_initial.sql, 002_..., etc.).

        Args:
            migrations_dir: Directory containing .sql files.
                            Defaults to src/storage/migrations/.
        """
        migrations_dir = migrations_dir or MIGRATIONS_DIR

        if not migrations_dir.exists():
            raise MigrationError(
                f"Migrations directory not found: {migrations_dir}"
            )

        sql_files = sorted(migrations_dir.glob("*.sql"))
        if not sql_files:
            log.warning("no_migrations_found", dir=str(migrations_dir))
            return

        for sql_file in sql_files:
            log.info("running_migration", file=sql_file.name)
            try:
                sql = sql_file.read_text(encoding="utf-8")
                async with self.pool.acquire() as conn:
                    await conn.execute(sql)
                log.info("migration_complete", file=sql_file.name)
            except asyncpg.PostgresError as e:
                raise MigrationError(
                    f"Migration {sql_file.name} failed: {e}",
                    details={"file": sql_file.name, "error": str(e)},
                ) from e


# Module-level singleton
_database: Database | None = None


def get_database() -> Database:
    """Get or create the global Database singleton."""
    global _database
    if _database is None:
        _database = Database()
    return _database


def reset_database() -> None:
    """Reset the database singleton (for testing)."""
    global _database
    _database = None
