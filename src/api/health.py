"""Quant Trading System — Health Monitor.

Aggregates health status from all subsystems: database, Redis,
WebSocket connections, and data freshness.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as redis

from src.core.enums import ConnectionStatus, Exchange
from src.core.logging import get_logger
from src.core.models import ComponentHealth, ConnectionHealth, HealthReport
from src.connectors.base import AbstractConnector
from src.storage.database import Database

log = get_logger(__name__)


class HealthChecker:
    """Aggregates health from all pipeline subsystems.

    Checks:
    - TimescaleDB: ping + last write latency + pool stats
    - Redis: ping + stream lengths
    - WebSockets: connected/disconnected per exchange
    - Data freshness: age of last tick per symbol

    Returns a structured HealthReport that can be logged, displayed
    in a dashboard, or used to trigger alerts.
    """

    def __init__(
        self,
        db: Database,
        redis_client: redis.Redis | None = None,
        connectors: dict[Exchange, AbstractConnector] | None = None,
        last_tick_times: dict[str, datetime] | None = None,
    ):
        """Initialize the health checker.

        Args:
            db: Database instance.
            redis_client: Optional Redis client.
            connectors: Map of Exchange → connector instances.
            last_tick_times: Live ingester's last_tick_times dict reference.
        """
        self._db = db
        self._redis = redis_client
        self._connectors = connectors or {}
        self._last_tick_times = last_tick_times or {}

    # -------------------------------------------------------------------------
    # Full Health Report
    # -------------------------------------------------------------------------

    async def check(self) -> HealthReport:
        """Run all health checks and return a comprehensive report.

        Returns:
            HealthReport with overall status and per-component details.
        """
        components: list[ComponentHealth] = []
        connections: list[ConnectionHealth] = []
        now = datetime.now(timezone.utc)

        # 1. Database health
        db_health = await self._check_database()
        components.append(db_health)

        # 2. Redis health
        redis_health = await self._check_redis()
        components.append(redis_health)

        # 3. WebSocket connections
        for exchange, connector in self._connectors.items():
            conn_health = self._check_connector(exchange, connector)
            connections.append(conn_health)

        # 4. Data freshness
        freshness_health = self._check_data_freshness()
        components.append(freshness_health)

        # Determine overall status
        statuses = [c.status for c in components] + [
            "healthy" if c.status == ConnectionStatus.CONNECTED else "unhealthy"
            for c in connections
        ]

        if all(s == "healthy" for s in statuses):
            overall = "healthy"
        elif any(s == "unhealthy" for s in statuses):
            overall = "unhealthy"
        else:
            overall = "degraded"

        report = HealthReport(
            timestamp=now,
            status=overall,
            components=components,
            connections=connections,
        )

        log.info(
            "health_check_complete",
            status=overall,
            components={c.name: c.status for c in components},
        )

        return report

    # -------------------------------------------------------------------------
    # Individual Checks
    # -------------------------------------------------------------------------

    async def _check_database(self) -> ComponentHealth:
        """Check TimescaleDB connectivity and performance."""
        try:
            health = await self._db.health_check()
            return ComponentHealth(
                name="timescaledb",
                status=health.get("status", "unhealthy"),
                latency_ms=health.get("latency_ms"),
                last_check=datetime.now(timezone.utc),
                details={
                    "pool_size": health.get("pool_size"),
                    "pool_free": health.get("pool_free"),
                    "pool_min": health.get("pool_min"),
                    "pool_max": health.get("pool_max"),
                },
            )
        except Exception as e:
            return ComponentHealth(
                name="timescaledb",
                status="unhealthy",
                last_check=datetime.now(timezone.utc),
                details={"error": str(e)},
            )

    async def _check_redis(self) -> ComponentHealth:
        """Check Redis connectivity and stream statistics."""
        if not self._redis:
            return ComponentHealth(
                name="redis",
                status="unhealthy",
                last_check=datetime.now(timezone.utc),
                details={"error": "Redis client not configured"},
            )

        try:
            start = time.monotonic()
            pong = await self._redis.ping()
            latency_ms = (time.monotonic() - start) * 1000

            # Get stream stats
            stream_count = 0
            total_messages = 0
            try:
                async for key in self._redis.scan_iter("market:*"):
                    stream_count += 1
                    length = await self._redis.xlen(key)
                    total_messages += length
            except Exception:
                pass

            # Get memory info
            memory_info = {}
            try:
                info = await self._redis.info("memory")
                memory_info = {
                    "used_memory_human": info.get("used_memory_human", "N/A"),
                    "used_memory_peak_human": info.get("used_memory_peak_human", "N/A"),
                }
            except Exception:
                pass

            return ComponentHealth(
                name="redis",
                status="healthy" if pong else "unhealthy",
                latency_ms=round(latency_ms, 2),
                last_check=datetime.now(timezone.utc),
                details={
                    "streams": stream_count,
                    "total_messages": total_messages,
                    **memory_info,
                },
            )

        except Exception as e:
            return ComponentHealth(
                name="redis",
                status="unhealthy",
                last_check=datetime.now(timezone.utc),
                details={"error": str(e)},
            )

    def _check_connector(
        self,
        exchange: Exchange,
        connector: AbstractConnector,
    ) -> ConnectionHealth:
        """Check a WebSocket connector's status."""
        return ConnectionHealth(
            exchange=exchange,
            status=connector.status,
            subscribed_symbols=[],  # Could be populated from connector internals
        )

    def _check_data_freshness(self) -> ComponentHealth:
        """Check how fresh the incoming data is."""
        if not self._last_tick_times:
            return ComponentHealth(
                name="data_freshness",
                status="degraded",
                last_check=datetime.now(timezone.utc),
                details={"info": "No live data received yet"},
            )

        now = datetime.now(timezone.utc)
        stale_symbols: list[str] = []
        fresh_symbols: list[str] = []
        stale_threshold = 120  # seconds

        for key, last_time in self._last_tick_times.items():
            age = (now - last_time).total_seconds()
            if age > stale_threshold:
                stale_symbols.append(f"{key} ({age:.0f}s)")
            else:
                fresh_symbols.append(key)

        status = "healthy"
        if stale_symbols:
            status = "degraded" if fresh_symbols else "unhealthy"

        return ComponentHealth(
            name="data_freshness",
            status=status,
            last_check=now,
            details={
                "fresh_count": len(fresh_symbols),
                "stale_count": len(stale_symbols),
                "stale_symbols": stale_symbols[:10],  # Limit output
            },
        )

    # -------------------------------------------------------------------------
    # Quick Checks
    # -------------------------------------------------------------------------

    async def is_healthy(self) -> bool:
        """Quick boolean health check.

        Returns:
            True if all critical systems are healthy.
        """
        report = await self.check()
        return report.status == "healthy"

    async def get_summary(self) -> dict[str, Any]:
        """Get a compact health summary dict.

        Useful for logging and quick diagnostics.
        """
        report = await self.check()
        return {
            "status": report.status,
            "timestamp": report.timestamp.isoformat(),
            "components": {c.name: c.status for c in report.components},
            "connections": {
                c.exchange.value: c.status.value for c in report.connections
            },
        }
