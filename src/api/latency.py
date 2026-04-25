"""Quant Trading System — Pipeline Latency Tracker.

Records timestamps at each processing stage and computes latencies.
Enables per-symbol latency monitoring and alerting.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from src.core.logging import get_logger
from src.storage.database import Database

log = get_logger(__name__)


@dataclass
class LatencyRecord:
    """Captures timestamps at each pipeline stage for a single data point.

    Create at ingestion, stamp at each stage, then flush to DB.
    """

    symbol: str
    exchange: str
    data_type: str = "candle"  # "candle" or "tick"

    # Pipeline stage timestamps (UTC)
    exchange_ts: datetime | None = None       # when exchange produced the data
    received_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    normalized_ts: datetime | None = None
    stored_ts: datetime | None = None
    published_ts: datetime | None = None

    def stamp_normalized(self) -> None:
        """Mark normalization complete."""
        self.normalized_ts = datetime.now(timezone.utc)

    def stamp_stored(self) -> None:
        """Mark DB storage complete."""
        self.stored_ts = datetime.now(timezone.utc)

    def stamp_published(self) -> None:
        """Mark Redis publish complete."""
        self.published_ts = datetime.now(timezone.utc)

    @property
    def ingestion_latency_ms(self) -> float | None:
        """Time from exchange to our system (network + processing)."""
        if self.exchange_ts and self.received_ts:
            return (self.received_ts - self.exchange_ts).total_seconds() * 1000
        return None

    @property
    def processing_latency_ms(self) -> float | None:
        """Time from receipt to DB storage."""
        if self.received_ts and self.stored_ts:
            return (self.stored_ts - self.received_ts).total_seconds() * 1000
        return None

    @property
    def e2e_latency_ms(self) -> float | None:
        """End-to-end: exchange to DB storage."""
        if self.exchange_ts and self.stored_ts:
            return (self.stored_ts - self.exchange_ts).total_seconds() * 1000
        return None

    def to_db_tuple(self) -> tuple:
        """Convert to tuple matching pipeline_latency table columns."""
        ts = self.exchange_ts or self.received_ts
        return (
            ts,
            self.symbol,
            self.exchange,
            self.data_type,
            self.exchange_ts,
            self.received_ts,
            self.normalized_ts,
            self.stored_ts,
            self.published_ts,
            self.ingestion_latency_ms,
            self.processing_latency_ms,
            self.e2e_latency_ms,
        )


class LatencyTracker:
    """Buffers and flushes latency records to the database.

    Usage:
        tracker = LatencyTracker(db)

        # At ingestion:
        rec = tracker.start("BTCUSDT", "binance", exchange_ts=candle.timestamp)

        # At each stage:
        rec.stamp_normalized()
        rec.stamp_stored()
        rec.stamp_published()

        # Periodic flush (or manual):
        await tracker.flush()
    """

    # Sampling rate: only track 1 in N records (reduces overhead)
    SAMPLE_RATE = 10  # Track every 10th record

    def __init__(self, db: Database, sample_rate: int | None = None):
        self._db = db
        self._buffer: list[LatencyRecord] = []
        self._counter = 0
        self._sample_rate = sample_rate or self.SAMPLE_RATE

    def start(
        self,
        symbol: str,
        exchange: str,
        data_type: str = "candle",
        exchange_ts: datetime | None = None,
    ) -> LatencyRecord | None:
        """Create a new latency record (sampled).

        Returns None if this record is skipped by sampling.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange name.
            data_type: "candle" or "tick".
            exchange_ts: When the exchange produced this data.

        Returns:
            LatencyRecord to stamp at each stage, or None (skip).
        """
        self._counter += 1
        if self._counter % self._sample_rate != 0:
            return None

        record = LatencyRecord(
            symbol=symbol,
            exchange=exchange,
            data_type=data_type,
            exchange_ts=exchange_ts,
        )
        self._buffer.append(record)
        return record

    async def flush(self) -> int:
        """Write all buffered latency records to the database.

        Returns:
            Number of records flushed.
        """
        if not self._buffer:
            return 0

        records = self._buffer.copy()
        self._buffer.clear()

        # Only flush records that have at least been stored
        complete = [r for r in records if r.stored_ts is not None]
        if not complete:
            return 0

        query = """
            INSERT INTO pipeline_latency (
                timestamp, symbol, exchange, data_type,
                exchange_ts, received_ts, normalized_ts, stored_ts, published_ts,
                ingestion_latency_ms, processing_latency_ms, e2e_latency_ms
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """

        args_list = [r.to_db_tuple() for r in complete]

        try:
            await self._db.execute_many(query, args_list)
            log.debug("latency_records_flushed", count=len(complete))
            return len(complete)
        except Exception as e:
            log.warning("latency_flush_error", error=str(e), count=len(complete))
            return 0

    async def get_summary(self, hours: int = 1) -> list[dict[str, Any]]:
        """Get latency summary from the latency_summary_24h view.

        Args:
            hours: Look-back window.

        Returns:
            List of latency summary dicts.
        """
        try:
            rows = await self._db.fetch("""
                SELECT symbol, exchange, data_type,
                       sample_count, avg_ingestion_ms, avg_processing_ms,
                       avg_e2e_ms, max_e2e_ms, p95_e2e_ms
                FROM latency_summary_24h
                ORDER BY avg_e2e_ms DESC
            """)
            return [dict(row) for row in rows]
        except Exception as e:
            log.warning("latency_summary_error", error=str(e))
            return []

    @property
    def buffer_size(self) -> int:
        return len(self._buffer)
