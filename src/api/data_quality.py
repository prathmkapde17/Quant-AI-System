"""Quant Trading System — Data Quality Analyzer.

Provides system-wide data quality visibility: completeness percentages,
anomaly rates, gap frequency, and per-symbol health scores.
Queries the SQL views created in 002_enhancements.sql.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from src.core.enums import Exchange, Timeframe
from src.core.logging import get_logger
from src.storage.database import Database

log = get_logger(__name__)


class DataQualityAnalyzer:
    """Aggregates data quality metrics across the entire pipeline.

    Provides:
    - Completeness % per symbol/timeframe/day
    - Anomaly/issue rates and trends
    - Gap frequency analysis
    - Per-symbol health scores
    - Quality reports for monitoring and alerting
    """

    def __init__(self, db: Database):
        self._db = db

    # -------------------------------------------------------------------------
    # Completeness
    # -------------------------------------------------------------------------

    async def get_completeness(
        self,
        exchange: Exchange | None = None,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Get data completeness % per symbol per day.

        Args:
            exchange: Optional filter by exchange.
            days: Number of days to look back.

        Returns:
            List of dicts with symbol, day, actual_candles, expected_candles,
            completeness_pct.
        """
        query = """
            SELECT day, symbol, exchange, timeframe,
                   actual_candles, expected_candles, completeness_pct
            FROM data_completeness_daily
            WHERE day >= NOW() - $1::INTERVAL
        """
        args: list[Any] = [f"{days} days"]

        if exchange:
            query += " AND exchange = $2"
            args.append(exchange.value)

        query += " ORDER BY day DESC, symbol"

        try:
            rows = await self._db.fetch(query, *args)
            return [dict(row) for row in rows]
        except Exception as e:
            log.warning("completeness_query_error", error=str(e))
            return []

    async def get_completeness_summary(
        self,
        exchange: Exchange | None = None,
    ) -> dict[str, Any]:
        """Get a high-level completeness summary.

        Returns:
            Dict with overall stats: avg_completeness, fully_complete_days,
            worst_symbol, etc.
        """
        query = """
            SELECT
                ROUND(AVG(completeness_pct), 1) AS avg_completeness,
                COUNT(CASE WHEN completeness_pct >= 99 THEN 1 END) AS perfect_days,
                COUNT(CASE WHEN completeness_pct < 90 THEN 1 END) AS poor_days,
                COUNT(*) AS total_symbol_days,
                MIN(completeness_pct) AS worst_completeness
            FROM data_completeness_daily
            WHERE day >= NOW() - INTERVAL '7 days'
              AND expected_candles IS NOT NULL
        """
        args: list[Any] = []

        if exchange:
            query += " AND exchange = $1"
            args.append(exchange.value)

        try:
            row = await self._db.fetchrow(query, *args)
            return dict(row) if row else {}
        except Exception as e:
            log.warning("completeness_summary_error", error=str(e))
            return {}

    # -------------------------------------------------------------------------
    # Anomalies
    # -------------------------------------------------------------------------

    async def get_anomaly_summary(
        self,
        days: int = 7,
        exchange: Exchange | None = None,
    ) -> list[dict[str, Any]]:
        """Get anomaly counts grouped by type and severity.

        Args:
            days: Look-back window.
            exchange: Optional filter.

        Returns:
            List of anomaly summary dicts.
        """
        query = """
            SELECT day, symbol, exchange, issue_type, severity, issue_count
            FROM data_anomaly_summary
            WHERE day >= NOW() - $1::INTERVAL
        """
        args: list[Any] = [f"{days} days"]

        if exchange:
            query += " AND exchange = $2"
            args.append(exchange.value)

        query += " ORDER BY day DESC, issue_count DESC"

        try:
            rows = await self._db.fetch(query, *args)
            return [dict(row) for row in rows]
        except Exception as e:
            log.warning("anomaly_summary_error", error=str(e))
            return []

    async def get_anomaly_rates(self) -> dict[str, Any]:
        """Get overall anomaly rates (issues per 1000 records).

        Returns:
            Dict with anomaly rate metrics.
        """
        try:
            # Total records last 7 days
            total_row = await self._db.fetchrow("""
                SELECT COUNT(*) AS total_records
                FROM ohlcv
                WHERE timestamp >= NOW() - INTERVAL '7 days'
            """)
            total_records = total_row["total_records"] if total_row else 0

            # Total issues last 7 days
            issues_row = await self._db.fetchrow("""
                SELECT COUNT(*) AS total_issues,
                       COUNT(CASE WHEN severity = 'error' THEN 1 END) AS error_count,
                       COUNT(CASE WHEN severity = 'warning' THEN 1 END) AS warning_count
                FROM data_quality_log
                WHERE timestamp >= NOW() - INTERVAL '7 days'
            """)

            total_issues = issues_row["total_issues"] if issues_row else 0

            rate_per_1k = (total_issues / max(total_records, 1)) * 1000

            return {
                "total_records_7d": total_records,
                "total_issues_7d": total_issues,
                "error_count_7d": issues_row["error_count"] if issues_row else 0,
                "warning_count_7d": issues_row["warning_count"] if issues_row else 0,
                "anomaly_rate_per_1k": round(rate_per_1k, 2),
            }
        except Exception as e:
            log.warning("anomaly_rates_error", error=str(e))
            return {}

    # -------------------------------------------------------------------------
    # Gap Analysis
    # -------------------------------------------------------------------------

    async def get_gap_report(
        self,
        symbol: str,
        exchange: Exchange,
        timeframe: Timeframe = Timeframe.M1,
        days: int = 7,
    ) -> list[dict[str, Any]]:
        """Detect gaps in the candle sequence for a symbol.

        Uses SQL window functions to find missing candles efficiently.

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.
            timeframe: Expected candle interval.
            days: Look-back window.

        Returns:
            List of gap descriptors with start, end, and missing count.
        """
        interval_minutes = timeframe.minutes

        # Use LAG to find gaps between consecutive candles
        query = """
            WITH ordered_candles AS (
                SELECT
                    timestamp,
                    LAG(timestamp) OVER (ORDER BY timestamp) AS prev_timestamp
                FROM ohlcv
                WHERE symbol = $1
                  AND exchange = $2
                  AND timeframe = $3
                  AND timestamp >= NOW() - $4::INTERVAL
            )
            SELECT
                prev_timestamp AS gap_start,
                timestamp AS gap_end,
                EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 60 AS gap_minutes,
                FLOOR(EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 60 / $5) - 1
                    AS missing_candles
            FROM ordered_candles
            WHERE prev_timestamp IS NOT NULL
              AND EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 60 > $5 * 1.5
            ORDER BY gap_minutes DESC
            LIMIT 50
        """

        try:
            rows = await self._db.fetch(
                query,
                symbol, exchange.value, timeframe.value,
                f"{days} days", float(interval_minutes),
            )
            return [dict(row) for row in rows]
        except Exception as e:
            log.warning("gap_report_error", error=str(e), symbol=symbol)
            return []

    # -------------------------------------------------------------------------
    # Health Overview
    # -------------------------------------------------------------------------

    async def get_health_overview(self) -> list[dict[str, Any]]:
        """Get per-symbol health overview from the SQL view.

        Returns:
            List of dicts with symbol, latest data, 24h counts, latency.
        """
        try:
            rows = await self._db.fetch("""
                SELECT symbol, exchange, asset_class, is_active,
                       latest_1m, candles_24h, issues_24h, avg_latency_1h_ms
                FROM data_health_overview
                ORDER BY symbol
            """)
            return [dict(row) for row in rows]
        except Exception as e:
            log.warning("health_overview_error", error=str(e))
            return []

    async def get_quality_score(
        self,
        symbol: str,
        exchange: Exchange,
    ) -> dict[str, Any]:
        """Compute a composite quality score (0-100) for a symbol.

        Scoring:
        - Completeness (40%): based on last 7 days
        - Anomaly rate (30%): fewer anomalies = higher score
        - Freshness (30%): how recent is the latest data

        Args:
            symbol: Instrument symbol.
            exchange: Exchange enum.

        Returns:
            Dict with component scores and overall score.
        """
        # Completeness score (0-40)
        completeness_data = await self.get_completeness(exchange, days=7)
        symbol_data = [d for d in completeness_data if d.get("symbol") == symbol]
        if symbol_data:
            avg_completeness = sum(
                d.get("completeness_pct", 0) or 0 for d in symbol_data
            ) / len(symbol_data)
            completeness_score = min(avg_completeness / 100 * 40, 40)
        else:
            completeness_score = 0

        # Anomaly score (0-30): 0 issues = 30, >10 issues/day = 0
        try:
            issues_row = await self._db.fetchrow("""
                SELECT COUNT(*) AS cnt FROM data_quality_log
                WHERE symbol = $1 AND exchange = $2
                AND timestamp >= NOW() - INTERVAL '7 days'
            """, symbol, exchange.value)
            issue_count = issues_row["cnt"] if issues_row else 0
            daily_issues = issue_count / 7
            anomaly_score = max(30 - daily_issues * 3, 0)
        except Exception:
            anomaly_score = 15  # Default middle score

        # Freshness score (0-30): <1min = 30, >1hour = 0
        try:
            latest_row = await self._db.fetchrow("""
                SELECT MAX(timestamp) AS latest FROM ohlcv
                WHERE symbol = $1 AND exchange = $2 AND timeframe = '1m'
            """, symbol, exchange.value)
            if latest_row and latest_row["latest"]:
                age_minutes = (
                    datetime.now(timezone.utc) - latest_row["latest"]
                ).total_seconds() / 60
                freshness_score = max(30 - age_minutes * 0.5, 0)
            else:
                freshness_score = 0
        except Exception:
            freshness_score = 0

        overall = round(completeness_score + anomaly_score + freshness_score, 1)

        return {
            "symbol": symbol,
            "exchange": exchange.value,
            "overall_score": overall,
            "completeness_score": round(completeness_score, 1),
            "anomaly_score": round(anomaly_score, 1),
            "freshness_score": round(freshness_score, 1),
            "grade": (
                "A" if overall >= 85 else
                "B" if overall >= 70 else
                "C" if overall >= 50 else
                "D" if overall >= 30 else "F"
            ),
        }
