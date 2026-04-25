"""Quant Trading System — OHLCV Data Validator.

Validates OHLCV records for integrity, timestamp consistency,
and price anomalies. Returns a detailed ValidationResult.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from src.core.config import get_settings
from src.core.enums import AssetClass, Exchange, Timeframe
from src.core.logging import get_logger
from src.core.models import OHLCV, ValidationResult

log = get_logger(__name__)


class OHLCVValidator:
    """Validates OHLCV data for correctness and consistency.

    Checks:
    1. OHLCV integrity: high >= max(open, close, low), low <= min(open, close, high)
    2. Volume: non-negative
    3. Timestamps: monotonically increasing, no future dates, no duplicates
    4. Price anomalies: large single-candle moves (configurable thresholds)
    5. Missing data: gaps in expected candle sequence
    """

    def __init__(self):
        settings = get_settings()
        self._cfg = settings.cleaning
        self._equity_max_pct = self._cfg.equity_max_single_candle_pct
        self._crypto_max_pct = self._cfg.crypto_max_single_candle_pct

    def validate(
        self,
        candles: list[OHLCV],
        asset_class: AssetClass = AssetClass.EQUITY,
    ) -> ValidationResult:
        """Validate a list of OHLCV candles.

        Args:
            candles: List of OHLCV objects (should be sorted by timestamp).
            asset_class: Used to pick the right anomaly threshold.

        Returns:
            ValidationResult with counts and detailed violations.
        """
        result = ValidationResult(total_records=len(candles))
        violations: list[dict[str, Any]] = []

        if not candles:
            return result

        max_pct = (
            self._crypto_max_pct
            if asset_class == AssetClass.CRYPTO_FUTURES
            else self._equity_max_pct
        )

        prev_candle: OHLCV | None = None
        seen_timestamps: set[datetime] = set()

        for i, candle in enumerate(candles):
            candle_violations: list[str] = []

            # --- 1. OHLCV Integrity ---
            if candle.high < candle.open or candle.high < candle.close:
                candle_violations.append(
                    f"high ({candle.high}) < open ({candle.open}) or close ({candle.close})"
                )

            if candle.low > candle.open or candle.low > candle.close:
                candle_violations.append(
                    f"low ({candle.low}) > open ({candle.open}) or close ({candle.close})"
                )

            if candle.high < candle.low:
                candle_violations.append(
                    f"high ({candle.high}) < low ({candle.low})"
                )

            # --- 2. Volume Check ---
            if candle.volume < 0:
                candle_violations.append(f"negative volume ({candle.volume})")

            # --- 3. Timestamp Checks ---
            now = datetime.now(timezone.utc)
            if candle.timestamp > now + timedelta(hours=1):
                candle_violations.append(
                    f"future timestamp ({candle.timestamp})"
                )

            if candle.timestamp in seen_timestamps:
                candle_violations.append(
                    f"duplicate timestamp ({candle.timestamp})"
                )
            seen_timestamps.add(candle.timestamp)

            if prev_candle and candle.timestamp <= prev_candle.timestamp:
                candle_violations.append(
                    f"non-monotonic timestamp: {candle.timestamp} <= {prev_candle.timestamp}"
                )

            # --- 4. Price Anomaly (single-candle move) ---
            if prev_candle and prev_candle.close > 0:
                change_pct = abs(candle.close - prev_candle.close) / prev_candle.close
                if change_pct > max_pct:
                    candle_violations.append(
                        f"large move: {change_pct:.2%} "
                        f"(prev close={prev_candle.close}, curr close={candle.close}, "
                        f"threshold={max_pct:.0%})"
                    )

            # --- 5. Zero-price Check ---
            if candle.open == 0 or candle.close == 0:
                candle_violations.append("zero open or close price")

            # Record results
            if candle_violations:
                result.invalid_records += 1
                violations.append({
                    "index": i,
                    "timestamp": str(candle.timestamp),
                    "symbol": candle.symbol,
                    "issues": candle_violations,
                })
            else:
                result.valid_records += 1

            prev_candle = candle

        result.violations = violations

        log.info(
            "validation_complete",
            total=result.total_records,
            valid=result.valid_records,
            invalid=result.invalid_records,
            pass_rate=f"{result.pass_rate:.1f}%",
        )

        return result

    def find_gaps(
        self,
        candles: list[OHLCV],
        timeframe: Timeframe,
        market_hours_only: bool = True,
    ) -> list[dict[str, Any]]:
        """Detect gaps in the candle sequence.

        A gap is a missing candle where one is expected based on the timeframe.

        Args:
            candles: Sorted list of OHLCV objects.
            timeframe: Expected candle timeframe.
            market_hours_only: If True, don't flag gaps during market closures
                               (only applies to equity markets).

        Returns:
            List of gap descriptors with expected_time and gap_size.
        """
        if len(candles) < 2:
            return []

        gaps: list[dict[str, Any]] = []
        expected_interval = timedelta(minutes=timeframe.minutes)

        for i in range(1, len(candles)):
            prev_ts = candles[i - 1].timestamp
            curr_ts = candles[i].timestamp
            actual_interval = curr_ts - prev_ts

            # Allow 10% tolerance for timing jitter
            if actual_interval > expected_interval * 1.5:
                gap_candles = int(actual_interval / expected_interval) - 1

                # Skip overnight/weekend gaps for equity markets
                if market_hours_only and candles[i].exchange == Exchange.ANGEL_ONE:
                    if self._is_market_closure(prev_ts, curr_ts):
                        continue

                gaps.append({
                    "after_timestamp": str(prev_ts),
                    "before_timestamp": str(curr_ts),
                    "expected_interval_min": timeframe.minutes,
                    "actual_interval_min": actual_interval.total_seconds() / 60,
                    "missing_candles": gap_candles,
                    "symbol": candles[i].symbol,
                })

        if gaps:
            log.info(
                "gaps_detected",
                count=len(gaps),
                total_missing=sum(g["missing_candles"] for g in gaps),
            )

        return gaps

    @staticmethod
    def _is_market_closure(ts1: datetime, ts2: datetime) -> bool:
        """Check if the gap spans a market closure (overnight or weekend).

        Indian market hours: 9:15 AM - 3:30 PM IST (Mon-Fri).
        """
        from datetime import timezone, timedelta

        ist = timezone(timedelta(hours=5, minutes=30))
        ts1_ist = ts1.astimezone(ist)
        ts2_ist = ts2.astimezone(ist)

        # Weekend gap
        if ts1_ist.weekday() == 4 and ts2_ist.weekday() == 0:  # Fri → Mon
            return True

        # Overnight gap (same or next day)
        if ts1_ist.hour >= 15 and ts2_ist.hour <= 10:
            return True

        return False
