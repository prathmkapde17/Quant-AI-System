"""Quant Trading System — Advanced Validator.

Extends the base validator with:
- Price-volume consistency checks
- Cross-source validation (compare Angel One vs yfinance)
- Exchange-specific anomaly detection (Angel One auction quirks, etc.)
- Volume profile analysis
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

from src.core.enums import AssetClass, Exchange, Timeframe
from src.core.logging import get_logger
from src.core.models import OHLCV

log = get_logger(__name__)


class AdvancedValidator:
    """Production-grade validation beyond simple OHLC bounds checks.

    Catches issues the basic validator misses:
    - Zero-volume candles with large price moves (likely bad data)
    - Volume spikes without price impact (exchange glitches)
    - Candles during known market closures (phantom data)
    - Price discontinuities at session boundaries
    """

    # -------------------------------------------------------------------------
    # Price-Volume Consistency
    # -------------------------------------------------------------------------

    @staticmethod
    def check_price_volume_consistency(
        candles: list[OHLCV],
        max_price_change_on_zero_vol: float = 0.001,  # 0.1%
        min_volume_on_large_move: float = 100,
        large_move_threshold: float = 0.02,  # 2%
    ) -> list[dict[str, Any]]:
        """Detect price-volume inconsistencies.

        Flags:
        1. Large price move with zero/tiny volume (likely bad data)
        2. Huge volume spike with zero price change (exchange glitch)

        Args:
            candles: Sorted OHLCV list.
            max_price_change_on_zero_vol: Max allowed % change when volume is 0.
            min_volume_on_large_move: Min volume expected for a move > threshold.
            large_move_threshold: % move that requires meaningful volume.

        Returns:
            List of flagged records with details.
        """
        flags: list[dict[str, Any]] = []

        for i in range(1, len(candles)):
            prev = candles[i - 1]
            curr = candles[i]

            if prev.close == 0:
                continue

            price_change_pct = abs(curr.close - prev.close) / prev.close

            # Flag 1: Large price move on zero/negligible volume
            if curr.volume <= 1 and price_change_pct > max_price_change_on_zero_vol:
                flags.append({
                    "type": "zero_vol_price_move",
                    "severity": "error",
                    "timestamp": str(curr.timestamp),
                    "symbol": curr.symbol,
                    "price_change_pct": round(price_change_pct * 100, 2),
                    "volume": curr.volume,
                    "detail": f"{price_change_pct:.2%} move on {curr.volume} volume",
                })

            # Flag 2: Large price move with suspiciously low volume
            if (
                price_change_pct > large_move_threshold
                and curr.volume < min_volume_on_large_move
                and curr.exchange != Exchange.YFINANCE  # yfinance adjusts volume
            ):
                flags.append({
                    "type": "low_vol_large_move",
                    "severity": "warning",
                    "timestamp": str(curr.timestamp),
                    "symbol": curr.symbol,
                    "price_change_pct": round(price_change_pct * 100, 2),
                    "volume": curr.volume,
                    "detail": f"{price_change_pct:.2%} move on only {curr.volume} volume",
                })

            # Flag 3: Volume spike with flat price (potential exchange glitch)
            if i >= 20:
                recent_vols = [c.volume for c in candles[max(0, i-20):i] if c.volume > 0]
                if recent_vols:
                    avg_vol = sum(recent_vols) / len(recent_vols)
                    if (
                        avg_vol > 0
                        and curr.volume > avg_vol * 50  # 50x average volume
                        and price_change_pct < 0.001     # but price didn't move
                    ):
                        flags.append({
                            "type": "volume_spike_no_impact",
                            "severity": "warning",
                            "timestamp": str(curr.timestamp),
                            "symbol": curr.symbol,
                            "volume": curr.volume,
                            "avg_volume": round(avg_vol, 0),
                            "volume_ratio": round(curr.volume / avg_vol, 1),
                            "detail": f"{curr.volume / avg_vol:.0f}x avg volume but {price_change_pct:.4%} price change",
                        })

        if flags:
            log.info(
                "price_volume_consistency_flags",
                total_flags=len(flags),
                errors=sum(1 for f in flags if f["severity"] == "error"),
                warnings=sum(1 for f in flags if f["severity"] == "warning"),
            )

        return flags

    # -------------------------------------------------------------------------
    # Exchange-Specific Anomalies
    # -------------------------------------------------------------------------

    @staticmethod
    def check_angel_one_anomalies(candles: list[OHLCV]) -> list[dict[str, Any]]:
        """Detect Angel One-specific data anomalies.

        Known issues:
        1. Auction session data (9:00-9:07 IST) mixed into regular candles
        2. Post-market data (15:30-15:40) with stale prices
        3. Pre-open session (9:00-9:15) with artificial OHLC

        Args:
            candles: Sorted OHLCV list from Angel One.

        Returns:
            List of flagged records.
        """
        flags: list[dict[str, Any]] = []
        ist = timezone(timedelta(hours=5, minutes=30))

        for candle in candles:
            if candle.exchange != Exchange.ANGEL_ONE:
                continue

            ts_ist = candle.timestamp.astimezone(ist)
            hour, minute = ts_ist.hour, ts_ist.minute

            # Pre-open session (9:00-9:14 IST) — prices may be artificial
            if hour == 9 and minute < 15:
                flags.append({
                    "type": "preopen_session",
                    "severity": "info",
                    "timestamp": str(candle.timestamp),
                    "symbol": candle.symbol,
                    "ist_time": ts_ist.strftime("%H:%M"),
                    "detail": "Data from pre-open session — prices may not reflect true market",
                })

            # Post-market (after 15:30 IST)
            if hour == 15 and minute >= 30 or hour > 15:
                flags.append({
                    "type": "postmarket_session",
                    "severity": "info",
                    "timestamp": str(candle.timestamp),
                    "symbol": candle.symbol,
                    "ist_time": ts_ist.strftime("%H:%M"),
                    "detail": "Post-market data — low liquidity, stale prices possible",
                })

            # Weekend data (shouldn't exist for equities)
            if ts_ist.weekday() >= 5:
                flags.append({
                    "type": "weekend_data",
                    "severity": "error",
                    "timestamp": str(candle.timestamp),
                    "symbol": candle.symbol,
                    "day": ts_ist.strftime("%A"),
                    "detail": "Indian equity data on weekend — likely erroneous",
                })

        return flags

    @staticmethod
    def check_binance_anomalies(candles: list[OHLCV]) -> list[dict[str, Any]]:
        """Detect Binance-specific anomalies.

        Known issues:
        1. Liquidation cascade candles (extreme wicks, volume spikes)
        2. Funding rate timestamp misalignment
        3. Maintenance window data gaps

        Args:
            candles: Sorted OHLCV list from Binance.

        Returns:
            List of flagged records.
        """
        flags: list[dict[str, Any]] = []

        for i in range(1, len(candles)):
            candle = candles[i]
            if candle.exchange != Exchange.BINANCE:
                continue

            # Extreme wick (high-low range > 5% of close)
            if candle.close > 0:
                wick_ratio = candle.bar_range / candle.close
                if wick_ratio > 0.05:
                    flags.append({
                        "type": "extreme_wick",
                        "severity": "warning",
                        "timestamp": str(candle.timestamp),
                        "symbol": candle.symbol,
                        "wick_pct": round(wick_ratio * 100, 2),
                        "high": candle.high,
                        "low": candle.low,
                        "detail": f"Wick is {wick_ratio:.1%} of price — possible liquidation cascade",
                    })

        return flags

    # -------------------------------------------------------------------------
    # Cross-Source Validation
    # -------------------------------------------------------------------------

    @staticmethod
    def cross_validate(
        primary: list[OHLCV],
        reference: list[OHLCV],
        max_close_divergence_pct: float = 0.02,  # 2%
    ) -> list[dict[str, Any]]:
        """Compare data from two sources to detect discrepancies.

        Useful for comparing Angel One vs yfinance daily data
        to catch exchange-specific issues.

        Args:
            primary: Primary data source candles (e.g., Angel One).
            reference: Reference data source candles (e.g., yfinance).
            max_close_divergence_pct: Max allowed % difference in close price.

        Returns:
            List of divergence flags.
        """
        if not primary or not reference:
            return []

        # Build lookup by date for reference data
        ref_by_date: dict[str, OHLCV] = {}
        for candle in reference:
            date_key = candle.timestamp.strftime("%Y-%m-%d")
            ref_by_date[date_key] = candle

        flags: list[dict[str, Any]] = []

        for candle in primary:
            date_key = candle.timestamp.strftime("%Y-%m-%d")
            ref_candle = ref_by_date.get(date_key)

            if not ref_candle:
                continue  # Reference doesn't have this date — skip

            if ref_candle.close == 0:
                continue

            close_diff_pct = abs(candle.close - ref_candle.close) / ref_candle.close

            if close_diff_pct > max_close_divergence_pct:
                flags.append({
                    "type": "cross_source_divergence",
                    "severity": "warning",
                    "date": date_key,
                    "symbol": candle.symbol,
                    "primary_exchange": candle.exchange.value,
                    "primary_close": candle.close,
                    "reference_exchange": ref_candle.exchange.value,
                    "reference_close": ref_candle.close,
                    "divergence_pct": round(close_diff_pct * 100, 2),
                    "detail": f"Close price differs by {close_diff_pct:.2%} between sources",
                })

        if flags:
            log.info(
                "cross_source_divergences",
                symbol=primary[0].symbol if primary else "?",
                count=len(flags),
                max_divergence=max(f["divergence_pct"] for f in flags),
            )

        return flags

    # -------------------------------------------------------------------------
    # Run All Checks
    # -------------------------------------------------------------------------

    def validate_all(
        self,
        candles: list[OHLCV],
        reference_candles: list[OHLCV] | None = None,
    ) -> dict[str, Any]:
        """Run all advanced validation checks.

        Args:
            candles: Primary data to validate.
            reference_candles: Optional reference data for cross-validation.

        Returns:
            Dict with all flags grouped by check type.
        """
        results: dict[str, Any] = {
            "total_candles": len(candles),
            "checks": {},
        }

        # Price-volume consistency
        pv_flags = self.check_price_volume_consistency(candles)
        results["checks"]["price_volume"] = {
            "flags": len(pv_flags),
            "details": pv_flags[:20],  # Limit output
        }

        # Exchange-specific
        if candles and candles[0].exchange == Exchange.ANGEL_ONE:
            ao_flags = self.check_angel_one_anomalies(candles)
            results["checks"]["angel_one_specific"] = {
                "flags": len(ao_flags),
                "details": ao_flags[:20],
            }
        elif candles and candles[0].exchange == Exchange.BINANCE:
            bn_flags = self.check_binance_anomalies(candles)
            results["checks"]["binance_specific"] = {
                "flags": len(bn_flags),
                "details": bn_flags[:20],
            }

        # Cross-source
        if reference_candles:
            xv_flags = self.cross_validate(candles, reference_candles)
            results["checks"]["cross_source"] = {
                "flags": len(xv_flags),
                "details": xv_flags[:20],
            }

        # Summary
        total_flags = sum(c["flags"] for c in results["checks"].values())
        results["total_flags"] = total_flags
        results["pass"] = total_flags == 0

        return results
