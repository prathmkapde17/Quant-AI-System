"""Quant Trading System — Data Cleaner.

Handles gap filling, outlier capping, and quality scoring for OHLCV data.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np

from src.core.config import get_settings
from src.core.enums import DataQuality, Exchange, Timeframe
from src.core.logging import get_logger
from src.core.models import OHLCV

log = get_logger(__name__)


class OHLCVCleaner:
    """Cleans OHLCV data: fills gaps, caps outliers, assigns quality flags.

    Cleaning pipeline:
    1. Fill small gaps (< N candles) via forward-fill
    2. Detect and cap statistical outliers (z-score based)
    3. Assign DataQuality enum to each record
    """

    def __init__(self):
        settings = get_settings()
        self._cfg = settings.cleaning
        self._max_gap = self._cfg.max_gap_candles_to_fill
        self._zscore_threshold = self._cfg.outlier_zscore_threshold

    def clean(
        self,
        candles: list[OHLCV],
        timeframe: Timeframe,
    ) -> list[OHLCV]:
        """Run the full cleaning pipeline on a list of candles.

        Args:
            candles: Sorted list of OHLCV objects (ascending by timestamp).
            timeframe: Expected candle timeframe.

        Returns:
            Cleaned list of OHLCV objects with updated quality flags.
        """
        if len(candles) < 2:
            return candles

        # Step 1: Fill gaps
        filled = self._fill_gaps(candles, timeframe)

        # Step 2: Cap outliers
        cleaned = self._cap_outliers(filled)

        # Step 3: Mark clean records
        result = self._assign_quality(cleaned)

        log.info(
            "cleaning_complete",
            input_count=len(candles),
            output_count=len(result),
            interpolated=sum(1 for c in result if c.quality == DataQuality.INTERPOLATED),
            suspicious=sum(1 for c in result if c.quality == DataQuality.SUSPICIOUS),
            clean=sum(1 for c in result if c.quality == DataQuality.CLEAN),
        )

        return result

    # -------------------------------------------------------------------------
    # Gap Filling
    # -------------------------------------------------------------------------

    def _fill_gaps(
        self,
        candles: list[OHLCV],
        timeframe: Timeframe,
    ) -> list[OHLCV]:
        """Fill small gaps with forward-filled candles.

        Gaps larger than max_gap_candles_to_fill are left as-is.

        Args:
            candles: Sorted OHLCV list.
            timeframe: Expected interval.

        Returns:
            List with interpolated candles inserted into gaps.
        """
        if not candles:
            return candles

        interval = timedelta(minutes=timeframe.minutes)
        result: list[OHLCV] = [candles[0]]
        gaps_filled = 0

        for i in range(1, len(candles)):
            prev = candles[i - 1]
            curr = candles[i]

            gap_duration = curr.timestamp - prev.timestamp
            expected_candles = int(gap_duration / interval) - 1

            # Fill small gaps
            if 0 < expected_candles <= self._max_gap:
                for j in range(1, expected_candles + 1):
                    fill_ts = prev.timestamp + interval * j

                    # Skip market closures for equity
                    if prev.exchange == Exchange.ANGEL_ONE:
                        from src.cleaning.validator import OHLCVValidator
                        if OHLCVValidator._is_market_closure(prev.timestamp, fill_ts):
                            continue

                    # Forward-fill: use previous close for all OHLC, volume = 0
                    filled_candle = OHLCV(
                        timestamp=fill_ts,
                        symbol=prev.symbol,
                        exchange=prev.exchange,
                        timeframe=prev.timeframe,
                        open=prev.close,
                        high=prev.close,
                        low=prev.close,
                        close=prev.close,
                        volume=0,
                        quality=DataQuality.INTERPOLATED,
                    )
                    result.append(filled_candle)
                    gaps_filled += 1

            result.append(curr)

        if gaps_filled > 0:
            log.debug("gaps_filled", count=gaps_filled)

        return result

    # -------------------------------------------------------------------------
    # Outlier Capping
    # -------------------------------------------------------------------------

    def _cap_outliers(self, candles: list[OHLCV]) -> list[OHLCV]:
        """Detect and cap statistical outliers based on log-return z-scores.

        Records with |z-score| > threshold are capped at the boundary
        and flagged as SUSPICIOUS.

        Args:
            candles: Sorted OHLCV list.

        Returns:
            List with outlier prices capped.
        """
        if len(candles) < 20:
            return candles  # Need enough data for meaningful statistics

        # Compute log returns
        closes = [c.close for c in candles]
        log_returns = []
        for i in range(1, len(closes)):
            if closes[i - 1] > 0 and closes[i] > 0:
                log_returns.append(math.log(closes[i] / closes[i - 1]))
            else:
                log_returns.append(0.0)

        if not log_returns:
            return candles

        # Compute rolling stats (use all returns for simplicity)
        returns_arr = np.array(log_returns)
        mean_ret = float(np.mean(returns_arr))
        std_ret = float(np.std(returns_arr))

        if std_ret == 0:
            return candles

        result: list[OHLCV] = [candles[0]]

        for i in range(1, len(candles)):
            candle = candles[i]
            log_ret = log_returns[i - 1]
            z_score = (log_ret - mean_ret) / std_ret

            if abs(z_score) > self._zscore_threshold:
                # Cap at threshold boundary
                capped_ret = mean_ret + (self._zscore_threshold * std_ret * (1 if z_score > 0 else -1))
                capped_close = candles[i - 1].close * math.exp(capped_ret)

                # Reconstruct candle with capped prices
                capped_candle = OHLCV(
                    timestamp=candle.timestamp,
                    symbol=candle.symbol,
                    exchange=candle.exchange,
                    timeframe=candle.timeframe,
                    open=candle.open,
                    high=min(candle.high, max(candle.open, capped_close) * 1.001),
                    low=max(candle.low, min(candle.open, capped_close) * 0.999),
                    close=capped_close,
                    volume=candle.volume,
                    turnover=candle.turnover,
                    num_trades=candle.num_trades,
                    quality=DataQuality.SUSPICIOUS,
                )
                result.append(capped_candle)

                log.debug(
                    "outlier_capped",
                    symbol=candle.symbol,
                    timestamp=str(candle.timestamp),
                    z_score=round(z_score, 2),
                    original_close=candle.close,
                    capped_close=round(capped_close, 4),
                )
            else:
                result.append(candle)

        return result

    # -------------------------------------------------------------------------
    # Quality Assignment
    # -------------------------------------------------------------------------

    @staticmethod
    def _assign_quality(candles: list[OHLCV]) -> list[OHLCV]:
        """Assign DataQuality.CLEAN to records that haven't been flagged.

        Records already marked as INTERPOLATED or SUSPICIOUS keep their flag.

        Args:
            candles: OHLCV list (some may already have quality flags).

        Returns:
            List with quality flags finalized.
        """
        result: list[OHLCV] = []

        for candle in candles:
            if candle.quality in (DataQuality.INTERPOLATED, DataQuality.SUSPICIOUS):
                result.append(candle)
            else:
                # Mark as clean (create new instance since models are frozen)
                result.append(OHLCV(
                    timestamp=candle.timestamp,
                    symbol=candle.symbol,
                    exchange=candle.exchange,
                    timeframe=candle.timeframe,
                    open=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    volume=candle.volume,
                    turnover=candle.turnover,
                    num_trades=candle.num_trades,
                    quality=DataQuality.CLEAN,
                ))

        return result
