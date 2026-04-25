"""Quant Trading System — Corporate Action Adjuster.

Adjusts historical prices for stock splits, bonuses, and dividends
to produce backward-adjusted series for accurate backtesting.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.core.enums import Exchange
from src.core.logging import get_logger
from src.core.models import OHLCV

log = get_logger(__name__)


class CorporateActionAdjuster:
    """Adjusts historical OHLCV data for corporate actions.

    Actions handled:
    - Stock splits (e.g., 1:5 split → divide old prices by 5)
    - Bonus issues (e.g., 1:1 bonus → divide old prices by 2)
    - Dividends (optional — subtract dividend from price, less common)

    The adjuster applies backward adjustment: it modifies historical prices
    to make them comparable with the current price. This is the standard
    approach for backtesting.
    """

    def __init__(self):
        # In-memory cache of corporate actions
        # Structure: {symbol: [{"date": datetime, "type": str, "ratio": float}, ...]}
        self._actions: dict[str, list[dict[str, Any]]] = {}

    def load_actions(self, actions: dict[str, list[dict[str, Any]]]) -> None:
        """Load corporate actions data.

        In production, this would be fetched from:
        - Angel One instruments API
        - BSE/NSE corporate action feeds
        - Manual CSV uploads

        Args:
            actions: Dict of {symbol: [action_dicts]}.
                     Each action_dict: {"date": datetime, "type": "split"|"bonus"|"dividend",
                                        "ratio": float, "value": float (for dividends)}
        """
        self._actions = actions
        total = sum(len(v) for v in actions.values())
        log.info("corporate_actions_loaded", symbols=len(actions), total_actions=total)

    def adjust(
        self,
        candles: list[OHLCV],
        symbol: str,
    ) -> list[OHLCV]:
        """Apply backward adjustment to historical candles.

        Prices before a corporate action are divided by the adjustment ratio.
        For example, a 1:2 stock split gives ratio=2, so all prices before
        the split date are divided by 2.

        Args:
            candles: Sorted list of OHLCV (ascending by timestamp).
            symbol: Instrument symbol.

        Returns:
            Adjusted list of OHLCV objects.
        """
        actions = self._actions.get(symbol, [])
        if not actions:
            return candles

        # Sort actions by date (most recent first for backward adjustment)
        sorted_actions = sorted(actions, key=lambda a: a["date"], reverse=True)

        result: list[OHLCV] = []

        for candle in candles:
            adjusted = candle
            cumulative_ratio = 1.0

            for action in sorted_actions:
                action_date = action["date"]
                if isinstance(action_date, str):
                    action_date = datetime.fromisoformat(action_date)
                if action_date.tzinfo is None:
                    action_date = action_date.replace(tzinfo=timezone.utc)

                if candle.timestamp < action_date:
                    action_type = action.get("type", "")
                    ratio = action.get("ratio", 1.0)

                    if action_type in ("split", "bonus"):
                        cumulative_ratio *= ratio
                    elif action_type == "dividend":
                        # For dividend adjustment, subtract the dividend from the price
                        div_value = action.get("value", 0)
                        if div_value > 0 and adjusted.close > 0:
                            div_ratio = 1 - (div_value / adjusted.close)
                            cumulative_ratio *= div_ratio

            if cumulative_ratio != 1.0 and cumulative_ratio > 0:
                adjusted = OHLCV(
                    timestamp=candle.timestamp,
                    symbol=candle.symbol,
                    exchange=candle.exchange,
                    timeframe=candle.timeframe,
                    open=round(candle.open / cumulative_ratio, 4),
                    high=round(candle.high / cumulative_ratio, 4),
                    low=round(candle.low / cumulative_ratio, 4),
                    close=round(candle.close / cumulative_ratio, 4),
                    volume=round(candle.volume * cumulative_ratio, 0),  # Adjust volume inversely
                    turnover=candle.turnover,
                    num_trades=candle.num_trades,
                    quality=candle.quality,
                )

            result.append(adjusted)

        adjusted_count = sum(
            1 for orig, adj in zip(candles, result) if orig.close != adj.close
        )
        if adjusted_count > 0:
            log.info(
                "corporate_action_adjustment",
                symbol=symbol,
                adjusted_records=adjusted_count,
                total_records=len(candles),
                actions_applied=len(sorted_actions),
            )

        return result

    def get_adjustment_ratio(
        self,
        symbol: str,
        from_date: datetime,
        to_date: datetime | None = None,
    ) -> float:
        """Calculate the cumulative adjustment ratio between two dates.

        Useful for converting a raw historical price to an adjusted price.

        Args:
            symbol: Instrument symbol.
            from_date: Historical date.
            to_date: Reference date (defaults to now).

        Returns:
            Cumulative ratio to divide the historical price by.
        """
        if to_date is None:
            to_date = datetime.now(timezone.utc)

        actions = self._actions.get(symbol, [])
        ratio = 1.0

        for action in actions:
            action_date = action["date"]
            if isinstance(action_date, str):
                action_date = datetime.fromisoformat(action_date)
            if action_date.tzinfo is None:
                action_date = action_date.replace(tzinfo=timezone.utc)

            if from_date < action_date <= to_date:
                action_type = action.get("type", "")
                if action_type in ("split", "bonus"):
                    ratio *= action.get("ratio", 1.0)

        return ratio
