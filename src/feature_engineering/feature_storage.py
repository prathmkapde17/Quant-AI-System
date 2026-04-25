"""Quant Trading System — Feature Storage.

Handles persistent storage and retrieval of technical indicators and alpha signals
in TimescaleDB.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from src.core.enums import Exchange, Timeframe
from src.core.logging import get_logger
from src.storage.database import Database

log = get_logger(__name__)


class FeatureRepository:
    """Repository for managing 'features' hypertable."""

    def __init__(self, db: Database):
        self._db = db

    async def save_features(
        self,
        timestamp: datetime,
        symbol: str,
        exchange: Exchange,
        timeframe: Timeframe,
        features: dict[str, float],
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Save a dictionary of features for a specific bar.

        Args:
            timestamp: Bar timestamp.
            symbol: Instrument symbol.
            exchange: Exchange enum.
            timeframe: Timeframe enum.
            features: Dict of {feature_name: value}.
            metadata: Optional params metadata.
        """
        if not features:
            return False

        query = """
            INSERT INTO features (timestamp, symbol, exchange, timeframe, feature_name, feature_value, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (timestamp, symbol, exchange, timeframe, feature_name)
            DO UPDATE SET feature_value = EXCLUDED.feature_value, metadata = EXCLUDED.metadata
        """
        
        # Prepare batch insert data
        meta_json = json.dumps(metadata) if metadata else None
        data = [
            (timestamp, symbol, exchange.value, timeframe.value, name, float(val), meta_json)
            for name, val in features.items() if val is not None and not (isinstance(val, float) and (val != val)) # skip NaN
        ]

        if not data:
            return False

        try:
            await self._db.execute_many(query, data)
            return True
        except Exception as e:
            log.error("save_features_error", symbol=symbol, error=str(e))
            return False

    async def get_latest_features(
        self,
        symbol: str,
        exchange: Exchange,
        timeframe: Timeframe,
        limit: int = 1,
    ) -> dict[str, Any]:
        """Get the most recent features for a symbol.

        Returns:
            Dict mapping feature_name -> value.
        """
        query = """
            SELECT timestamp, feature_name, feature_value
            FROM features
            WHERE symbol = $1 AND exchange = $2 AND timeframe = $3
            ORDER BY timestamp DESC
            LIMIT $4 * 20 -- Assume ~20 features per timestamp
        """
        rows = await self._db.fetch(query, symbol, exchange.value, timeframe.value, limit)
        
        result = {}
        for row in rows:
            ts = row["timestamp"]
            if ts not in result:
                result[ts] = {}
            result[ts][row["feature_name"]] = row["feature_value"]
            
        return result