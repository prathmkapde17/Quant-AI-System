"""Quant Trading System — Backtest Data Repository.

Specialized repository for joining price data and pre-computed features.
"""

from __future__ import annotations
from datetime import datetime
from typing import Any, List
import pandas as pd
import asyncpg

from src.core.enums import Exchange, Timeframe
from src.core.exceptions import DatabaseReadError
from src.core.logging import get_logger
from src.storage.database import Database

log = get_logger(__name__)

class BacktestRepository:
    """Handles retrieval of joined OHLCV and Feature data for backtesting."""

    def __init__(self, db: Database):
        self._db = db

    async def get_backtest_data(
        self,
        symbol: str,
        exchange: Exchange,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        """Fetch OHLCV data joined with all available features for the period.
        
        Returns a pivoted DataFrame where each feature is a column.
        """
        # 1. Fetch OHLCV
        ohlcv_query = """
            SELECT timestamp, open, high, low, close, volume
            FROM ohlcv
            WHERE symbol = $1 AND exchange = $2 AND timeframe = $3
              AND timestamp >= $4 AND timestamp <= $5
            ORDER BY timestamp ASC
        """
        
        # 2. Fetch Features
        features_query = """
            SELECT timestamp, feature_name, feature_value
            FROM features
            WHERE symbol = $1 AND exchange = $2 AND timeframe = $3
              AND timestamp >= $4 AND timestamp <= $5
        """
        
        try:
            ohlcv_rows = await self._db.fetch(ohlcv_query, symbol, exchange.value, timeframe.value, start, end)
            feature_rows = await self._db.fetch(features_query, symbol, exchange.value, timeframe.value, start, end)
            
            if not ohlcv_rows:
                return pd.DataFrame()
            
            # Convert to DataFrames
            df_ohlcv = pd.DataFrame([dict(r) for r in ohlcv_rows])
            df_ohlcv.set_index('timestamp', inplace=True)
            
            if feature_rows:
                df_features = pd.DataFrame([dict(r) for r in feature_rows])
                # Pivot features so each feature_name becomes a column
                df_features_pivot = df_features.pivot(index='timestamp', columns='feature_name', values='feature_value')
                
                # Merge
                df_combined = df_ohlcv.join(df_features_pivot, how='left')
            else:
                df_combined = df_ohlcv
                
            return df_combined.fillna(method='ffill')
            
        except asyncpg.PostgresError as e:
            raise DatabaseReadError(f"Failed to fetch backtest data: {e}")
