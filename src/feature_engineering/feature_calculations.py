"""Quant Trading System - Feature Engineering Module.

This module provides the foundation for Phase 2 feature engineering work.
It will contain components for:

1. Technical indicator calculations (e.g., moving averages, RSI, MACD, etc.)
2. Feature extraction and transformation from raw market data
3. Feature storage and retrieval mechanisms
4. Data quality and validation for features

The module will build upon the data service layer established in Phase 1.
"""

# Core dependencies for feature engineering
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

# Project specific imports
from src.core.logging import get_logger

log = get_logger(__name__)

class FeatureCalculator:
    """Base class for calculating technical indicators and features."""
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    @staticmethod
    def calculate_sma(data: pd.DataFrame, period: int = 20) -> pd.Series:
        """Calculate Simple Moving Average.
        
        Args:
            data: DataFrame with 'close' prices
            period: Window period for SMA calculation
            
        Returns:
            Series with SMA values
        """
        if 'close' not in data.columns:
            raise ValueError("DataFrame must contain 'close' column")
            
        return data['close'].rolling(window=period).mean()
    
    @staticmethod
    def calculate_returns(data: pd.DataFrame, period: int = 1) -> pd.Series:
        """Calculate returns.
        
        Args:
            data: DataFrame with 'close' prices
            period: Period for return calculation
            
        Returns:
            Series with return values
        """
        if 'close' not in data.columns:
            raise ValueError("DataFrame must contain 'close' column")
            
        return data['close'].pct_change(periods=period)
    
    @staticmethod
    def calculate_volatility(data: pd.DataFrame, period: int = 20) -> pd.Series:
        """Calculate rolling volatility (standard deviation of returns).
        
        Args:
            data: DataFrame with 'close' prices
            period: Window period for volatility calculation
            
        Returns:
            Series with volatility values
        """
        returns = data['close'].pct_change()
        return returns.rolling(window=period).std()
    
    @staticmethod
    def calculate_rsi(data: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index."""
        delta = data['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def calculate_ema(data: pd.DataFrame, period: int = 20) -> pd.Series:
        """Calculate Exponential Moving Average."""
        return data['close'].ewm(span=period, adjust=False).mean()

    @staticmethod
    def calculate_macd(
        data: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate MACD (Line, Signal, Histogram)."""
        ema_fast = data['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = data['close'].ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def calculate_bollinger_bands(
        data: pd.DataFrame, period: int = 20, std_dev: int = 2
    ) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate Bollinger Bands (Upper, Middle, Lower)."""
        sma = data['close'].rolling(window=period).mean()
        std = data['close'].rolling(window=period).std()
        upper = sma + (std * std_dev)
        lower = sma - (std * std_dev)
        return upper, sma, lower


class FeatureManager:
    """Orchestrates feature calculation and storage for the pipeline."""

    def __init__(self, db: Any, repository: Any | None = None):
        self._db = db
        self._repo = repository or __import__("src.feature_engineering.feature_storage", fromlist=["FeatureRepository"]).FeatureRepository(db)
        self._calc = FeatureCalculator()

    async def process_symbol(
        self,
        symbol: str,
        exchange: Any,
        timeframe: Any,
        data: pd.DataFrame,
    ) -> int:
        """Calculate and save all features for a dataset.

        Returns:
            Number of bars processed.
        """
        if data.empty or len(data) < 30:
            return 0

        # 1. Calculate features
        data = data.sort_values("timestamp")
        
        features_df = pd.DataFrame(index=data.index)
        features_df["rsi_14"] = self._calc.calculate_rsi(data, 14)
        features_df["sma_20"] = self._calc.calculate_sma(data, 20)
        features_df["ema_20"] = self._calc.calculate_ema(data, 20)
        features_df["returns_1"] = self._calc.calculate_returns(data, 1)
        
        macd, signal, _ = self._calc.calculate_macd(data)
        features_df["macd"] = macd
        features_df["macd_signal"] = signal
        
        upper, mid, lower = self._calc.calculate_bollinger_bands(data)
        features_df["bb_upper"] = upper
        features_df["bb_lower"] = lower

        # 2. Save most recent features (usually just the last complete bar)
        # In a real system, we might save all, but for Phase 2 we'll focus on the head
        latest_bar = data.iloc[-1]
        latest_features = features_df.iloc[-1].dropna().to_dict()

        if latest_features:
            await self._repo.save_features(
                timestamp=latest_bar["timestamp"],
                symbol=symbol,
                exchange=exchange,
                timeframe=timeframe,
                features=latest_features
            )
            return 1
        
        return 0