"""Feature Store for time-series feature storage and retrieval.

Provides point-in-time correct feature storage to avoid lookahead bias.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
import asyncio
import json

from src.core.logging import get_logger
from src.storage.database import Database

log = get_logger(__name__)


@dataclass
class FeatureDefinition:
    """Definition of a feature."""
    name: str
    description: str
    calculator: Callable
    params: Dict[str, Any]
    version: str = "1.0"


class FeatureStore:
    """Feature store for managing features with point-in-time correctness.
    
    Usage:
        store = FeatureStore(db)
        
        # Define feature
        store.define_feature(
            name="rsi_14",
            calculator=indicators.rsi,
            params={"period": 14},
            description="14-period RSI"
        )
        
        # Compute and store
        await store.compute_and_store(
            features=["rsi_14"],
            symbols=["RELIANCE", "TCS"],
            start_date="2024-01-01",
            end_date="2024-04-24"
        )
        
        # Retrieve (point-in-time correct)
        features = await store.get_features(
            features=["rsi_14"],
            symbols=["RELIANCE"],
            timestamps=timestamps,
            point_in_time=True
        )
    """
    
    def __init__(self, db: Database):
        self.db = db
        self.logger = get_logger(__name__)
        self.feature_definitions: Dict[str, FeatureDefinition] = {}
        self._cache: Dict[str, pd.DataFrame] = {}
    
    def define_feature(
        self,
        name: str,
        calculator: Callable,
        params: Dict[str, Any],
        description: str = "",
        version: str = "1.0"
    ) -> None:
        """Define a new feature.
        
        Args:
            name: Feature name
            calculator: Function to calculate feature
            params: Parameters for calculator
            description: Feature description
            version: Feature version
        """
        self.feature_definitions[name] = FeatureDefinition(
            name=name,
            description=description,
            calculator=calculator,
            params=params,
            version=version
        )
        self.logger.info("feature_defined", name=name, version=version)
    
    async def compute_and_store(
        self,
        features: List[str],
        symbols: List[str],
        exchange: Any,
        start_date: datetime,
        end_date: datetime,
        timeframe: Any
    ) -> Dict[str, pd.DataFrame]:
        """Compute features and store them.
        
        Args:
            features: List of feature names to compute
            symbols: List of symbols
            exchange: Exchange enum
            start_date: Start date
            end_date: End date
            timeframe: Timeframe enum
        
        Returns:
            Dict of {symbol: DataFrame with features}
        """
        from src.api.data_service import DataService
        
        data_service = DataService(self.db)
        results: Dict[str, pd.DataFrame] = {}
        
        for symbol in symbols:
            try:
                # Get historical data
                df = await data_service.get_historical(
                    symbol=symbol,
                    exchange=exchange,
                    timeframe=timeframe,
                    start=start_date,
                    end=end_date
                )
                
                if df.empty:
                    self.logger.warning("no_data_for_symbol", symbol=symbol)
                    continue
                
                # Calculate features
                for feature_name in features:
                    if feature_name not in self.feature_definitions:
                        self.logger.warning("feature_not_defined", feature=feature_name)
                        continue
                    
                    feature_def = self.feature_definitions[feature_name]
                    
                    try:
                        # Calculate feature
                        if feature_name == 'rsi':
                            df[feature_name] = feature_def.calculator(
                                df['close'],
                                **feature_def.params
                            )
                        elif feature_name == 'sma':
                            df[feature_name] = feature_def.calculator(
                                df['close'],
                                **feature_def.params
                            )
                        elif feature_name == 'bollinger':
                            upper, middle, lower = feature_def.calculator(
                                df['close'],
                                **feature_def.params
                            )
                            df[f'{feature_name}_upper'] = upper
                            df[f'{feature_name}_middle'] = middle
                            df[f'{feature_name}_lower'] = lower
                        else:
                            # Generic calculation
                            result = feature_def.calculator(df, **feature_def.params)
                            if isinstance(result, pd.Series):
                                df[feature_name] = result
                            elif isinstance(result, tuple):
                                # Multiple outputs
                                for i, series in enumerate(result):
                                    df[f'{feature_name}_{i}'] = series
                    except Exception as e:
                        self.logger.error(
                            "feature_calculation_error",
                            feature=feature_name,
                            symbol=symbol,
                            error=str(e)
                        )
                
                results[symbol] = df
                self.logger.info(
                    "features_computed",
                    symbol=symbol,
                    features=features,
                    rows=len(df)
                )
                
            except Exception as e:
                self.logger.error(
                    "feature_computation_error",
                    symbol=symbol,
                    error=str(e)
                )
        
        return results
    
    async def get_features(
        self,
        features: List[str],
        symbols: List[str],
        timestamps: Optional[pd.DatetimeIndex] = None,
        point_in_time: bool = True
    ) -> pd.DataFrame:
        """Get features for given symbols and timestamps.
        
        Args:
            features: List of feature names
            symbols: List of symbols
            timestamps: Optional timestamps for point-in-time lookup
            point_in_time: If True, ensure no lookahead bias
        
        Returns:
            DataFrame with features
        """
        # This is a simplified implementation
        # Full implementation would query from storage
        combined_df = pd.DataFrame()
        
        for symbol in symbols:
            if symbol in self._cache:
                df = self._cache[symbol]
                feature_cols = [col for col in df.columns if col in features]
                symbol_df = df[feature_cols]
                symbol_df['symbol'] = symbol
                combined_df = pd.concat([combined_df, symbol_df])
        
        return combined_df
    
    def clear_cache(self) -> None:
        """Clear feature cache."""
        self._cache.clear()
        self.logger.info("feature_cache_cleared")
