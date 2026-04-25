"""Feature Engineering Module - Phase 2.

Complete feature engineering pipeline for quantitative trading:
- Technical indicators (momentum, trend, volatility, volume)
- Feature store with point-in-time correctness
- Statistical features
- Cross-sectional features
"""

from .indicators.momentum import rsi, macd, stochastic
from .indicators.trend import sma, ema, wma
from .indicators.volatility import bollinger_bands, atr, keltner_channel
from .indicators.volume import obv, vwap, mfi
from .store import FeatureStore
from .stats import rolling_stats, correlation_matrix

__all__ = [
    # Momentum
    'rsi',
    'macd',
    'stochastic',
    # Trend
    'sma',
    'ema',
    'wma',
    # Volatility
    'bollinger_bands',
    'atr',
    'keltner_channel',
    # Volume
    'obv',
    'vwap',
    'mfi',
    # Store
    'FeatureStore',
    # Stats
    'rolling_stats',
    'correlation_matrix',
]
