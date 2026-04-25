"""Volatility indicators: Bollinger Bands, ATR, Keltner Channel."""

import pandas as pd
import numpy as np
from typing import Tuple


def bollinger_bands(
    close: pd.Series,
    period: int = 20,
    num_std: float = 2.0
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Calculate Bollinger Bands.
    
    Args:
        close: Series of close prices
        period: Lookback period (default: 20)
        num_std: Number of standard deviations (default: 2)
    
    Returns:
        Tuple of (upper_band, middle_band, lower_band)
    """
    middle = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()
    upper = middle + (std * num_std)
    lower = middle - (std * num_std)
    return upper, middle, lower


def atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14
) -> pd.Series:
    """Calculate Average True Range (ATR).
    
    Args:
        high: Series of high prices
        low: Series of low prices
        close: Series of close prices
        period: Lookback period (default: 14)
    
    Returns:
        ATR values
    """
    tr1 = high - low
    tr2 = abs(high - close.shift())
    tr3 = abs(low - close.shift())
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_values = true_range.rolling(window=period).mean()
    return atr_values


def keltner_channel(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    ema_period: int = 20,
    atr_period: int = 10,
    multiplier: float = 2.0
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Calculate Keltner Channel.
    
    Args:
        high: Series of high prices
        low: Series of low prices
        close: Series of close prices
        ema_period: EMA period (default: 20)
        atr_period: ATR period (default: 10)
        multiplier: Channel multiplier (default: 2)
    
    Returns:
        Tuple of (upper, middle, lower)
    """
    middle = ema(close, ema_period)
    atr_value = atr(high, low, close, atr_period)
    upper = middle + (atr_value * multiplier)
    lower = middle - (atr_value * multiplier)
    return upper, middle, lower
