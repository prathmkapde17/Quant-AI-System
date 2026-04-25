"""Trend indicators: SMA, EMA, WMA."""

import pandas as pd
import numpy as np


def sma(close: pd.Series, period: int = 20) -> pd.Series:
    """Calculate Simple Moving Average (SMA).
    
    Args:
        close: Series of close prices
        period: Lookback period (default: 20)
    
    Returns:
        SMA values
    """
    return close.rolling(window=period).mean()


def ema(close: pd.Series, period: int = 20) -> pd.Series:
    """Calculate Exponential Moving Average (EMA).
    
    Args:
        close: Series of close prices
        period: Lookback period (default: 20)
    
    Returns:
        EMA values
    """
    return close.ewm(span=period, adjust=False).mean()


def wma(close: pd.Series, period: int = 20) -> pd.Series:
    """Calculate Weighted Moving Average (WMA).
    
    Args:
        close: Series of close prices
        period: Lookback period (default: 20)
    
    Returns:
        WMA values
    """
    weights = np.arange(1, period + 1)
    
    def weighted_avg(window):
        return np.sum(window * weights) / np.sum(weights)
    
    return close.rolling(window=period).apply(weighted_avg, raw=True)
