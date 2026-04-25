"""Volume indicators: OBV, VWAP, MFI."""

import pandas as pd
import numpy as np


def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    """Calculate On-Balance Volume (OBV).
    
    Args:
        close: Series of close prices
        volume: Series of volume
    
    Returns:
        OBV values
    """
    direction = close.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    obv_values = (volume * direction).cumsum()
    return obv_values


def vwap(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series
) -> pd.Series:
    """Calculate Volume Weighted Average Price (VWAP).
    
    Args:
        high: Series of high prices
        low: Series of low prices
        close: Series of close prices
        volume: Series of volume
    
    Returns:
        VWAP values
    """
    typical_price = (high + low + close) / 3
    vwap_values = (typical_price * volume).cumsum() / volume.cumsum()
    return vwap_values


def mfi(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    period: int = 14
) -> pd.Series:
    """Calculate Money Flow Index (MFI).
    
    Args:
        high: Series of high prices
        low: Series of low prices
        close: Series of close prices
        volume: Series of volume
        period: Lookback period (default: 14)
    
    Returns:
        MFI values (0-100)
    """
    typical_price = (high + low + close) / 3
    money_flow = typical_price * volume
    
    direction = typical_price.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    money_flow['direction'] = direction * money_flow
    
    positive_flow = money_flow['direction'].where(money_flow['direction'] > 0, 0)
    negative_flow = -money_flow['direction'].where(money_flow['direction'] < 0, 0)
    
    positive_mf = positive_flow.rolling(window=period).sum()
    negative_mf = negative_flow.rolling(window=period).sum()
    
    mf_ratio = positive_mf / negative_mf
    mfi_values = 100 - (100 / (1 + mf_ratio))
    return mfi_values
