"""Statistical features: rolling statistics, correlation, PCA."""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional


def rolling_stats(
    data: pd.Series,
    window: int = 20
) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    """Calculate rolling statistics.
    
    Args:
        data: Series of data
        window: Rolling window size
    
    Returns:
        Tuple of (mean, std, skew, kurtosis)
    """
    mean = data.rolling(window=window).mean()
    std = data.rolling(window=window).std()
    skew = data.rolling(window=window).skew()
    kurt = data.rolling(window=window).kurt()
    return mean, std, skew, kurt


def correlation_matrix(
    data: pd.DataFrame,
    window: Optional[int] = None
) -> pd.DataFrame:
    """Calculate correlation matrix.
    
    Args:
        data: DataFrame with multiple columns
        window: Optional rolling window size
    
    Returns:
        Correlation matrix
    """
    if window:
        return data.rolling(window=window).corr()
    else:
        return data.corr()


def rolling_correlation(
    x: pd.Series,
    y: pd.Series,
    window: int = 20
) -> pd.Series:
    """Calculate rolling correlation between two series.
    
    Args:
        x: First series
        y: Second series
        window: Rolling window size
    
    Returns:
        Rolling correlation series
    """
    df = pd.concat([x, y], axis=1)
    return df.rolling(window=window).corr().iloc[:, 0].iloc[1::2].reset_index(drop=True)


def zscore(
    data: pd.Series,
    window: Optional[int] = None
) -> pd.Series:
    """Calculate z-score.
    
    Args:
        data: Series of data
        window: Optional rolling window
    
    Returns:
        Z-score series
    """
    if window:
        mean = data.rolling(window=window).mean()
        std = data.rolling(window=window).std()
    else:
        mean = data.mean()
        std = data.std()
    
    return (data - mean) / std


def percentile_rank(
    data: pd.Series,
    window: Optional[int] = None
) -> pd.Series:
    """Calculate percentile rank.
    
    Args:
        data: Series of data
        window: Optional rolling window
    
    Returns:
        Percentile rank series
    """
    if window:
        def rank_func(x):
            return pd.Series(x).rank(pct=True).iloc[-1]
        return data.rolling(window=window).apply(rank_func, raw=False)
    else:
        return data.rank(pct=True)
