"""Quant Trading System — Signal Generators.

Provides strategy logic that converts price/features into trade signals.
"""

import pandas as pd
from typing import Dict, Any

def rsi_strategy(df: pd.DataFrame, params: Dict[str, Any]) -> pd.Series:
    """RSI Mean Reversion Strategy."""
    oversold = params.get("oversold", 30)
    overbought = params.get("overbought", 70)
    
    rsi_col = 'rsi_14' if 'rsi_14' in df.columns else None
    if not rsi_col:
        # Fallback search
        rsi_col = next((c for c in df.columns if 'rsi' in c.lower()), None)
        
    if not rsi_col:
        return pd.Series(0, index=df.index)
        
    signals = pd.Series(0, index=df.index)
    signals[df[rsi_col] < oversold] = 1   # Long
    signals[df[rsi_col] > overbought] = -1 # Short
    
    return signals

def trend_following_ema(df: pd.DataFrame, params: Dict[str, Any]) -> pd.Series:
    """EMA Cross Strategy."""
    fast_ema = params.get("fast", 20)
    slow_ema = params.get("slow", 50)
    
    # Assuming these columns exist in features
    fast_col = f'ema_{fast_ema}'
    slow_col = f'ema_{slow_ema}'
    
    if fast_col not in df.columns or slow_col not in df.columns:
        return pd.Series(0, index=df.index)
        
    signals = pd.Series(0, index=df.index)
    signals[df[fast_col] > df[slow_col]] = 1
    signals[df[fast_col] < df[slow_col]] = -1
    
    return signals

def macd_strategy(df: pd.DataFrame, params: Dict[str, Any]) -> pd.Series:
    """MACD Crossover Strategy."""
    if 'macd_main' not in df.columns or 'macd_signal' not in df.columns:
        return pd.Series(0, index=df.index)
        
    signals = pd.Series(0, index=df.index)
    signals[df['macd_main'] > df['macd_signal']] = 1
    signals[df['macd_main'] < df['macd_signal']] = -1
    
    return signals
