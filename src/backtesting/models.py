"""Quant Trading System — Backtesting Models.

Defines the data structures for Trades, Signals, and Positions.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from src.core.enums import Exchange, Timeframe

class Signal(BaseModel):
    timestamp: datetime
    symbol: str
    direction: int  # 1: Long, -1: Short, 0: Flat
    price: float
    metadata: Dict[str, Any] = {}

class Trade(BaseModel):
    symbol: str
    entry_time: datetime
    entry_price: float
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    direction: int
    size: float
    commission: float = 0.0
    slippage: float = 0.0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    metadata: Dict[str, Any] = {}

class BacktestWindow(BaseModel):
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime
    params: Dict[str, Any]
    metrics: Dict[str, Any]
