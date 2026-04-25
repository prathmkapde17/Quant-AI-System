"""Quant Trading System — Vectorized Backtesting Engine.

Designed for speed and integration with the Phase 2 Feature Store.
Calculates performance metrics using Pandas/NumPy vectorization.
"""

import numpy as np
import pandas as pd
from typing import Any, Dict, List
from pydantic import BaseModel

class BacktestResult(BaseModel):
    """Container for backtest performance metrics."""
    total_return_pct: float
    annualized_return_pct: float
    max_drawdown_pct: float
    sharpe_ratio: float
    win_rate_pct: float
    total_trades: int
    equity_curve: List[float]
    trades: List[Dict[str, Any]]

class VectorizedEngine:
    """High-speed engine for backtesting strategies on historical data."""

    def __init__(self, initial_capital: float = 10000.0, commission_pct: float = 0.0004):
        self.initial_capital = initial_capital
        self.commission_pct = commission_pct

    def run(self, df: pd.DataFrame, signals: pd.Series) -> BacktestResult:
        """Run backtest on a DataFrame with a pre-calculated signals series.
        
        Signals: 1 for Long, -1 for Short, 0 for Neutral.
        """
        # Ensure we have a copy to avoid side effects
        data = df.copy()
        data['signal'] = signals.shift(1).fillna(0)  # Shift to avoid look-ahead bias
        
        # Calculate returns
        data['market_return'] = data['close'].pct_change().fillna(0)
        data['strategy_return'] = data['market_return'] * data['signal']
        
        # Apply commission on signal changes
        data['signal_change'] = data['signal'].diff().abs().fillna(0)
        data['costs'] = data['signal_change'] * self.commission_pct
        data['net_return'] = data['strategy_return'] - data['costs']
        
        # Equity curve
        data['cumulative_return'] = (1 + data['net_return']).cumprod()
        data['equity'] = self.initial_capital * data['cumulative_return']
        
        # Metrics Calculation
        total_return = (data['equity'].iloc[-1] / self.initial_capital) - 1
        
        # Annualization factor (assuming 1m data)
        # 60 mins * 24 hours * 365 days = 525,600
        ann_factor = 525600 / len(data) if len(data) > 0 else 1
        ann_return = ((1 + total_return) ** ann_factor) - 1
        
        # Drawdown
        data['rolling_max'] = data['equity'].cummax()
        data['drawdown'] = (data['equity'] - data['rolling_max']) / data['rolling_max']
        max_dd = data['drawdown'].min()
        
        # Sharpe Ratio (assuming 0 risk-free rate for simplicity)
        std = data['net_return'].std()
        sharpe = (data['net_return'].mean() / std * np.sqrt(525600)) if std > 0 else 0
        
        # Trade Analysis
        trades = []
        trade_signals = data[data['signal_change'] != 0]
        win_count = 0
        
        # Simple win rate calculation based on strategy return chunks
        # In a real engine we'd track entries/exits more precisely
        wins = data[data['strategy_return'] > 0]
        losses = data[data['strategy_return'] < 0]
        win_rate = (len(wins) / (len(wins) + len(losses))) * 100 if (len(wins) + len(losses)) > 0 else 0

        return BacktestResult(
            total_return_pct=total_return * 100,
            annualized_return_pct=ann_return * 100,
            max_drawdown_pct=max_dd * 100,
            sharpe_ratio=sharpe,
            win_rate_pct=win_rate,
            total_trades=int(data['signal_change'].sum()),
            equity_curve=data['equity'].tolist(),
            trades=[] # Placeholder for detailed trade list
        )
