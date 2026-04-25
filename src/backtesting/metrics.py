"""Quant Trading System — Performance Metrics.

Calculates advanced financial metrics like Sharpe, Sortino, Calmar, and Drawdown.
"""

import numpy as np
import pandas as pd
from typing import Dict, Any, List

def calculate_metrics(equity_curve: pd.Series, trades: List[Any], ann_factor: int = 252) -> Dict[str, Any]:
    """Calculate a comprehensive suite of performance metrics.
    
    ann_factor: 252 for Daily, 252*6.5 for Hourly, etc.
    """
    if equity_curve.empty:
        return {}

    returns = equity_curve.pct_change().dropna()
    total_return = (equity_curve.iloc[-1] / equity_curve.iloc[0]) - 1
    
    # CAGR
    years = len(equity_curve) / ann_factor
    cagr = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
    
    # Volatility
    vol = returns.std() * np.sqrt(ann_factor)
    
    # Sharpe Ratio (Assuming 0 risk-free rate)
    sharpe = (returns.mean() / returns.std() * np.sqrt(ann_factor)) if returns.std() > 0 else 0
    
    # Sortino Ratio (Downside deviation only)
    downside_returns = returns[returns < 0]
    downside_std = downside_returns.std() * np.sqrt(ann_factor)
    sortino = (returns.mean() / downside_returns.std() * np.sqrt(ann_factor)) if len(downside_returns) > 0 and downside_returns.std() > 0 else 0
    
    # Drawdown
    rolling_max = equity_curve.cummax()
    drawdown = (equity_curve - rolling_max) / rolling_max
    max_dd = drawdown.min()
    
    # Calmar Ratio
    calmar = (cagr / abs(max_dd)) if max_dd != 0 else 0
    
    # Trade-based metrics
    win_trades = [t for t in trades if t.pnl > 0]
    total_trades = len(trades)
    win_rate = (len(win_trades) / total_trades) if total_trades > 0 else 0
    
    avg_win = np.mean([t.pnl_pct for t in win_trades]) if win_trades else 0
    loss_trades = [t for t in trades if t.pnl <= 0]
    avg_loss = np.mean([t.pnl_pct for t in loss_trades]) if loss_trades else 0
    
    profit_factor = (sum([t.pnl for t in win_trades]) / abs(sum([t.pnl for t in loss_trades]))) if loss_trades and sum([t.pnl for t in loss_trades]) != 0 else 0

    return {
        "total_return_pct": total_return * 100,
        "cagr_pct": cagr * 100,
        "max_drawdown_pct": max_dd * 100,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "calmar_ratio": calmar,
        "win_rate_pct": win_rate * 100,
        "profit_factor": profit_factor,
        "total_trades": total_trades,
        "avg_win_pct": avg_win * 100,
        "avg_loss_pct": avg_loss * 100,
        "recovery_factor": (total_return / abs(max_dd)) if max_dd != 0 else 0
    }
