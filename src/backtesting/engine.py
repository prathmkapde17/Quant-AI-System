"""Quant Trading System — Walk Forward Backtester.

The core engine that orchestrates single-run and walk-forward simulations.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Callable, Optional
from datetime import datetime

from src.backtesting.models import Signal, Trade, BacktestWindow
from src.backtesting.metrics import calculate_metrics
from src.core.logging import get_logger

log = get_logger(__name__)

class WalkForwardBacktester:
    """Orchestrates complex backtesting scenarios including Walk-Forward analysis."""

    def __init__(
        self, 
        initial_capital: float = 10000.0,
        commission_pct: float = 0.0004,
        slippage_pct: float = 0.0001
    ):
        self.initial_capital = initial_capital
        self.commission_pct = commission_pct
        self.slippage_pct = slippage_pct

    def run_single_backtest(
        self, 
        df: pd.DataFrame, 
        strategy_func: Callable[[pd.DataFrame, Dict[str, Any]], pd.Series],
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a single-period backtest."""
        # 1. Generate Signals
        signals = strategy_func(df, params)
        
        # 2. Execute Simulation
        return self._execute_simulation(df, signals)

    def _execute_simulation(self, df: pd.DataFrame, signals: pd.Series) -> Dict[str, Any]:
        """Core simulation loop with realistic fills."""
        equity = self.initial_capital
        position: Optional[Trade] = None
        trades: List[Trade] = []
        equity_curve = [equity]
        
        # We use a loop for high-fidelity execution simulation
        # signals is a series of 1, -1, 0
        
        for i in range(1, len(df)):
            curr_price = df['close'].iloc[i]
            curr_time = df.index[i]
            signal = signals.iloc[i-1] # Use previous bar signal for next bar open fill
            
            # 1. Exit Logic
            if position:
                # Check for exit signal
                should_exit = (position.direction == 1 and signal <= 0) or \
                             (position.direction == -1 and signal >= 0)
                
                if should_exit:
                    # Apply slippage and commission
                    exit_price = curr_price * (1 - self.slippage_pct if position.direction == 1 else 1 + self.slippage_pct)
                    comm = exit_price * position.size * self.commission_pct
                    
                    position.exit_time = curr_time
                    position.exit_price = exit_price
                    position.commission += comm
                    
                    # Calculate PnL
                    if position.direction == 1:
                        position.pnl = (exit_price - position.entry_price) * position.size - position.commission
                    else:
                        position.pnl = (position.entry_price - exit_price) * position.size - position.commission
                        
                    position.pnl_pct = position.pnl / (position.entry_price * position.size)
                    
                    equity += position.pnl
                    trades.append(position)
                    position = None

            # 2. Entry Logic
            if not position and signal != 0:
                direction = 1 if signal > 0 else -1
                entry_price = curr_price * (1 + self.slippage_pct if direction == 1 else 1 - self.slippage_pct)
                
                # Simple position sizing: use 95% of equity
                size = (equity * 0.95) / entry_price
                comm = entry_price * size * self.commission_pct
                
                position = Trade(
                    symbol="UNKNOWN",
                    entry_time=curr_time,
                    entry_price=entry_price,
                    direction=direction,
                    size=size,
                    commission=comm,
                    slippage=entry_price - curr_price
                )
            
            equity_curve.append(equity)

        # Final Metrics
        s_equity = pd.Series(equity_curve, index=df.index)
        metrics = calculate_metrics(s_equity, trades)
        
        return {
            "metrics": metrics,
            "equity_curve": equity_curve,
            "trades": [t.dict() for t in trades]
        }

    def run_walk_forward(
        self,
        df: pd.DataFrame,
        strategy_func: Callable,
        param_grid: List[Dict[str, Any]],
        window_gen: Any
    ) -> List[BacktestWindow]:
        """Execute Walk-Forward Analysis."""
        results = []
        
        for train_df, test_df in window_gen.generate_windows():
            # 1. Optimization Phase (In-Sample)
            best_params = None
            best_sharpe = -np.inf
            
            for params in param_grid:
                res = self.run_single_backtest(train_df, strategy_func, params)
                sharpe = res['metrics']['sharpe_ratio']
                if sharpe > best_sharpe:
                    best_sharpe = sharpe
                    best_params = params
            
            # 2. Validation Phase (Out-of-Sample)
            log.info("wf_oos_validation", start=test_df.index.min(), params=best_params)
            oos_res = self.run_single_backtest(test_df, strategy_func, best_params)
            
            results.append(BacktestWindow(
                train_start=train_df.index.min(),
                train_end=train_df.index.max(),
                test_start=test_df.index.min(),
                test_end=test_df.index.max(),
                params=best_params,
                metrics=oos_res['metrics']
            ))
            
        return results
