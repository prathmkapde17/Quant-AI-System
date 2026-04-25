"""Verification script for Phase 3 Backtesting Engine."""

import asyncio
import pandas as pd
from datetime import datetime, timedelta

from src.storage.database import get_database
from src.storage.backtest_repository import BacktestRepository
from src.backtesting.engine import WalkForwardBacktester
from src.backtesting.signal_generator import rsi_strategy
from src.backtesting.walk_forward import WalkForwardWindowGenerator

async def main():
    db = get_database()
    await db.connect()
    
    repo = BacktestRepository(db)
    
    symbol = "BTCUSDT"
    end = datetime.now()
    start = end - timedelta(days=30)
    
    print(f"Fetching data for {symbol}...")
    # Convert exchange string to Enum if needed, but repo takes Exchange enum
    from src.core.enums import Exchange, Timeframe
    df = await repo.get_backtest_data(symbol, Exchange.BINANCE, Timeframe.M1, start, end)
    
    if df.empty:
        print("No data found!")
        await db.disconnect()
        return
        
    print(f"Data loaded: {len(df)} bars")
    
    tester = WalkForwardBacktester(initial_capital=10000)
    
    # 1. Test Single Backtest
    print("\n--- Running Single Backtest ---")
    res = tester.run_single_backtest(df, rsi_strategy, {"oversold": 30, "overbought": 70})
    print(f"Total Return: {res['metrics']['total_return_pct']:.2f}%")
    print(f"Sharpe Ratio: {res['metrics']['sharpe_ratio']:.2f}")
    print(f"Trades: {len(res['trades'])}")
    
    # 2. Test Walk-Forward
    print("\n--- Running Walk-Forward Analysis ---")
    window_gen = WalkForwardWindowGenerator(
        df, 
        train_size_days=10, 
        test_size_days=3, 
        step_size_days=3
    )
    
    param_grid = [
        {"oversold": 20, "overbought": 80},
        {"oversold": 30, "overbought": 70},
        {"oversold": 40, "overbought": 60}
    ]
    
    wf_results = tester.run_walk_forward(df, rsi_strategy, param_grid, window_gen)
    
    for i, window in enumerate(wf_results):
        print(f"Window {i+1}: Sharpe={window.metrics['sharpe_ratio']:.2f}, Params={window.params}")
        
    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
