"""Quant Trading System — Research API Server.

Serves backtesting requests and provides system health info.
"""

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime, timedelta

from src.core.config import get_settings
from src.core.enums import Exchange, Timeframe
from src.storage.database import get_database, Database
from src.storage.repository import OHLCVRepository
from src.storage.backtest_repository import BacktestRepository
from src.backtesting.engine import WalkForwardBacktester
from src.backtesting.signal_generator import rsi_strategy, trend_following_ema, macd_strategy
from src.backtesting.walk_forward import WalkForwardWindowGenerator

app = FastAPI(title="Quant Research API")

# Enable CORS for the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class BacktestRequest(BaseModel):
    symbol: str
    exchange: str
    timeframe: str
    strategy_name: str
    params: Dict[str, Any]
    days: int = 30

@app.on_event("startup")
async def startup():
    db = get_database()
    await db.connect()

@app.on_event("shutdown")
async def shutdown():
    db = get_database()
    await db.disconnect()

@app.get("/api/symbols")
async def get_symbols():
    db = get_database()
    repo = OHLCVRepository(db)
    symbols = await repo.get_symbols()
    return {"symbols": symbols}

@app.post("/api/backtest/run")
async def run_backtest(req: BacktestRequest):
    db = get_database()
    bt_repo = BacktestRepository(db)
    
    # 1. Fetch Data
    end = datetime.now()
    start = end - timedelta(days=req.days)
    
    df = await bt_repo.get_backtest_data(
        req.symbol, 
        Exchange(req.exchange), 
        Timeframe(req.timeframe),
        start, 
        end
    )
    
    if df.empty:
        raise HTTPException(status_code=404, detail="No data found for the selected period.")
        
    # 2. Map Strategy
    strategies = {
        "rsi_mean_reversion": rsi_strategy,
        "ema_cross": trend_following_ema,
        "macd_cross": macd_strategy
    }
    
    strategy_func = strategies.get(req.strategy_name)
    if not strategy_func:
        raise HTTPException(status_code=400, detail=f"Strategy {req.strategy_name} not implemented.")

    # 3. Execute Backtest
    tester = WalkForwardBacktester()
    result = tester.run_single_backtest(df, strategy_func, req.params)
    
    # 4. Persistence
    run_id = await bt_repo.save_backtest_run(
        symbol=req.symbol,
        strategy_name=req.strategy_name,
        params=req.params,
        metrics=result['metrics']
    )
    
    return {
        "id": run_id,
        "metrics": result['metrics'],
        "equity_curve": result['equity_curve'],
        "trades": result['trades']
    }

@app.post("/api/backtest/walk-forward")
async def run_wf(
    symbol: str,
    exchange: str,
    timeframe: str,
    days: int,
    strategy: str,
    train_days: int = 15,
    test_days: int = 5
):
    """Execute Walk-Forward Analysis."""
    db = get_database()
    repo = BacktestRepository(db)
    
    end = datetime.now()
    start = end - timedelta(days=days)
    
    df = await repo.get_backtest_data(symbol, Exchange(exchange), Timeframe(timeframe), start, end)
    
    strategies = {"rsi_mean_reversion": rsi_strategy}
    strategy_func = strategies.get(strategy)
    
    if not strategy_func:
        raise HTTPException(status_code=400, detail="Strategy not supported for Walk-Forward.")
        
    window_gen = WalkForwardWindowGenerator(df, train_days, test_days, test_days)
    
    # Optimization Grid
    param_grid = [
        {"oversold": 20, "overbought": 80},
        {"oversold": 30, "overbought": 70}
    ]
    
    tester = WalkForwardBacktester()
    results = tester.run_walk_forward(df, strategy_func, param_grid, window_gen)
    
    return [r.dict() for r in results]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
