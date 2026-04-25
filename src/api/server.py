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
from src.backtesting.engine import VectorizedEngine, BacktestResult

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

@app.post("/api/backtest/run", response_model=BacktestResult)
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
        
    # 2. Run Strategy (Example: RSI Crossover)
    # This is a stub for real dynamic strategy loading
    signals = pd.Series(0, index=df.index)
    
    if req.strategy_name == "rsi_mean_reversion":
        oversold = req.params.get("oversold", 30)
        overbought = req.params.get("overbought", 70)
        
        if 'rsi_14' in df.columns:
            signals[df['rsi_14'] < oversold] = 1   # Buy
            signals[df['rsi_14'] > overbought] = -1 # Sell
        else:
            raise HTTPException(status_code=400, detail="RSI feature not found in database for this symbol.")

    # 3. Execute Backtest
    engine = VectorizedEngine()
    result = engine.run(df, signals)
    
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
