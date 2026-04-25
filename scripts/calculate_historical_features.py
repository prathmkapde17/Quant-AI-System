"""Calculate features for all historical data in the database."""

import asyncio
import sys
from pathlib import Path
import pandas as pd

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.storage.database import Database
from src.storage.repository import OHLCVRepository
from src.feature_engineering import FeatureManager
from src.core.enums import Exchange, Timeframe

async def main():
    db = Database()
    await db.connect()
    
    repo = OHLCVRepository(db)
    manager = FeatureManager(db)
    
    # Get all unique symbol/exchange/timeframe combos
    query = "SELECT DISTINCT symbol, exchange, timeframe FROM ohlcv"
    combos = await db.fetch(query)
    
    print(f"Found {len(combos)} combinations to process.")
    
    for combo in combos:
        symbol = combo["symbol"]
        exchange = Exchange(combo["exchange"])
        timeframe = Timeframe(combo["timeframe"])
        
        print(f"Processing {symbol} ({exchange.value}) {timeframe.value}...")
        
        # Fetch all bars (last 30 days for testing)
        end_time = pd.Timestamp.now(tz='UTC')
        start_time = end_time - pd.Timedelta(days=30)
        
        bars = await repo.get_ohlcv(
            symbol=symbol,
            exchange=exchange,
            timeframe=timeframe,
            start=start_time,
            end=end_time
        )
        
        if not bars:
            continue
            
        df = pd.DataFrame(bars)
        
        # Process in windows to simulate live flow or just do the whole thing
        # For this script, we'll process the whole dataframe
        # Note: manager.process_symbol currently only saves the LATEST bar.
        # Let's modify the manager or use a bulk save here.
        
        from src.feature_engineering.feature_calculations import FeatureCalculator
        calc = FeatureCalculator()
        
        # Calculate
        df["rsi_14"] = calc.calculate_rsi(df, 14)
        df["sma_20"] = calc.calculate_sma(df, 20)
        df["ema_20"] = calc.calculate_ema(df, 20)
        macd, signal, _ = calc.calculate_macd(df)
        df["macd"] = macd
        df["macd_signal"] = signal
        
        # Bulk save
        from src.feature_engineering.feature_storage import FeatureRepository
        f_repo = FeatureRepository(db)
        
        count = 0
        for _, row in df.dropna().iterrows():
            features = {
                "rsi_14": row["rsi_14"],
                "sma_20": row["sma_20"],
                "ema_20": row["ema_20"],
                "macd": row["macd"],
                "macd_signal": row["macd_signal"]
            }
            success = await f_repo.save_features(
                row["timestamp"], symbol, exchange, timeframe, features
            )
            if success:
                count += 1
                
        print(f"  ✓ Saved {count} feature bars for {symbol}")
        
    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
