import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.storage.database import Database
from src.core.enums import Exchange, Timeframe

async def main():
    db = Database()
    await db.connect()
    
    print("\n--- Latest Features ---")
    rows = await db.fetch("SELECT * FROM features ORDER BY timestamp DESC LIMIT 20")
    if not rows:
        print("No features found yet. The pipeline might still be waiting for the next 1m candle flush.")
    for row in rows:
        print(f"{row['timestamp']} | {row['symbol']} | {row['feature_name']}: {row['feature_value']:.4f}")
        
    print("\n--- Feature Counts ---")
    counts = await db.fetch("SELECT feature_name, COUNT(*) FROM features GROUP BY feature_name")
    for c in counts:
        print(f"{c['feature_name']}: {c['count']}")
        
    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
