import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.storage.database import Database

async def main():
    db = Database()
    await db.connect()
    
    print("Clearing all market data and features...")
    await db.execute("TRUNCATE TABLE ohlcv CASCADE")
    await db.execute("TRUNCATE TABLE features CASCADE")
    await db.execute("TRUNCATE TABLE ticks CASCADE")
    await db.execute("TRUNCATE TABLE pipeline_latency CASCADE")
    
    print("✅ All data cleared.")
    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
