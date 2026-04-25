"""TimescaleDB adapter for feature storage."""

import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from src.core.logging import get_logger
from src.storage.database import Database

log = get_logger(__name__)


class TimescaleFeatureAdapter:
    """Adapter for storing features in TimescaleDB.
    
    Creates and manages feature tables with proper indexing.
    """
    
    def __init__(self, db: Database):
        self.db = db
        self.logger = get_logger(__name__)
    
    async def create_feature_table(
        self,
        feature_name: str,
        feature_type: str = 'numeric',
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Create a new feature table.
        
        Args:
            feature_name: Name of the feature
            feature_type: Type of feature (numeric, categorical, etc.)
            metadata: Additional metadata
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS feature_{feature_name} (
            timestamp TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            value DOUBLE PRECISION,
            quality TEXT DEFAULT 'CLEAN',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (timestamp, symbol)
        );
        
        SELECT create_hypertable('feature_{feature_name}', 'timestamp', if_not_exists => TRUE);
        CREATE INDEX IF NOT EXISTS idx_feature_{feature_name}_symbol 
        ON feature_{feature_name} (symbol, timestamp DESC);
        """
        
        await self.db.execute(query)
        self.logger.info("feature_table_created", feature=feature_name)
    
    async def store_features(
        self,
        feature_name: str,
        features: pd.DataFrame
    ) -> int:
        """Store features in database.
        
        Args:
            feature_name: Name of the feature
            features: DataFrame with features (index=timestamp, columns=['symbol', 'value'])
        
        Returns:
            Number of rows stored
        """
        if features.empty:
            return 0
        
        # Prepare data for insertion
        features = features.copy()
        features['feature_name'] = feature_name
        features['created_at'] = datetime.now(timezone.utc)
        
        # Bulk insert
        records = features.reset_index().to_dict('records')
        
        query = f"""
        INSERT INTO feature_{feature_name} (timestamp, symbol, value, quality)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (timestamp, symbol) DO UPDATE
        SET value = EXCLUDED.value, quality = EXCLUDED.quality
        """
        
        count = 0
        for _, row in features.iterrows():
            try:
                await self.db.execute(
                    query,
                    row.name,  # timestamp
                    row.get('symbol', 'UNKNOWN'),
                    row.get('value', 0),
                    'CLEAN'
                )
                count += 1
            except Exception as e:
                self.logger.error(
                    "feature_insert_error",
                    feature=feature_name,
                    symbol=row.get('symbol'),
                    error=str(e)
                )
        
        self.logger.info("features_stored", feature=feature_name, count=count)
        return count
    
    async def get_features(
        self,
        feature_name: str,
        symbols: List[str],
        start: datetime,
        end: datetime
    ) -> pd.DataFrame:
        """Retrieve features from database.
        
        Args:
            feature_name: Name of the feature
            symbols: List of symbols
            start: Start timestamp
            end: End timestamp
        
        Returns:
            DataFrame with features
        """
        symbol_list = "','".join(symbols)
        query = f"""
        SELECT timestamp, symbol, value, quality
        FROM feature_{feature_name}
        WHERE symbol IN ('{symbol_list}')
          AND timestamp BETWEEN $1 AND $2
        ORDER BY timestamp, symbol
        """
        
        rows = await self.db.fetch(query, start, end)
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows)
        df.set_index('timestamp', inplace=True)
        return df
