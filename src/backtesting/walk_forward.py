"""Quant Trading System — Walk Forward Window Generator.

Logic for generating training and testing windows for robust validation.
"""

from datetime import datetime
from typing import List, Tuple, Iterator
import pandas as pd

class WalkForwardWindowGenerator:
    """Generates rolling or anchored windows for backtesting."""

    def __init__(
        self, 
        df: pd.DataFrame, 
        train_size_days: int, 
        test_size_days: int, 
        step_size_days: int,
        anchored: bool = False
    ):
        self.df = df
        self.train_size = pd.Timedelta(days=train_size_days)
        self.test_size = pd.Timedelta(days=test_size_days)
        self.step_size = pd.Timedelta(days=step_size_days)
        self.anchored = anchored

    def generate_windows(self) -> Iterator[Tuple[pd.DataFrame, pd.DataFrame]]:
        """Yield (train_df, test_df) pairs."""
        if self.df.empty:
            return

        start_time = self.df.index.min()
        end_time = self.df.index.max()
        
        current_train_start = start_time
        
        while True:
            current_train_end = current_train_start + self.train_size
            current_test_end = current_train_end + self.test_size
            
            if current_test_end > end_time:
                break
                
            train_df = self.df.loc[current_train_start:current_train_end]
            test_df = self.df.loc[current_train_end:current_test_end]
            
            yield train_df, test_df
            
            # Move forward
            if not self.anchored:
                current_train_start += self.step_size
            else:
                # Anchored: start stays at beginning, end moves
                # Note: step_size here applies to the training end/test start
                current_train_start = start_time
                self.train_size += self.step_size
