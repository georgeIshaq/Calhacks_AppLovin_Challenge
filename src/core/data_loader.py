#!/usr/bin/env python3
"""
Data Loader - Streaming CSV reader with time dimension extraction

This module provides memory-efficient streaming of the 49 CSV files (225M rows).
Uses Polars lazy loading to avoid loading everything into RAM.

Key features:
- Lazy CSV scanning (no full load)
- Time dimension extraction (day, hour, minute, week)
- Memory-efficient streaming aggregation
- Handles 225M rows on 16GB RAM
"""

import polars as pl
from pathlib import Path
from typing import List, Iterator
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataLoader:
    """
    Streaming data loader for ad events CSV files.
    
    Uses Polars lazy evaluation to avoid loading entire dataset into memory.
    """
    
    def __init__(self, data_dir: Path):
        """
        Initialize data loader.
        
        Args:
            data_dir: Directory containing CSV files
        """
        self.data_dir = Path(data_dir)
        self.csv_files = sorted(self.data_dir.glob("*.csv"))
        
        if not self.csv_files:
            raise ValueError(f"No CSV files found in {data_dir}")
        
        logger.info(f"Found {len(self.csv_files)} CSV files in {data_dir}")
    
    def load_lazy(self) -> pl.LazyFrame:
        """
        Load all CSV files as a single lazy DataFrame.
        
        This doesn't actually load data into memory - it creates a query plan
        that will be executed when needed.
        
        Returns:
            Polars LazyFrame with all data
        """
        logger.info("Creating lazy frame from CSV files...")
        
        # Schema for the CSV files
        # Note: ts is Unix timestamp (milliseconds), auction_id is UUID
        schema = {
            'ts': pl.Int64,  # Unix timestamp in milliseconds
            'type': pl.Utf8,
            'auction_id': pl.Utf8,  # UUID string
            'advertiser_id': pl.Int64,
            'publisher_id': pl.Int64,
            'bid_price': pl.Float64,
            'user_id': pl.Int64,
            'total_price': pl.Float64,
            'country': pl.Utf8,
        }
        
        # Read all CSV files lazily and concatenate
        lazy_frames = []
        
        for csv_file in self.csv_files:
            lf = pl.scan_csv(
                csv_file,
                schema=schema,
                try_parse_dates=False,  # We'll parse manually for control
            )
            lazy_frames.append(lf)
        
        # Concatenate all lazy frames
        combined = pl.concat(lazy_frames)
        
        logger.info(f"Lazy frame created from {len(self.csv_files)} files")
        
        return combined
    
    def add_time_dimensions(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        Add time dimension columns to lazy frame.
        
        Extracts:
        - day: YYYY-DDD format (e.g., "2024-001")
        - hour: YYYY-DDD HH format (e.g., "2024-001 14")
        - minute: YYYY-DDD HH:MM format (e.g., "2024-001 14:23")
        - week: YYYY-WW format (e.g., "2024-01")
        - date: YYYY-MM-DD format (e.g., "2024-01-01")
        
        Args:
            lf: Lazy frame with 'ts' column
            
        Returns:
            Lazy frame with additional time dimension columns
        """
        logger.info("Adding time dimension columns...")
        
        # Parse Unix timestamp (milliseconds) to datetime
        lf = lf.with_columns([
            # Convert Unix timestamp (ms) to datetime
            pl.from_epoch(pl.col('ts'), time_unit='ms').alias('timestamp'),
        ])
        
        lf = lf.with_columns([
            # Extract day of year (1-366)
            pl.col('timestamp').dt.ordinal_day().alias('day_of_year'),
            # Extract year
            pl.col('timestamp').dt.year().alias('year'),
            # Extract hour (0-23)
            pl.col('timestamp').dt.hour().alias('hour_of_day'),
            # Extract minute (0-59)
            pl.col('timestamp').dt.minute().alias('minute_of_hour'),
            # Extract ISO week (1-53)
            pl.col('timestamp').dt.week().alias('week_of_year'),
            # Extract date
            pl.col('timestamp').dt.date().alias('date'),
        ])
        
        lf = lf.with_columns([
            # day: "2024-001" format
            (pl.col('year').cast(pl.Utf8) + "-" + 
             pl.col('day_of_year').cast(pl.Utf8).str.zfill(3)).alias('day'),
            
            # hour: "2024-001 14" format
            (pl.col('year').cast(pl.Utf8) + "-" + 
             pl.col('day_of_year').cast(pl.Utf8).str.zfill(3) + " " +
             pl.col('hour_of_day').cast(pl.Utf8).str.zfill(2)).alias('hour'),
            
            # minute: "2024-001 14:23" format
            (pl.col('year').cast(pl.Utf8) + "-" + 
             pl.col('day_of_year').cast(pl.Utf8).str.zfill(3) + " " +
             pl.col('hour_of_day').cast(pl.Utf8).str.zfill(2) + ":" +
             pl.col('minute_of_hour').cast(pl.Utf8).str.zfill(2)).alias('minute'),
            
            # week: "2024-01" format
            (pl.col('year').cast(pl.Utf8) + "-" + 
             pl.col('week_of_year').cast(pl.Utf8).str.zfill(2)).alias('week'),
        ])
        
        # Drop intermediate columns
        lf = lf.drop(['timestamp', 'year', 'day_of_year', 'hour_of_day', 
                      'minute_of_hour', 'week_of_year'])
        
        logger.info("Time dimensions added: day, hour, minute, week, date")
        
        return lf
    
    def load_with_time_dims(self) -> pl.LazyFrame:
        """
        Load data with time dimensions added.
        
        This is the main entry point for loading data with all necessary
        time dimensions for rollup building.
        
        Returns:
            LazyFrame with original columns + time dimensions
        """
        lf = self.load_lazy()
        lf = self.add_time_dimensions(lf)
        
        logger.info("Data loaded with time dimensions (lazy evaluation)")
        logger.info("Columns: ts, type, auction_id, advertiser_id, publisher_id, "
                   "bid_price, user_id, total_price, country, day, hour, minute, week, date")
        
        return lf
    
    def get_sample(self, n: int = 1000) -> pl.DataFrame:
        """
        Get a sample of the data for inspection.
        
        Args:
            n: Number of rows to sample
            
        Returns:
            Materialized DataFrame with n rows
        """
        lf = self.load_with_time_dims()
        return lf.limit(n).collect()
    
    def get_stats(self) -> dict:
        """
        Get basic statistics about the data.
        
        Returns:
            Dictionary with data statistics
        """
        logger.info("Computing data statistics...")
        
        lf = self.load_lazy()
        
        # Count rows
        row_count = lf.select(pl.count()).collect().item()
        
        # Get unique counts for key dimensions
        stats = {
            'total_rows': row_count,
            'num_files': len(self.csv_files),
        }
        
        # Compute unique counts (this will materialize some data)
        lf_with_dims = self.load_with_time_dims()
        
        unique_counts = lf_with_dims.select([
            pl.col('type').n_unique().alias('unique_types'),
            pl.col('country').n_unique().alias('unique_countries'),
            pl.col('advertiser_id').n_unique().alias('unique_advertisers'),
            pl.col('publisher_id').n_unique().alias('unique_publishers'),
            pl.col('day').n_unique().alias('unique_days'),
        ]).collect()
        
        stats.update(unique_counts.to_dict(as_series=False))
        
        logger.info(f"Data statistics: {stats}")
        
        return stats


def main():
    """Test the data loader."""
    import time
    
    # Initialize loader
    data_dir = Path(__file__).parent.parent.parent / "data"
    loader = DataLoader(data_dir)
    
    print("="*60)
    print("Data Loader Test")
    print("="*60)
    
    # Test 1: Load lazy
    print("\nTest 1: Loading data lazily...")
    start = time.time()
    lf = loader.load_lazy()
    print(f"✅ Lazy frame created in {time.time() - start:.3f}s")
    print(f"   Schema: {lf.schema}")
    
    # Test 2: Add time dimensions
    print("\nTest 2: Adding time dimensions...")
    start = time.time()
    lf_with_dims = loader.add_time_dimensions(lf)
    print(f"✅ Time dimensions added in {time.time() - start:.3f}s (lazy)")
    print(f"   New schema: {lf_with_dims.schema}")
    
    # Test 3: Get sample
    print("\nTest 3: Materializing sample...")
    start = time.time()
    sample = loader.get_sample(10)
    print(f"✅ Sample loaded in {time.time() - start:.3f}s")
    print(f"\nSample data:")
    print(sample)
    
    # Test 4: Get statistics
    print("\nTest 4: Computing statistics...")
    start = time.time()
    stats = loader.get_stats()
    print(f"✅ Statistics computed in {time.time() - start:.3f}s")
    print(f"\nStatistics:")
    for key, value in stats.items():
        if isinstance(value, list):
            print(f"   {key}: {value[0]:,}")
        else:
            print(f"   {key}: {value:,}")
    
    print("\n" + "="*60)
    print("All tests passed! ✅")
    print("="*60)


if __name__ == "__main__":
    main()
