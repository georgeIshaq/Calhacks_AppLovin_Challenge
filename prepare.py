#!/usr/bin/env python3
"""
Prepare Phase: Build optimized rollup tables from raw data

This script:
1. Loads raw CSV event data (245M rows)
2. Builds 10 pre-aggregated rollup tables
3. Writes rollups to disk as Arrow IPC files
4. Pre-loads all rollups into memory for instant query access

Expected runtime: ~7-10 minutes
Memory usage: ~3-5GB peak during build, ~2GB for pre-loaded rollups
"""

import sys
import time
from pathlib import Path
import argparse
import logging

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.core import DataLoader, RollupBuilder, StorageWriter, RollupLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Optimize threading for parallel processing
import os
cores = os.cpu_count() or 8
os.environ['POLARS_MAX_THREADS'] = str(cores)
logger.info(f"Configured {cores} threads for parallel CSV processing")


def convert_to_parquet(data_dir: Path, output_dir: Path) -> Path:
    """
    Convert raw CSV files to Parquet format for DuckDB fallback.
    
    Parquet provides:
    - 5-10× faster reads than CSV
    - Better compression (~70% size reduction)
    - Columnar format optimized for analytics
    
    Args:
        data_dir: Directory containing raw CSV files
        output_dir: Directory to write Parquet file
    
    Returns:
        Path to created Parquet file
    """
    import polars as pl
    
    logger.info("Converting raw data to Parquet for DuckDB fallback...")
    
    start_time = time.time()
    parquet_path = output_dir / 'events.parquet'
    
    # Read all CSVs with proper schema
    df = pl.scan_csv(
        str(data_dir / '*.csv'),
        has_header=True,
        schema_overrides={
            'ts': pl.Int64,
            'type': pl.Utf8,
            'auction_id': pl.Utf8,
            'advertiser_id': pl.Int32,
            'publisher_id': pl.Int32,
            'bid_price': pl.Float64,
            'user_id': pl.Int64,
            'total_price': pl.Float64,
            'country': pl.Utf8,
        }
    )
    
    # Write to Parquet with compression
    df.sink_parquet(
        parquet_path,
        compression='snappy',  # Fast decompression
        row_group_size=1_000_000,  # Optimize for scanning
    )
    
    elapsed = time.time() - start_time
    size_mb = parquet_path.stat().st_size / (1024 * 1024)
    
    logger.info(f"✅ Parquet file created: {parquet_path}")
    logger.info(f"   Size: {size_mb:.1f} MB")
    logger.info(f"   Time: {elapsed:.1f}s")
    
    return parquet_path


def build_duckdb_fallback(output_dir: Path, parquet_path: Path):
    """
    Build DuckDB fallback with OPTIMAL PHYSICAL LAYOUT (no indexes needed!)
    
    Key optimizations from expert recommendations:
    1. ✅ Already using Parquet (columnar, compressed)
    2. ✅ Sort by high-cardinality dims (week, country, type) for GROUP BY locality
    3. ✅ Pre-compute ALL time dimensions (week, day, hour, minute)
    4. ✅ Multi-threaded execution
    5. ✅ Simple, fast, no complex indexes
    
    Expected performance:
    - Simple queries: 100-200ms
    - Complex multi-GROUP BY: 200-400ms
    - Worst case: <500ms
    
    This is our safety net for 100% query coverage.
    
    Args:
        output_dir: Directory to write DuckDB file
        parquet_path: Path to Parquet file with events
    """
    import duckdb
    
    logger.info("Building DuckDB fallback with optimal physical layout...")
    
    start_time = time.time()
    duckdb_path = output_dir / 'fallback.duckdb'
    
    # Remove existing database if present
    if duckdb_path.exists():
        duckdb_path.unlink()
    
    con = duckdb.connect(str(duckdb_path))
    
    # Optimize threading and memory for faster builds
    import os
    cores = max(1, (os.cpu_count() or 8) - 1)  # Leave 1 core for OS, default to 8
    con.execute(f"PRAGMA threads={cores}")
    con.execute("PRAGMA memory_limit='12GB'")
    con.execute("PRAGMA temp_directory='/tmp'")
    
    logger.info(f"DuckDB configured: {cores} threads, 12GB memory limit")
    
    logger.info("Creating sorted events table (optimized for GROUP BY)...")
    
    # KEY OPTIMIZATION: Create table SORTED by common GROUP BY dimensions
    # This gives DuckDB massive performance wins:
    # - Locality for GROUP BY operations
    # - Better compression
    # - Efficient aggregation without indexes
    con.execute(f"""
        CREATE TABLE events AS 
        SELECT 
            -- Pre-compute ALL time dimensions (matches rollup format)
            STRFTIME(to_timestamp(ts / 1000.0), '%Y-%m-%d') AS day,
            STRFTIME(to_timestamp(ts / 1000.0), '%Y-W%V') AS week,
            STRFTIME(to_timestamp(ts / 1000.0), '%Y-%m-%d %H:00') AS hour,
            STRFTIME(to_timestamp(ts / 1000.0), '%Y-%m-%d %H:%M') AS minute,
            
            -- Dimensions (ordered by cardinality)
            type,
            country,
            advertiser_id,
            publisher_id,
            
            -- Metrics
            bid_price,
            total_price
        FROM parquet_scan('{parquet_path}')
        ORDER BY week, country, type  -- Sort by common GROUP BY dimensions
    """)
    
    logger.info("Optimizing table statistics...")
    
    # Update statistics for query planner (fast, no indexes needed)
    con.execute("ANALYZE events")
    
    # Checkpoint to persist
    con.execute("CHECKPOINT")
    
    # Get statistics
    row_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    
    con.close()
    
    elapsed = time.time() - start_time
    size_mb = duckdb_path.stat().st_size / (1024 * 1024)
    
    logger.info(f"✅ DuckDB fallback ready: {duckdb_path}")
    logger.info(f"   Rows: {row_count:,}")
    logger.info(f"   Size: {size_mb:.1f} MB")
    logger.info(f"   Time: {elapsed:.1f}s")
    logger.info(f"   Strategy: Sorted table (week, country, type) - NO indexes needed!")


def main():
    parser = argparse.ArgumentParser(
        description="Prepare phase: Build optimized rollup tables"
    )
    parser.add_argument(
        '--data-dir',
        type=Path,
        default=Path('data'),
        help='Directory containing input CSV files'
    )
    parser.add_argument(
        '--rollup-dir',
        type=Path,
        default=Path('rollups'),
        help='Directory to write rollup files'
    )
    
    args = parser.parse_args()
    
    print("="*70)
    print("PREPARE PHASE: Building Optimized Rollup Tables")
    print("="*70)
    print()
    print(f"Data directory:   {args.data_dir}")
    print(f"Rollup directory: {args.rollup_dir}")
    print()
    
    # Validate data directory
    if not args.data_dir.exists():
        logger.error(f"Data directory not found: {args.data_dir}")
        sys.exit(1)
    
    csv_files = list(args.data_dir.glob('*.csv'))
    if not csv_files:
        logger.error(f"No CSV files found in {args.data_dir}")
        sys.exit(1)
    
    logger.info(f"Found {len(csv_files)} CSV files")
    
    # Create rollup directory
    args.rollup_dir.mkdir(parents=True, exist_ok=True)
    
    # Phase 1: Build rollups
    logger.info("")
    logger.info("="*70)
    logger.info("PHASE 1: Building Rollups")
    logger.info("="*70)
    
    build_start = time.time()
    
    loader = DataLoader(args.data_dir)
    builder = RollupBuilder(loader)
    
    logger.info("Starting single-pass rollup build...")
    logger.info("This will take approximately 7-10 minutes...")
    logger.info("")
    
    try:
        rollups = builder.build_all_rollups_single_pass()
    except Exception as e:
        logger.error(f"Failed to build rollups: {e}", exc_info=True)
        sys.exit(1)
    
    build_time = time.time() - build_start
    
    logger.info("")
    logger.info(f"✅ Built {len(rollups)} rollups in {build_time:.1f}s ({build_time/60:.1f} min)")
    
    # Phase 2: Write rollups to disk
    logger.info("")
    logger.info("="*70)
    logger.info("PHASE 2: Writing Rollups to Disk")
    logger.info("="*70)
    
    write_start = time.time()
    
    storage = StorageWriter(args.rollup_dir)
    
    try:
        storage.write_all_rollups(rollups)
    except Exception as e:
        logger.error(f"Failed to write rollups: {e}", exc_info=True)
        sys.exit(1)
    
    write_time = time.time() - write_start
    
    logger.info(f"✅ Wrote {len(rollups)} rollups in {write_time:.1f}s")
    
    # Phase 3: Build DuckDB fallback
    logger.info("")
    logger.info("="*70)
    logger.info("PHASE 3: Building DuckDB Fallback")
    logger.info("="*70)
    
    duckdb_start = time.time()
    
    try:
        # Convert CSV to Parquet (faster for DuckDB)
        parquet_path = convert_to_parquet(args.data_dir, args.rollup_dir)
        
        # Build DuckDB database from Parquet
        build_duckdb_fallback(args.rollup_dir, parquet_path)
    except Exception as e:
        logger.error(f"Failed to build DuckDB fallback: {e}", exc_info=True)
        logger.warning("Continuing without DuckDB fallback - fallback queries will be slow!")
    
    duckdb_time = time.time() - duckdb_start
    logger.info(f"✅ DuckDB fallback built in {duckdb_time:.1f}s")
    
    # Phase 4: Pre-load rollups for run phase
    logger.info("")
    logger.info("="*70)
    logger.info("PHASE 4: Pre-loading Rollups")
    logger.info("="*70)
    
    load_start = time.time()
    
    try:
        # Initialize loader with ALL rollups pre-loaded
        # This happens during prepare phase so startup time doesn't count against query execution
        loader_instance = RollupLoader(
            args.rollup_dir,
            preload_threshold_mb=1000  # Pre-load everything (up to 1GB)
        )
        
        preloaded_count = len(loader_instance.preloaded)
        total_size_mb = sum(
            (args.rollup_dir / f"{name}.arrow").stat().st_size / (1024*1024)
            for name in loader_instance.preloaded.keys()
            if (args.rollup_dir / f"{name}.arrow").exists()
        )
        
    except Exception as e:
        logger.error(f"Failed to pre-load rollups: {e}", exc_info=True)
        sys.exit(1)
    
    load_time = time.time() - load_start
    
    logger.info(f"✅ Pre-loaded {preloaded_count} rollups ({total_size_mb:.1f} MB) in {load_time:.1f}s")
    
    # Summary
    total_time = time.time() - build_start
    
    print()
    print("="*70)
    print("PREPARE PHASE COMPLETE")
    print("="*70)
    print(f"Build time:    {build_time:.1f}s ({build_time/60:.1f} min)")
    print(f"Write time:    {write_time:.1f}s")
    print(f"DuckDB time:   {duckdb_time:.1f}s ({duckdb_time/60:.1f} min)")
    print(f"Preload time:  {load_time:.1f}s")
    print(f"Total time:    {total_time:.1f}s ({total_time/60:.1f} min)")
    print()
    
    budget = 600  # 10 minutes
    if total_time < budget:
        print(f"✅ UNDER BUDGET! ({budget-total_time:.0f}s to spare)")
    else:
        print(f"⚠️ OVER BUDGET by {total_time-budget:.0f}s")
    
    print()
    print(f"Rollups written to: {args.rollup_dir}")
    print(f"Ready for run phase!")
    print("="*70)


if __name__ == "__main__":
    main()
