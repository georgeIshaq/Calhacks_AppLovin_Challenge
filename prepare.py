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
    
    # Phase 3: Pre-load rollups for run phase
    logger.info("")
    logger.info("="*70)
    logger.info("PHASE 3: Pre-loading Rollups")
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
