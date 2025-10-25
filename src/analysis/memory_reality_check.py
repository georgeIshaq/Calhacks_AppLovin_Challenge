"""
Memory Reality Check

Tests actual memory usage with realistic data structures to validate
our 8GB memory budget claim.

Tests:
1. Python interpreter baseline
2. Loading dictionaries for all columns
3. Loading sample pre-aggregations
4. Allocating "hot cache" buffers
5. Peak memory during query processing simulation
"""

import sys
import os
import psutil
import gc
from pathlib import Path
from collections import defaultdict
import csv
import time


def get_memory_mb():
    """Get current RSS memory in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024**2


def get_system_memory_info():
    """Get system memory info"""
    mem = psutil.virtual_memory()
    return {
        'total_gb': mem.total / 1024**3,
        'available_gb': mem.available / 1024**3,
        'used_gb': mem.used / 1024**3,
        'percent': mem.percent
    }


def test_baseline_memory():
    """Test 1: Baseline Python + imports memory"""
    print("\n" + "="*80)
    print("TEST 1: Baseline Memory (Python + Libraries)")
    print("="*80)
    
    gc.collect()
    baseline = get_memory_mb()
    print(f"Baseline RSS: {baseline:.1f} MB")
    
    # Import heavy libraries
    import numpy as np
    import pandas as pd
    
    gc.collect()
    with_libs = get_memory_mb()
    print(f"After numpy/pandas: {with_libs:.1f} MB (+{with_libs-baseline:.1f} MB)")
    
    return with_libs


def test_dictionary_memory(data_dir: Path, sample_size: int = 500000):
    """Test 2: Memory for column dictionaries"""
    print("\n" + "="*80)
    print("TEST 2: Dictionary Memory")
    print("="*80)
    
    gc.collect()
    before = get_memory_mb()
    
    # Load dictionaries for key columns
    dictionaries = {}
    columns = ['type', 'country', 'advertiser_id', 'publisher_id', 'user_id']
    
    csv_files = list(data_dir.glob("*.csv"))
    if not csv_files:
        print("No CSV files found!")
        return before
    
    print(f"Loading dictionaries from {csv_files[0].name} ({sample_size:,} rows)...")
    
    with open(csv_files[0], 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= sample_size:
                break
            
            for col in columns:
                if col not in dictionaries:
                    dictionaries[col] = set()
                val = row.get(col, '')
                if val:
                    dictionaries[col].add(val)
    
    # Convert to lists (as we'd store them)
    for col in columns:
        if col in dictionaries:
            dictionaries[col] = list(dictionaries[col])
            print(f"  {col:20s}: {len(dictionaries[col]):>8,} unique values")
    
    gc.collect()
    after = get_memory_mb()
    dict_mb = after - before
    
    print(f"\nDictionary memory: {dict_mb:.1f} MB")
    print(f"Total RSS: {after:.1f} MB")
    
    return after, dictionaries


def test_preagg_memory(data_dir: Path, sample_ratio: float = 0.1):
    """Test 3: Memory for pre-aggregations"""
    print("\n" + "="*80)
    print("TEST 3: Pre-Aggregation Memory")
    print("="*80)
    
    gc.collect()
    before = get_memory_mb()
    
    # Simulate pre-agg structures
    # (day, type) -> {sum_bid, sum_total, count}
    print("Building (day, type) pre-aggregation from sample...")
    
    preagg_day_type = {}
    
    csv_files = list(data_dir.glob("*.csv"))
    sample_files = csv_files[:max(1, int(len(csv_files) * sample_ratio))]
    
    print(f"Sampling {len(sample_files)} files...")
    
    from datetime import datetime
    
    for csv_file in sample_files:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts = row.get('ts', '')
                event_type = row.get('type', '')
                bid_price = row.get('bid_price', '')
                
                if ts and event_type:
                    try:
                        dt = datetime.fromtimestamp(int(ts) / 1000)
                        day = dt.strftime('%Y-%m-%d')
                        key = (day, event_type)
                        
                        if key not in preagg_day_type:
                            preagg_day_type[key] = {
                                'sum_bid': 0.0,
                                'count': 0,
                                'count_non_null_bid': 0
                            }
                        
                        preagg_day_type[key]['count'] += 1
                        if bid_price:
                            try:
                                preagg_day_type[key]['sum_bid'] += float(bid_price)
                                preagg_day_type[key]['count_non_null_bid'] += 1
                            except:
                                pass
                    except:
                        pass
    
    print(f"Pre-agg (day, type) rows: {len(preagg_day_type):,}")
    
    gc.collect()
    after = get_memory_mb()
    preagg_mb = after - before
    
    print(f"\nPre-agg memory (sample): {preagg_mb:.1f} MB")
    print(f"Extrapolated for full data: ~{preagg_mb / sample_ratio:.1f} MB")
    print(f"Total RSS: {after:.1f} MB")
    
    return after, preagg_day_type


def test_hot_cache_memory(cache_size_mb: int = 3000):
    """Test 4: Memory for hot column cache"""
    print("\n" + "="*80)
    print("TEST 4: Hot Cache Memory")
    print("="*80)
    
    gc.collect()
    before = get_memory_mb()
    
    print(f"Allocating {cache_size_mb} MB cache buffer...")
    
    # Allocate byte array to simulate compressed column cache
    import numpy as np
    cache_bytes = cache_size_mb * 1024 * 1024
    hot_cache = np.zeros(cache_bytes, dtype=np.uint8)
    
    # Force actual memory allocation by writing to it
    print("  Writing to buffer to force physical allocation...")
    hot_cache[::1000000] = 1  # Touch every ~1MB
    
    gc.collect()
    after = get_memory_mb()
    cache_mb = after - before
    
    print(f"Allocated: {cache_size_mb} MB")
    print(f"Actual RSS increase: {cache_mb:.1f} MB")
    print(f"Total RSS: {after:.1f} MB")
    
    # Check if we're in danger zone
    sys_mem = get_system_memory_info()
    if sys_mem['available_gb'] < 2.0:
        print(f"\n⚠️  WARNING: Only {sys_mem['available_gb']:.1f} GB available!")
        print("   System may start swapping!")
    
    return after, hot_cache


def test_query_processing_memory():
    """Test 5: Simulate query processing buffers"""
    print("\n" + "="*80)
    print("TEST 5: Query Processing Buffers")
    print("="*80)
    
    gc.collect()
    before = get_memory_mb()
    
    print("Simulating query buffers (decompression, grouping, results)...")
    
    import numpy as np
    
    # Simulate buffers for processing a query
    buffers = []
    
    # Decompression buffer (100MB)
    print("  - Decompression buffer: 100 MB")
    buf1 = np.zeros(100 * 1024 * 1024, dtype=np.uint8)
    buf1[::1000000] = 1  # Force allocation
    buffers.append(buf1)
    
    # Group keys buffer (50MB)
    print("  - Group keys buffer: 50 MB")
    buf2 = np.zeros(50 * 1024 * 1024, dtype=np.uint8)
    buf2[::1000000] = 1  # Force allocation
    buffers.append(buf2)
    
    # Aggregation buffer (50MB)
    print("  - Aggregation buffer: 50 MB")
    buf3 = np.zeros(50 * 1024 * 1024, dtype=np.uint8)
    buf3[::1000000] = 1  # Force allocation
    buffers.append(buf3)
    
    gc.collect()
    after = get_memory_mb()
    buffer_mb = after - before
    
    print(f"\nQuery buffer memory: {buffer_mb:.1f} MB")
    print(f"Total RSS: {after:.1f} MB")
    
    return after, buffers


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Memory reality check')
    parser.add_argument('--data-dir', default='data',
                       help='Directory containing CSV files')
    parser.add_argument('--cache-size-mb', type=int, default=3000,
                       help='Hot cache size in MB to test')
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"Error: Data directory not found: {args.data_dir}")
        return 1
    
    print("="*80)
    print("MEMORY REALITY CHECK")
    print("="*80)
    
    # System info
    sys_mem = get_system_memory_info()
    print(f"\nSystem Memory:")
    print(f"  Total:     {sys_mem['total_gb']:.1f} GB")
    print(f"  Used:      {sys_mem['used_gb']:.1f} GB")
    print(f"  Available: {sys_mem['available_gb']:.1f} GB")
    print(f"  Usage:     {sys_mem['percent']:.1f}%")
    
    # Run tests
    mem_baseline = test_baseline_memory()
    mem_with_dicts, dicts = test_dictionary_memory(data_dir)
    mem_with_preagg, preagg = test_preagg_memory(data_dir, sample_ratio=0.2)
    
    # Try to allocate hot cache
    try:
        mem_with_cache, cache = test_hot_cache_memory(args.cache_size_mb)
        mem_with_buffers, buffers = test_query_processing_memory()
    except MemoryError:
        print("\n❌ MEMORY ERROR: Could not allocate requested memory!")
        mem_with_cache = get_memory_mb()
        mem_with_buffers = mem_with_cache
    
    # Final summary
    print("\n" + "="*80)
    print("MEMORY SUMMARY")
    print("="*80)
    
    print(f"\nMemory breakdown:")
    print(f"  Baseline (Python + libs):     {mem_baseline:>8.1f} MB")
    print(f"  + Dictionaries:               {mem_with_dicts - mem_baseline:>8.1f} MB")
    print(f"  + Pre-aggregations:           {mem_with_preagg - mem_with_dicts:>8.1f} MB")
    print(f"  + Hot cache ({args.cache_size_mb} MB):      {mem_with_cache - mem_with_preagg:>8.1f} MB")
    print(f"  + Query buffers:              {mem_with_buffers - mem_with_cache:>8.1f} MB")
    print(f"  {'─'*40}")
    print(f"  TOTAL RSS:                    {mem_with_buffers:>8.1f} MB ({mem_with_buffers/1024:.2f} GB)")
    
    # System check
    sys_mem_after = get_system_memory_info()
    print(f"\nSystem memory after test:")
    print(f"  Available: {sys_mem_after['available_gb']:.1f} GB")
    print(f"  Usage:     {sys_mem_after['percent']:.1f}%")
    
    # Verdict
    print(f"\n{'='*80}")
    print("VERDICT")
    print("="*80)
    
    total_gb = mem_with_buffers / 1024
    
    if total_gb < 6:
        print(f"✅ SAFE: {total_gb:.2f} GB is well within 8GB budget")
    elif total_gb < 8:
        print(f"⚠️  BORDERLINE: {total_gb:.2f} GB is close to limit")
    else:
        print(f"❌ DANGER: {total_gb:.2f} GB exceeds 8GB budget!")
    
    if sys_mem_after['available_gb'] < 2:
        print(f"⚠️  WARNING: Low system memory ({sys_mem_after['available_gb']:.1f} GB available)")
        print("   System may start swapping, which will kill performance!")
    
    print(f"\n💡 RECOMMENDATION:")
    if total_gb > 6:
        recommended_cache = int((6 - (mem_with_preagg / 1024)) * 1024)
        print(f"   Reduce hot cache to ~{recommended_cache} MB")
    else:
        print(f"   Current allocation is safe. Proceed with confidence.")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
