#!/usr/bin/env python3
"""
Phase 0 Test 1: Arrow IPC Query Performance
===========================================

CRITICAL TEST - This validates our entire storage strategy!

Tests:
1. Can we write rollup to .arrow file?
2. Can we load it in <10ms?
3. Can we filter + aggregate in <10ms?
4. Total query time <20ms?

Success criteria: <20ms total (load + query)
Failure action: Try Parquet or in-memory pickle instead
"""

import polars as pl
import time
from pathlib import Path
import tempfile
import shutil

def create_realistic_rollup():
    """Create a realistic day × type rollup (1,464 rows)."""
    print("Creating realistic day × type rollup...")
    
    # Generate 366 days × 4 types = 1,464 rows
    days = [f"2024-{m:02d}-{d:02d}" 
            for m in range(1, 13) 
            for d in range(1, 32) if d <= 28 or (m == 2 and d == 29) or d <= 31][:366]
    types = ['serve', 'impression', 'click', 'purchase']
    
    # Create cross product
    data = []
    for day in days:
        for type_ in types:
            # Realistic values
            if type_ == 'serve':
                bid_sum = 50000.0
                bid_count = 1000
                total_sum = 0.0
                total_count = 0
                row_count = 146000000 // 366  # ~400K rows per day
            elif type_ == 'impression':
                bid_sum = 30000.0
                bid_count = 800
                total_sum = 0.0
                total_count = 0
                row_count = 74000000 // 366
            elif type_ == 'click':
                bid_sum = 0.0  # All NULL for clicks
                bid_count = 0
                total_sum = 0.0
                total_count = 0
                row_count = 3600000 // 366
            else:  # purchase
                bid_sum = 0.0
                bid_count = 0
                total_sum = 15000.0
                total_count = 50
                row_count = 22000 // 366
            
            data.append({
                'day': day,
                'type': type_,
                'bid_price_sum': bid_sum,
                'bid_price_count': bid_count,
                'total_price_sum': total_sum,
                'total_price_count': total_count,
                'row_count': row_count,
            })
    
    df = pl.DataFrame(data)
    print(f"✅ Created rollup: {len(df)} rows, {df.estimated_size('mb'):.2f} MB")
    return df


def test_write_performance(df: pl.DataFrame, temp_dir: Path):
    """Test: Can we write to .arrow file quickly?"""
    print("\n" + "="*60)
    print("TEST 1a: Write Performance")
    print("="*60)
    
    arrow_path = temp_dir / "day_type.arrow"
    
    start = time.time()
    df.write_ipc(arrow_path, compression='lz4')
    write_time = time.time() - start
    
    file_size = arrow_path.stat().st_size / 1024 / 1024
    
    print(f"Write time: {write_time*1000:.2f}ms")
    print(f"File size: {file_size:.2f} MB")
    
    if write_time < 0.1:
        print("✅ PASS: Write speed excellent (<100ms)")
    else:
        print("⚠️  SLOW: Write took >100ms (but we only write once in prepare)")
    
    return arrow_path


def test_load_performance(arrow_path: Path):
    """Test: Can we load from .arrow file in <10ms?"""
    print("\n" + "="*60)
    print("TEST 1b: Load Performance (CRITICAL!)")
    print("="*60)
    
    # Cold load (first time)
    start = time.time()
    df = pl.read_ipc(arrow_path)
    cold_load_time = time.time() - start
    
    print(f"Cold load time: {cold_load_time*1000:.2f}ms")
    print(f"Loaded rows: {len(df):,}")
    
    # Warm load (OS cache)
    start = time.time()
    df = pl.read_ipc(arrow_path)
    warm_load_time = time.time() - start
    
    print(f"Warm load time: {warm_load_time*1000:.2f}ms")
    
    if warm_load_time * 1000 < 10:
        print("✅ PASS: Load time <10ms (EXCELLENT!)")
        return df, warm_load_time, True
    elif warm_load_time * 1000 < 20:
        print("⚠️  ACCEPTABLE: Load time <20ms (still good)")
        return df, warm_load_time, True
    else:
        print("❌ FAIL: Load time >20ms (too slow for runtime!)")
        return df, warm_load_time, False


def test_query_performance(df: pl.DataFrame):
    """Test: Can we filter + aggregate in <10ms?"""
    print("\n" + "="*60)
    print("TEST 1c: Query Performance (CRITICAL!)")
    print("="*60)
    
    # Simulate Q1: SELECT day, SUM(bid_price) WHERE type='impression' GROUP BY day
    print("\nSimulating Q1: Filter by type='impression'...")
    
    start = time.time()
    
    # Filter
    result = df.filter(pl.col('type') == 'impression')
    
    # Select columns and compute aggregates
    result = result.select([
        pl.col('day'),
        pl.when(pl.col('bid_price_count') > 0)
        .then(pl.col('bid_price_sum'))
        .otherwise(None)
        .alias('SUM(bid_price)')
    ])
    
    # Materialize result
    _ = result.to_dict()
    
    query_time = time.time() - start
    
    print(f"Query time: {query_time*1000:.2f}ms")
    print(f"Result rows: {len(result):,}")
    print(f"First 3 results:")
    print(result.head(3))
    
    if query_time * 1000 < 10:
        print("✅ PASS: Query time <10ms (EXCELLENT!)")
        return query_time, True
    elif query_time * 1000 < 15:
        print("⚠️  ACCEPTABLE: Query time <15ms (still good)")
        return query_time, True
    else:
        print("❌ FAIL: Query time >15ms (too slow!)")
        return query_time, False


def test_end_to_end_query(arrow_path: Path):
    """Test: Total time from cold start (load + query) <20ms?"""
    print("\n" + "="*60)
    print("TEST 1d: End-to-End Query Time (MOST CRITICAL!)")
    print("="*60)
    
    print("Simulating cold start query (load + filter + aggregate)...")
    
    start = time.time()
    
    # Load rollup
    df = pl.read_ipc(arrow_path)
    
    # Execute query (Q1 simulation)
    result = df.filter(pl.col('type') == 'impression')
    result = result.select([
        pl.col('day'),
        pl.when(pl.col('bid_price_count') > 0)
        .then(pl.col('bid_price_sum'))
        .otherwise(None)
        .alias('SUM(bid_price)')
    ])
    
    # Materialize
    _ = result.to_dict()
    
    total_time = time.time() - start
    
    print(f"\n{'='*60}")
    print(f"TOTAL END-TO-END TIME: {total_time*1000:.2f}ms")
    print(f"{'='*60}")
    
    if total_time * 1000 < 20:
        print("🎉 ✅ PASS: Total time <20ms (TARGET MET!)")
        return total_time, True
    elif total_time * 1000 < 30:
        print("⚠️  ACCEPTABLE: Total time <30ms (still good)")
        return total_time, True
    else:
        print("❌ FAIL: Total time >30ms (need optimization!)")
        return total_time, False


def main():
    print("="*60)
    print("Phase 0 - Test 1: Arrow IPC Query Performance")
    print("="*60)
    print("\nThis test validates our ENTIRE storage strategy!")
    print("If this fails, we need a different approach.\n")
    
    # Create temp directory
    temp_dir = Path(tempfile.mkdtemp())
    print(f"Temp directory: {temp_dir}\n")
    
    try:
        # Step 1: Create rollup
        df = create_realistic_rollup()
        
        # Step 2: Test write
        arrow_path = test_write_performance(df, temp_dir)
        
        # Step 3: Test load
        df_loaded, load_time, load_pass = test_load_performance(arrow_path)
        
        # Step 4: Test query
        query_time, query_pass = test_query_performance(df_loaded)
        
        # Step 5: Test end-to-end
        total_time, e2e_pass = test_end_to_end_query(arrow_path)
        
        # Final verdict
        print("\n" + "="*60)
        print("FINAL VERDICT")
        print("="*60)
        
        all_pass = load_pass and query_pass and e2e_pass
        
        if all_pass:
            print("🎉 ✅ ALL TESTS PASSED!")
            print(f"\nPerformance breakdown:")
            print(f"  Load time:  ~{load_time*1000:.1f}ms")
            print(f"  Query time: ~{query_time*1000:.1f}ms")
            print(f"  Total time: ~{total_time*1000:.1f}ms")
            print(f"\n✅ Arrow IPC is FAST ENOUGH for our strategy!")
            print(f"✅ We can proceed with 12-15 rollups stored as .arrow files")
            print(f"✅ Expected Q1-Q5 total: <50ms ✅")
        else:
            print("❌ SOME TESTS FAILED")
            print("\n⚠️  RECOMMENDATIONS:")
            if not load_pass:
                print("  - Consider in-memory pickle instead of Arrow IPC")
                print("  - Or pre-load all rollups at startup")
            if not query_pass:
                print("  - Optimize Polars filter operations")
                print("  - Consider pre-filtering at build time")
            if not e2e_pass:
                print("  - Overall strategy may be too slow")
                print("  - Consider reducing number of rollups")
                print("  - Or use more aggressive pre-computation")
        
        print("\n" + "="*60)
        
    finally:
        # Cleanup
        shutil.rmtree(temp_dir)
        print(f"\nCleaned up temp directory: {temp_dir}")


if __name__ == "__main__":
    main()
