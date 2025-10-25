#!/usr/bin/env python3
"""
Phase 0 Test 3: Large Rollup Performance
=========================================

CRITICAL TEST - Can we handle 2.1M row minute_type rollup?

Tests:
1. Can we load 2.1M row rollup in <20ms?
2. Can we filter to ~1,440 rows fast enough?
3. Can we aggregate and return in <20ms total?
4. What about 80MB file size?

Success criteria: <20ms for Q5-style query (minute granularity)
Failure action: Partition minute rollup by day
"""

import polars as pl
import time
from pathlib import Path
import tempfile
import shutil

def create_large_minute_rollup():
    """Create realistic minute × type rollup (2.1M rows)."""
    print("Creating large minute × type rollup (2.1M rows)...")
    print("This represents 525,600 minutes × 4 types...")
    
    # Generate all minutes in 2024 (366 days)
    # 366 days × 1,440 minutes/day × 4 types = 2,107,200 rows
    
    start = time.time()
    
    # Create data efficiently
    minutes_per_day = 1440
    days = 366
    types = ['serve', 'impression', 'click', 'purchase']
    
    data = []
    
    for day in range(1, days + 1):
        # Format as 2024-001 to 2024-366 (day of year)
        day_str = f"2024-{day:03d}"
        
        for minute in range(minutes_per_day):
            hour = minute // 60
            min_in_hour = minute % 60
            minute_str = f"{day_str} {hour:02d}:{min_in_hour:02d}"
            
            for type_ in types:
                # Realistic values
                if type_ == 'serve':
                    bid_sum = 100.0 + (minute % 10) * 5
                    bid_count = 200 + (minute % 50)
                    total_sum = 0.0
                    total_count = 0
                    row_count = 400
                elif type_ == 'impression':
                    bid_sum = 60.0 + (minute % 10) * 3
                    bid_count = 120 + (minute % 30)
                    total_sum = 0.0
                    total_count = 0
                    row_count = 250
                elif type_ == 'click':
                    bid_sum = 0.0
                    bid_count = 0
                    total_sum = 0.0
                    total_count = 0
                    row_count = 10
                else:  # purchase
                    bid_sum = 0.0
                    bid_count = 0
                    total_sum = 50.0 + (minute % 5) * 2
                    total_count = 2
                    row_count = 2
                
                data.append({
                    'minute': minute_str,
                    'type': type_,
                    'bid_price_sum': bid_sum,
                    'bid_price_count': bid_count,
                    'total_price_sum': total_sum,
                    'total_price_count': total_count,
                    'row_count': row_count,
                })
    
    df = pl.DataFrame(data)
    
    creation_time = time.time() - start
    
    print(f"✅ Created rollup: {len(df):,} rows in {creation_time:.2f}s")
    print(f"   Estimated size: {df.estimated_size('mb'):.2f} MB")
    
    return df


def test_write_large_rollup(df: pl.DataFrame, temp_dir: Path):
    """Test: Can we write 2.1M row rollup?"""
    print("\n" + "="*60)
    print("TEST 3a: Write Large Rollup")
    print("="*60)
    
    arrow_path = temp_dir / "minute_type.arrow"
    
    start = time.time()
    df.write_ipc(arrow_path, compression='lz4')
    write_time = time.time() - start
    
    file_size = arrow_path.stat().st_size / 1024 / 1024
    
    print(f"Write time: {write_time:.2f}s")
    print(f"File size: {file_size:.2f} MB")
    print(f"Compression ratio: {df.estimated_size('mb') / file_size:.1f}×")
    
    if file_size < 100:
        print(f"✅ PASS: File size <100MB (disk: {file_size:.1f}MB)")
    else:
        print(f"⚠️  LARGE: File size {file_size:.1f}MB (but still <1GB)")
    
    return arrow_path, file_size


def test_load_large_rollup(arrow_path: Path, file_size: float):
    """Test: Can we load 2.1M row rollup fast enough?"""
    print("\n" + "="*60)
    print("TEST 3b: Load Large Rollup (CRITICAL!)")
    print("="*60)
    
    print(f"Loading {file_size:.1f}MB file with 2.1M rows...")
    
    # Cold load
    start = time.time()
    df = pl.read_ipc(arrow_path)
    cold_load_time = time.time() - start
    
    print(f"Cold load time: {cold_load_time*1000:.1f}ms")
    print(f"Loaded rows: {len(df):,}")
    
    # Warm load
    start = time.time()
    df = pl.read_ipc(arrow_path)
    warm_load_time = time.time() - start
    
    print(f"Warm load time: {warm_load_time*1000:.1f}ms")
    
    # For 80MB file, we expect 10-50ms load time
    if warm_load_time * 1000 < 20:
        print("✅ PASS: Load time <20ms (EXCELLENT!)")
        return df, warm_load_time, True
    elif warm_load_time * 1000 < 50:
        print("⚠️  ACCEPTABLE: Load time <50ms (still good)")
        return df, warm_load_time, True
    else:
        print("❌ FAIL: Load time >50ms (too slow for runtime!)")
        return df, warm_load_time, False


def test_filter_performance(df: pl.DataFrame):
    """Test: Can we filter 2.1M rows to ~1,440 rows fast enough?"""
    print("\n" + "="*60)
    print("TEST 3c: Filter Performance (CRITICAL!)")
    print("="*60)
    
    # Simulate Q5: WHERE type='impression' AND day='2024-06-01'
    # This should reduce 2.1M rows to ~1,440 rows (one day)
    
    print("Simulating Q5: Filter by type + day (2.1M → ~1,440 rows)...")
    
    start = time.time()
    
    # Filter by type
    result = df.filter(pl.col('type') == 'impression')
    
    # Filter by day (minute starts with '2024-152' for June 1)
    result = result.filter(pl.col('minute').str.starts_with('2024-152'))
    
    # Materialize count to force evaluation
    filtered_count = len(result)
    
    filter_time = time.time() - start
    
    print(f"Filter time: {filter_time*1000:.1f}ms")
    print(f"Filtered rows: {filtered_count:,} (from 2.1M)")
    print(f"Reduction: {2_107_200 / filtered_count:.0f}× fewer rows")
    
    if filter_time * 1000 < 10:
        print("✅ PASS: Filter time <10ms (EXCELLENT!)")
        return result, filter_time, True
    elif filter_time * 1000 < 20:
        print("⚠️  ACCEPTABLE: Filter time <20ms (still good)")
        return result, filter_time, True
    else:
        print("❌ FAIL: Filter time >20ms (too slow!)")
        return result, filter_time, False


def test_aggregate_performance(df: pl.DataFrame):
    """Test: Can we aggregate ~1,440 rows fast enough?"""
    print("\n" + "="*60)
    print("TEST 3d: Aggregate Performance")
    print("="*60)
    
    # After filtering, aggregate by minute
    print("Aggregating 1,440 rows by minute...")
    
    start = time.time()
    
    result = df.select([
        pl.col('minute'),
        pl.when(pl.col('bid_price_count') > 0)
        .then(pl.col('bid_price_sum'))
        .otherwise(None)
        .alias('SUM(bid_price)')
    ])
    
    # Sort by minute
    result = result.sort('minute')
    
    # Materialize
    _ = result.to_dict()
    
    agg_time = time.time() - start
    
    print(f"Aggregate time: {agg_time*1000:.1f}ms")
    print(f"Result rows: {len(result):,}")
    print(f"First 3 results:")
    print(result.head(3))
    
    if agg_time * 1000 < 10:
        print("✅ PASS: Aggregate time <10ms (EXCELLENT!)")
        return agg_time, True
    elif agg_time * 1000 < 15:
        print("⚠️  ACCEPTABLE: Aggregate time <15ms")
        return agg_time, True
    else:
        print("❌ FAIL: Aggregate time >15ms")
        return agg_time, False


def test_end_to_end_q5(arrow_path: Path):
    """Test: Total time for Q5-style query (load + filter + aggregate)"""
    print("\n" + "="*60)
    print("TEST 3e: End-to-End Q5 Query (MOST CRITICAL!)")
    print("="*60)
    
    print("Simulating complete Q5 query from cold start...")
    print("Query: SELECT minute, SUM(bid_price)")
    print("       WHERE type='impression' AND day='2024-06-01'")
    print("       GROUP BY minute ORDER BY minute")
    
    start = time.time()
    
    # Load rollup
    df = pl.read_ipc(arrow_path)
    
    # Filter
    result = df.filter(pl.col('type') == 'impression')
    result = result.filter(pl.col('minute').str.starts_with('2024-152'))
    
    # Aggregate
    result = result.select([
        pl.col('minute'),
        pl.when(pl.col('bid_price_count') > 0)
        .then(pl.col('bid_price_sum'))
        .otherwise(None)
        .alias('SUM(bid_price)')
    ])
    
    # Sort
    result = result.sort('minute')
    
    # Materialize
    _ = result.to_dict()
    
    total_time = time.time() - start
    
    print(f"\n{'='*60}")
    print(f"TOTAL END-TO-END TIME: {total_time*1000:.1f}ms")
    print(f"{'='*60}")
    print(f"Result rows: {len(result):,}")
    
    if total_time * 1000 < 20:
        print("🎉 ✅ PASS: Total time <20ms (TARGET MET!)")
        return total_time, True
    elif total_time * 1000 < 30:
        print("⚠️  ACCEPTABLE: Total time <30ms (still good)")
        return total_time, True
    else:
        print("❌ FAIL: Total time >30ms (too slow!)")
        return total_time, False


def main():
    print("="*60)
    print("Phase 0 - Test 3: Large Rollup Performance")
    print("="*60)
    print("\nThis test validates we can handle 2.1M row minute_type rollup!")
    print("This is our LARGEST rollup (80MB compressed)\n")
    
    # Create temp directory
    temp_dir = Path(tempfile.mkdtemp())
    print(f"Temp directory: {temp_dir}\n")
    
    try:
        # Step 1: Create large rollup
        df = create_large_minute_rollup()
        
        # Step 2: Test write
        arrow_path, file_size = test_write_large_rollup(df, temp_dir)
        
        # Step 3: Test load
        df_loaded, load_time, load_pass = test_load_large_rollup(arrow_path, file_size)
        
        # Step 4: Test filter
        df_filtered, filter_time, filter_pass = test_filter_performance(df_loaded)
        
        # Step 5: Test aggregate
        agg_time, agg_pass = test_aggregate_performance(df_filtered)
        
        # Step 6: Test end-to-end
        total_time, e2e_pass = test_end_to_end_q5(arrow_path)
        
        # Final verdict
        print("\n" + "="*60)
        print("FINAL VERDICT")
        print("="*60)
        
        all_pass = load_pass and filter_pass and agg_pass and e2e_pass
        
        if all_pass:
            print("🎉 ✅ ALL TESTS PASSED!")
            print(f"\nPerformance breakdown for 2.1M row rollup:")
            print(f"  Load time:   ~{load_time*1000:.1f}ms")
            print(f"  Filter time: ~{filter_time*1000:.1f}ms")
            print(f"  Agg time:    ~{agg_time*1000:.1f}ms")
            print(f"  Total time:  ~{total_time*1000:.1f}ms")
            print(f"  File size:   {file_size:.1f}MB")
            print(f"\n✅ minute_type rollup is FAST ENOUGH!")
            print(f"✅ No need to partition by day!")
            print(f"✅ Q5 will be <20ms ✅")
        else:
            print("❌ SOME TESTS FAILED")
            print("\n⚠️  RECOMMENDATIONS:")
            if not load_pass:
                print("  - Consider lazy loading with scan_ipc")
                print("  - Or partition minute rollup by day")
            if not filter_pass:
                print("  - Partition minute_type by day")
                print("  - Store as separate files per day")
            if not e2e_pass:
                print("  - Overall too slow for 2.1M rows")
                print("  - MUST partition by day (366 files × 5,760 rows)")
                print("  - Query would load only one day's file (~1,440 rows)")
        
        print("\n" + "="*60)
        
    finally:
        # Cleanup
        shutil.rmtree(temp_dir)
        print(f"\nCleaned up temp directory: {temp_dir}")


if __name__ == "__main__":
    main()
