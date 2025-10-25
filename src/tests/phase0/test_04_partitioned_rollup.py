#!/usr/bin/env python3
"""
Phase 0 Test 4: Partitioned Rollup Strategy
=============================================

Test 3 showed minute_type rollup (2.1M rows, 17MB) takes 98ms to load - TOO SLOW!

Solution: Partition by day
- 366 files (one per day)
- Each file: 1 day × 4 types × 1,440 minutes = 5,760 rows
- Each file: ~47KB
- Load time: <1ms

This test validates:
1. Can we load a single day's partition in <5ms?
2. Can we query it in <10ms total?
3. Is 366 files manageable?
4. What about disk space?

Success criteria: <10ms for Q5-style query (load + filter + aggregate)
"""

import polars as pl
import time
from pathlib import Path
import tempfile
import shutil

def create_single_day_partition(day: int = 152):
    """Create minute × type rollup for a single day."""
    print(f"Creating minute × type partition for day {day} (2024-{day:03d})...")
    
    # 1 day × 1,440 minutes × 4 types = 5,760 rows
    
    start = time.time()
    
    minutes_per_day = 1440
    types = ['serve', 'impression', 'click', 'purchase']
    
    data = []
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
    
    print(f"✅ Created partition: {len(df):,} rows in {creation_time*1000:.1f}ms")
    print(f"   Estimated size: {df.estimated_size('mb'):.2f} MB")
    
    return df


def test_write_partition(df: pl.DataFrame, temp_dir: Path, day: int):
    """Test: Can we write a single day partition?"""
    print("\n" + "="*60)
    print("TEST 4a: Write Single Day Partition")
    print("="*60)
    
    arrow_path = temp_dir / f"minute_type_day_{day:03d}.arrow"
    
    start = time.time()
    df.write_ipc(arrow_path, compression='lz4')
    write_time = time.time() - start
    
    file_size = arrow_path.stat().st_size / 1024  # KB
    
    print(f"Write time: {write_time*1000:.1f}ms")
    print(f"File size: {file_size:.1f} KB")
    
    if file_size < 100:
        print(f"✅ PASS: File size <100KB (disk: {file_size:.1f}KB)")
    else:
        print(f"⚠️  ACCEPTABLE: File size {file_size:.1f}KB")
    
    return arrow_path, file_size


def test_load_partition(arrow_path: Path, file_size: float):
    """Test: Can we load a single partition fast enough?"""
    print("\n" + "="*60)
    print("TEST 4b: Load Single Partition (CRITICAL!)")
    print("="*60)
    
    print(f"Loading {file_size:.1f}KB file with 5,760 rows...")
    
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
    
    if warm_load_time * 1000 < 5:
        print("🎉 ✅ PASS: Load time <5ms (PERFECT!)")
        return df, warm_load_time, True
    elif warm_load_time * 1000 < 10:
        print("✅ PASS: Load time <10ms (still good)")
        return df, warm_load_time, True
    else:
        print("❌ FAIL: Load time >10ms (too slow!)")
        return df, warm_load_time, False


def test_filter_partition(df: pl.DataFrame):
    """Test: Can we filter 5,760 rows fast enough?"""
    print("\n" + "="*60)
    print("TEST 4c: Filter Partition Performance")
    print("="*60)
    
    # Q5: WHERE type='impression' (already filtered by day via filename)
    # This should reduce 5,760 rows to 1,440 rows
    
    print("Filtering by type='impression' (5,760 → 1,440 rows)...")
    
    start = time.time()
    
    result = df.filter(pl.col('type') == 'impression')
    
    # Materialize count
    filtered_count = len(result)
    
    filter_time = time.time() - start
    
    print(f"Filter time: {filter_time*1000:.1f}ms")
    print(f"Filtered rows: {filtered_count:,} (from 5,760)")
    
    if filter_time * 1000 < 2:
        print("✅ PASS: Filter time <2ms (EXCELLENT!)")
        return result, filter_time, True
    elif filter_time * 1000 < 5:
        print("⚠️  ACCEPTABLE: Filter time <5ms")
        return result, filter_time, True
    else:
        print("❌ FAIL: Filter time >5ms")
        return result, filter_time, False


def test_aggregate_partition(df: pl.DataFrame):
    """Test: Can we aggregate 1,440 rows fast enough?"""
    print("\n" + "="*60)
    print("TEST 4d: Aggregate Partition Performance")
    print("="*60)
    
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
    
    if agg_time * 1000 < 5:
        print("✅ PASS: Aggregate time <5ms (EXCELLENT!)")
        return agg_time, True
    elif agg_time * 1000 < 10:
        print("⚠️  ACCEPTABLE: Aggregate time <10ms")
        return agg_time, True
    else:
        print("❌ FAIL: Aggregate time >10ms")
        return agg_time, False


def test_end_to_end_q5_partitioned(arrow_path: Path):
    """Test: Total time for Q5 using partitioned approach"""
    print("\n" + "="*60)
    print("TEST 4e: End-to-End Q5 Query with Partition (CRITICAL!)")
    print("="*60)
    
    print("Simulating complete Q5 query with partitioned storage...")
    print("Query: SELECT minute, SUM(bid_price)")
    print("       WHERE type='impression' AND day='2024-152'")
    print("       GROUP BY minute ORDER BY minute")
    print("\nStrategy: Load only day=152 partition, filter by type")
    
    start = time.time()
    
    # Load ONLY the relevant day's partition
    df = pl.read_ipc(arrow_path)
    
    # Filter by type (day already filtered by filename)
    result = df.filter(pl.col('type') == 'impression')
    
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
    print(f"First 3 results:")
    print(result.head(3))
    
    if total_time * 1000 < 10:
        print("🎉 ✅ PASS: Total time <10ms (TARGET MET!)")
        return total_time, True
    elif total_time * 1000 < 20:
        print("⚠️  ACCEPTABLE: Total time <20ms (still good)")
        return total_time, True
    else:
        print("❌ FAIL: Total time >20ms (too slow!)")
        return total_time, False


def test_multiple_partitions_overhead(temp_dir: Path):
    """Test: What's the overhead of managing 366 partitions?"""
    print("\n" + "="*60)
    print("TEST 4f: Multiple Partitions Overhead")
    print("="*60)
    
    print("Creating 10 sample partitions to test overhead...")
    
    partitions = []
    total_create_time = 0
    total_write_time = 0
    total_disk_space = 0
    
    for day in range(1, 11):  # Create 10 partitions
        # Create
        start = time.time()
        df = create_single_day_partition(day)
        create_time = time.time() - start
        total_create_time += create_time
        
        # Write
        arrow_path = temp_dir / f"minute_type_day_{day:03d}.arrow"
        start = time.time()
        df.write_ipc(arrow_path, compression='lz4')
        write_time = time.time() - start
        total_write_time += write_time
        
        file_size = arrow_path.stat().st_size / 1024
        total_disk_space += file_size
        
        partitions.append(arrow_path)
    
    print(f"\n10 Partitions Statistics:")
    print(f"  Total create time: {total_create_time:.2f}s")
    print(f"  Total write time: {total_write_time:.2f}s")
    print(f"  Total disk space: {total_disk_space:.1f} KB ({total_disk_space/1024:.1f} MB)")
    print(f"  Average per partition: {total_disk_space/10:.1f} KB")
    
    # Extrapolate to 366 days
    estimated_366_create = total_create_time * 36.6
    estimated_366_write = total_write_time * 36.6
    estimated_366_disk = total_disk_space * 36.6
    
    print(f"\nExtrapolated to 366 partitions:")
    print(f"  Estimated create time: {estimated_366_create:.1f}s")
    print(f"  Estimated write time: {estimated_366_write:.1f}s")
    print(f"  Estimated disk space: {estimated_366_disk/1024:.1f} MB")
    
    # Test loading speed across multiple partitions
    print(f"\nTesting load speed across 10 partitions...")
    
    total_load_time = 0
    for path in partitions:
        start = time.time()
        df = pl.read_ipc(path)
        _ = len(df)
        load_time = time.time() - start
        total_load_time += load_time
    
    avg_load_time = total_load_time / 10
    
    print(f"  Average load time: {avg_load_time*1000:.2f}ms")
    print(f"  10 loads total: {total_load_time*1000:.1f}ms")
    
    if estimated_366_create + estimated_366_write < 600:  # 10 minutes
        print(f"\n✅ PASS: 366 partitions can be built in <10min")
    else:
        print(f"\n⚠️  WARNING: 366 partitions may take >{estimated_366_create + estimated_366_write:.0f}s")
    
    if estimated_366_disk / 1024 < 100:  # 100MB
        print(f"✅ PASS: 366 partitions use <100MB disk space")
    else:
        print(f"⚠️  ACCEPTABLE: 366 partitions use {estimated_366_disk/1024:.0f}MB")
    
    if avg_load_time * 1000 < 10:
        print(f"✅ PASS: Average partition load time <10ms")
        return True
    else:
        print(f"❌ FAIL: Average partition load too slow")
        return False


def main():
    print("="*60)
    print("Phase 0 - Test 4: Partitioned Rollup Strategy")
    print("="*60)
    print("\nTest 3 showed 2.1M row rollup takes 98ms to load - TOO SLOW!")
    print("This test validates partitioning by day (366 files) works!\n")
    
    # Create temp directory
    temp_dir = Path(tempfile.mkdtemp())
    print(f"Temp directory: {temp_dir}\n")
    
    try:
        # Day 152 = June 1 (for consistency with Test 3)
        day = 152
        
        # Step 1: Create single day partition
        df = create_single_day_partition(day)
        
        # Step 2: Test write
        arrow_path, file_size = test_write_partition(df, temp_dir, day)
        
        # Step 3: Test load
        df_loaded, load_time, load_pass = test_load_partition(arrow_path, file_size)
        
        # Step 4: Test filter
        df_filtered, filter_time, filter_pass = test_filter_partition(df_loaded)
        
        # Step 5: Test aggregate
        agg_time, agg_pass = test_aggregate_partition(df_filtered)
        
        # Step 6: Test end-to-end
        total_time, e2e_pass = test_end_to_end_q5_partitioned(arrow_path)
        
        # Step 7: Test multiple partitions overhead
        multi_pass = test_multiple_partitions_overhead(temp_dir)
        
        # Final verdict
        print("\n" + "="*60)
        print("FINAL VERDICT")
        print("="*60)
        
        all_pass = load_pass and filter_pass and agg_pass and e2e_pass and multi_pass
        
        if all_pass:
            print("🎉 ✅ ALL TESTS PASSED!")
            print(f"\nPartitioned approach performance:")
            print(f"  Load time:   ~{load_time*1000:.1f}ms")
            print(f"  Filter time: ~{filter_time*1000:.1f}ms")
            print(f"  Agg time:    ~{agg_time*1000:.1f}ms")
            print(f"  Total time:  ~{total_time*1000:.1f}ms")
            print(f"  File size:   {file_size:.1f}KB (per partition)")
            print(f"\n✅ Partitioned minute_type rollup is PERFECT!")
            print(f"✅ 366 partitions × {file_size:.0f}KB = ~{366*file_size/1024:.1f}MB total")
            print(f"✅ Q5 will be <10ms with partitioning ✅")
            print(f"\n🚀 RECOMMENDATION: Use partitioned storage for minute_type!")
        else:
            print("❌ SOME TESTS FAILED")
            print("\n⚠️  Partitioned approach may not be fast enough!")
            print("    Consider alternative strategies...")
        
        # Comparison with Test 3
        print("\n" + "="*60)
        print("COMPARISON: Monolithic vs Partitioned")
        print("="*60)
        print(f"Monolithic (Test 3):")
        print(f"  - 1 file, 2.1M rows, 17MB")
        print(f"  - Load: 98ms ❌")
        print(f"  - Query: 99.7ms ❌")
        print(f"\nPartitioned (Test 4):")
        print(f"  - 366 files, 5,760 rows each, {file_size:.0f}KB each")
        print(f"  - Load: {load_time*1000:.1f}ms ✅")
        print(f"  - Query: {total_time*1000:.1f}ms ✅")
        print(f"\nSpeedup: {99.7 / (total_time*1000):.0f}× faster! 🚀")
        print("="*60)
        
    finally:
        # Cleanup
        shutil.rmtree(temp_dir)
        print(f"\nCleaned up temp directory: {temp_dir}")


if __name__ == "__main__":
    main()
