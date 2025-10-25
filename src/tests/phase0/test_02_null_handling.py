#!/usr/bin/env python3
"""
Phase 0 Test 2: NULL Handling Correctness
==========================================

CRITICAL TEST - Wrong NULL handling = -5% per query!

Tests:
1. Does SUM(all-NULL) return NULL (not 0)?
2. Does AVG(all-NULL) return NULL (not NaN)?
3. Does SUM(mixed-NULL) match DuckDB?
4. Does AVG(mixed-NULL) match DuckDB?
5. Does COUNT(*) vs COUNT(column) work correctly?

Success criteria: 100% match with DuckDB behavior
Failure action: Fix aggregation logic with proper NULL handling
"""

import polars as pl
import duckdb
import sys
from pathlib import Path

def test_sum_all_null():
    """Test: SUM of all-NULL column should return NULL (not 0)"""
    print("\n" + "="*60)
    print("TEST 2a: SUM with All-NULL Values")
    print("="*60)
    
    # Create test data
    df = pl.DataFrame({
        'type': ['click', 'click', 'click'],
        'bid_price': [None, None, None]
    })
    
    print("Test data:")
    print(df)
    
    # Polars aggregation (simulating our rollup approach)
    print("\nPolars approach (our rollup):")
    rollup = df.group_by('type').agg([
        pl.col('bid_price').drop_nulls().sum().alias('bid_price_sum'),
        pl.col('bid_price').drop_nulls().count().alias('bid_price_count'),
    ])
    print(rollup)
    
    # Compute SUM with NULL handling
    result_polars = rollup.select([
        pl.col('type'),
        pl.when(pl.col('bid_price_count') > 0)
        .then(pl.col('bid_price_sum'))
        .otherwise(None)
        .alias('SUM(bid_price)')
    ])
    
    print("\nPolars final result (with NULL handling):")
    print(result_polars)
    polars_sum = result_polars['SUM(bid_price)'][0]
    
    # DuckDB comparison
    print("\nDuckDB result:")
    con = duckdb.connect(':memory:')
    con.execute("CREATE TABLE events AS SELECT * FROM df")
    result_duck = con.execute("""
        SELECT type, SUM(bid_price) as "SUM(bid_price)"
        FROM events
        GROUP BY type
    """).fetchdf()
    print(result_duck)
    duckdb_sum = result_duck['SUM(bid_price)'][0]
    
    con.close()
    
    # Compare
    print("\n" + "-"*60)
    print(f"Polars result: {polars_sum}")
    print(f"DuckDB result: {duckdb_sum}")
    
    # Check if both are None/NULL
    polars_is_null = polars_sum is None
    duckdb_is_null = duckdb_sum is None or (isinstance(duckdb_sum, float) and str(duckdb_sum) == 'nan')
    
    if polars_is_null and duckdb_is_null:
        print("✅ PASS: Both return NULL for all-NULL SUM")
        return True
    else:
        print(f"❌ FAIL: Mismatch! Polars NULL={polars_is_null}, DuckDB NULL={duckdb_is_null}")
        return False


def test_avg_all_null():
    """Test: AVG of all-NULL column should return NULL"""
    print("\n" + "="*60)
    print("TEST 2b: AVG with All-NULL Values")
    print("="*60)
    
    # Create test data
    df = pl.DataFrame({
        'type': ['click', 'click', 'click'],
        'bid_price': [None, None, None]
    })
    
    print("Test data:")
    print(df)
    
    # Polars aggregation
    rollup = df.group_by('type').agg([
        pl.col('bid_price').drop_nulls().sum().alias('bid_price_sum'),
        pl.col('bid_price').drop_nulls().count().alias('bid_price_count'),
    ])
    
    # Compute AVG with NULL handling
    result_polars = rollup.select([
        pl.col('type'),
        pl.when(pl.col('bid_price_count') > 0)
        .then(pl.col('bid_price_sum') / pl.col('bid_price_count'))
        .otherwise(None)
        .alias('AVG(bid_price)')
    ])
    
    print("\nPolars result:")
    print(result_polars)
    polars_avg = result_polars['AVG(bid_price)'][0]
    
    # DuckDB comparison
    con = duckdb.connect(':memory:')
    con.execute("CREATE TABLE events AS SELECT * FROM df")
    result_duck = con.execute("""
        SELECT type, AVG(bid_price) as "AVG(bid_price)"
        FROM events
        GROUP BY type
    """).fetchdf()
    print("\nDuckDB result:")
    print(result_duck)
    duckdb_avg = result_duck['AVG(bid_price)'][0]
    
    con.close()
    
    # Compare
    print("\n" + "-"*60)
    print(f"Polars result: {polars_avg}")
    print(f"DuckDB result: {duckdb_avg}")
    
    polars_is_null = polars_avg is None
    duckdb_is_null = duckdb_avg is None or (isinstance(duckdb_avg, float) and str(duckdb_avg) == 'nan')
    
    if polars_is_null and duckdb_is_null:
        print("✅ PASS: Both return NULL for all-NULL AVG")
        return True
    else:
        print(f"❌ FAIL: Mismatch! Polars NULL={polars_is_null}, DuckDB NULL={duckdb_is_null}")
        return False


def test_sum_mixed_null():
    """Test: SUM with some NULL values"""
    print("\n" + "="*60)
    print("TEST 2c: SUM with Mixed NULL Values")
    print("="*60)
    
    # Create test data with mixed NULLs
    df = pl.DataFrame({
        'type': ['impression', 'impression', 'impression', 'impression'],
        'bid_price': [10.5, None, 20.3, None]
    })
    
    print("Test data:")
    print(df)
    
    # Polars aggregation
    rollup = df.group_by('type').agg([
        pl.col('bid_price').drop_nulls().sum().alias('bid_price_sum'),
        pl.col('bid_price').drop_nulls().count().alias('bid_price_count'),
    ])
    
    result_polars = rollup.select([
        pl.col('type'),
        pl.when(pl.col('bid_price_count') > 0)
        .then(pl.col('bid_price_sum'))
        .otherwise(None)
        .alias('SUM(bid_price)')
    ])
    
    print("\nPolars result:")
    print(result_polars)
    polars_sum = result_polars['SUM(bid_price)'][0]
    
    # DuckDB comparison
    con = duckdb.connect(':memory:')
    con.execute("CREATE TABLE events AS SELECT * FROM df")
    result_duck = con.execute("""
        SELECT type, SUM(bid_price) as "SUM(bid_price)"
        FROM events
        GROUP BY type
    """).fetchdf()
    print("\nDuckDB result:")
    print(result_duck)
    duckdb_sum = result_duck['SUM(bid_price)'][0]
    
    con.close()
    
    # Compare
    print("\n" + "-"*60)
    print(f"Polars result: {polars_sum}")
    print(f"DuckDB result: {duckdb_sum}")
    
    # Should be 10.5 + 20.3 = 30.8
    expected = 10.5 + 20.3
    
    polars_correct = abs(polars_sum - expected) < 0.001
    duckdb_correct = abs(duckdb_sum - expected) < 0.001
    match = abs(polars_sum - duckdb_sum) < 0.001
    
    if polars_correct and duckdb_correct and match:
        print(f"✅ PASS: Both return {expected} (ignoring NULLs correctly)")
        return True
    else:
        print(f"❌ FAIL: Expected {expected}, got Polars={polars_sum}, DuckDB={duckdb_sum}")
        return False


def test_avg_mixed_null():
    """Test: AVG with some NULL values"""
    print("\n" + "="*60)
    print("TEST 2d: AVG with Mixed NULL Values")
    print("="*60)
    
    # Create test data
    df = pl.DataFrame({
        'type': ['impression', 'impression', 'impression', 'impression'],
        'bid_price': [10.0, None, 20.0, None]
    })
    
    print("Test data:")
    print(df)
    
    # Polars aggregation
    rollup = df.group_by('type').agg([
        pl.col('bid_price').drop_nulls().sum().alias('bid_price_sum'),
        pl.col('bid_price').drop_nulls().count().alias('bid_price_count'),
    ])
    
    result_polars = rollup.select([
        pl.col('type'),
        pl.when(pl.col('bid_price_count') > 0)
        .then(pl.col('bid_price_sum') / pl.col('bid_price_count'))
        .otherwise(None)
        .alias('AVG(bid_price)')
    ])
    
    print("\nPolars result:")
    print(result_polars)
    polars_avg = result_polars['AVG(bid_price)'][0]
    
    # DuckDB comparison
    con = duckdb.connect(':memory:')
    con.execute("CREATE TABLE events AS SELECT * FROM df")
    result_duck = con.execute("""
        SELECT type, AVG(bid_price) as "AVG(bid_price)"
        FROM events
        GROUP BY type
    """).fetchdf()
    print("\nDuckDB result:")
    print(result_duck)
    duckdb_avg = result_duck['AVG(bid_price)'][0]
    
    con.close()
    
    # Compare
    print("\n" + "-"*60)
    print(f"Polars result: {polars_avg}")
    print(f"DuckDB result: {duckdb_avg}")
    
    # Should be (10 + 20) / 2 = 15.0
    expected = 15.0
    
    match = abs(polars_avg - duckdb_avg) < 0.001
    correct = abs(polars_avg - expected) < 0.001
    
    if match and correct:
        print(f"✅ PASS: Both return {expected} (averaging non-NULL values only)")
        return True
    else:
        print(f"❌ FAIL: Expected {expected}, got Polars={polars_avg}, DuckDB={duckdb_avg}")
        return False


def test_count_star_vs_count_column():
    """Test: COUNT(*) vs COUNT(column) with NULLs"""
    print("\n" + "="*60)
    print("TEST 2e: COUNT(*) vs COUNT(column)")
    print("="*60)
    
    # Create test data
    df = pl.DataFrame({
        'type': ['click', 'click', 'click'],
        'bid_price': [None, None, None]
    })
    
    print("Test data:")
    print(df)
    
    # Polars aggregation
    rollup = df.group_by('type').agg([
        pl.count().alias('row_count'),
        pl.col('bid_price').drop_nulls().count().alias('bid_price_count'),
    ])
    
    print("\nPolars rollup:")
    print(rollup)
    
    # DuckDB comparison
    con = duckdb.connect(':memory:')
    con.execute("CREATE TABLE events AS SELECT * FROM df")
    result_duck = con.execute("""
        SELECT 
            type,
            COUNT(*) as row_count,
            COUNT(bid_price) as bid_price_count
        FROM events
        GROUP BY type
    """).fetchdf()
    print("\nDuckDB result:")
    print(result_duck)
    
    con.close()
    
    # Compare
    print("\n" + "-"*60)
    polars_count_star = rollup['row_count'][0]
    polars_count_col = rollup['bid_price_count'][0]
    duckdb_count_star = result_duck['row_count'][0]
    duckdb_count_col = result_duck['bid_price_count'][0]
    
    print(f"COUNT(*):        Polars={polars_count_star}, DuckDB={duckdb_count_star}")
    print(f"COUNT(column):   Polars={polars_count_col}, DuckDB={duckdb_count_col}")
    
    count_star_match = polars_count_star == duckdb_count_star == 3
    count_col_match = polars_count_col == duckdb_count_col == 0
    
    if count_star_match and count_col_match:
        print("✅ PASS: COUNT(*) = 3, COUNT(column) = 0 (correct NULL handling)")
        return True
    else:
        print("❌ FAIL: COUNT mismatch!")
        return False


def main():
    print("="*60)
    print("Phase 0 - Test 2: NULL Handling Correctness")
    print("="*60)
    print("\nThis test validates we match DuckDB's NULL behavior!")
    print("Wrong NULL handling = -5% per query = FAIL\n")
    
    results = []
    
    # Run all tests
    results.append(("SUM all-NULL", test_sum_all_null()))
    results.append(("AVG all-NULL", test_avg_all_null()))
    results.append(("SUM mixed-NULL", test_sum_mixed_null()))
    results.append(("AVG mixed-NULL", test_avg_mixed_null()))
    results.append(("COUNT(*) vs COUNT(col)", test_count_star_vs_count_column()))
    
    # Final verdict
    print("\n" + "="*60)
    print("FINAL VERDICT")
    print("="*60)
    
    print("\nTest Results:")
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}: {name}")
    
    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)
    
    print(f"\nPassed: {passed_count}/{total_count}")
    
    if passed_count == total_count:
        print("\n🎉 ✅ ALL TESTS PASSED!")
        print("\n✅ Our NULL handling matches DuckDB exactly!")
        print("✅ Aggregation logic is CORRECT!")
        print("\nKey implementation:")
        print("  - Use pl.col().drop_nulls() for sum/count at build time")
        print("  - Use pl.when(count > 0).then(sum).otherwise(None) at query time")
        print("  - This ensures NULL for all-NULL groups, not 0")
        return 0
    else:
        print("\n❌ SOME TESTS FAILED!")
        print("\n⚠️  CRITICAL: Our NULL handling does NOT match DuckDB!")
        print("⚠️  This will cause WRONG RESULTS (-5% per query)")
        print("\n🔧 REQUIRED FIX:")
        print("  - Review aggregation logic")
        print("  - Ensure when/then/otherwise for NULL handling")
        print("  - Test again before proceeding!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
