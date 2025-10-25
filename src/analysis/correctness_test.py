"""
Numeric Correctness Test

Validates that our aggregate computations match DuckDB exactly,
especially for AVG with NULL handling.

This is CRITICAL - each wrong query costs 5%.
"""

import sys
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np


def test_avg_with_nulls():
    """Test AVG computation with NULL values"""
    print("\n" + "="*80)
    print("TEST: AVG with NULLs")
    print("="*80)
    
    # Create test data
    test_data = pd.DataFrame({
        'country': ['US', 'US', 'US', 'JP', 'JP', 'JP'],
        'value': [10.0, 20.0, None, 5.0, None, None]
    })
    
    print("\nTest data:")
    print(test_data)
    
    # DuckDB computation
    conn = duckdb.connect()
    conn.register('test_data', test_data)
    result_duckdb = conn.execute("""
        SELECT country, AVG(value) as avg_value
        FROM test_data
        GROUP BY country
        ORDER BY country
    """).df()
    
    print("\n✅ DuckDB result:")
    print(result_duckdb)
    
    # Our manual computation (what we'd implement)
    def manual_avg(df):
        result = []
        for country in df['country'].unique():
            subset = df[df['country'] == country]
            values = subset['value'].dropna()  # Remove NULLs
            if len(values) > 0:
                avg = values.sum() / len(values)
            else:
                avg = None
            result.append({'country': country, 'avg_value': avg})
        return pd.DataFrame(result).sort_values('country')
    
    result_manual = manual_avg(test_data)
    
    print("\n✅ Our manual result:")
    print(result_manual)
    
    # Compare
    print("\n" + "="*80)
    print("COMPARISON")
    print("="*80)
    
    duckdb_us = result_duckdb[result_duckdb['country'] == 'US']['avg_value'].values[0]
    manual_us = result_manual[result_manual['country'] == 'US']['avg_value'].values[0]
    
    duckdb_jp = result_duckdb[result_duckdb['country'] == 'JP']['avg_value'].values[0]
    manual_jp = result_manual[result_manual['country'] == 'JP']['avg_value'].values[0]
    
    print(f"\nUS average:")
    print(f"  DuckDB: {duckdb_us}")
    print(f"  Manual: {manual_us}")
    print(f"  Match: {abs(duckdb_us - manual_us) < 1e-9}")
    
    print(f"\nJP average:")
    print(f"  DuckDB: {duckdb_jp}")
    print(f"  Manual: {manual_jp}")
    print(f"  Match: {np.isnan(duckdb_jp) and np.isnan(manual_jp)}")
    
    # Verdict
    us_match = abs(duckdb_us - manual_us) < 1e-9
    jp_match = (np.isnan(duckdb_jp) and np.isnan(manual_jp)) or abs(duckdb_jp - manual_jp) < 1e-9
    
    if us_match and jp_match:
        print("\n✅ PASS: AVG with NULLs computed correctly")
        return True
    else:
        print("\n❌ FAIL: Mismatch in AVG computation!")
        return False


def test_sum_with_nulls():
    """Test SUM with NULL values"""
    print("\n" + "="*80)
    print("TEST: SUM with NULLs")
    print("="*80)
    
    test_data = pd.DataFrame({
        'type': ['impression', 'impression', 'impression', 'click', 'click'],
        'bid_price': [1.5, None, 2.5, None, None]
    })
    
    print("\nTest data:")
    print(test_data)
    
    # DuckDB
    conn = duckdb.connect()
    conn.register('test_data', test_data)
    result_duckdb = conn.execute("""
        SELECT type, SUM(bid_price) as total_bid
        FROM test_data
        GROUP BY type
        ORDER BY type
    """).df()
    
    print("\n✅ DuckDB result:")
    print(result_duckdb)
    
    # Manual
    result_manual = test_data.groupby('type')['bid_price'].sum().reset_index()
    result_manual.columns = ['type', 'total_bid']
    result_manual = result_manual.sort_values('type')
    
    print("\n✅ Our manual result:")
    print(result_manual)
    
    # Compare
    impression_duck = result_duckdb[result_duckdb['type'] == 'impression']['total_bid'].values[0]
    impression_manual = result_manual[result_manual['type'] == 'impression']['total_bid'].values[0]
    
    click_duck = result_duckdb[result_duckdb['type'] == 'click']['total_bid'].values[0]
    click_manual = result_manual[result_manual['type'] == 'click']['total_bid'].values[0]
    
    print(f"\nImpression SUM:")
    print(f"  DuckDB: {impression_duck}")
    print(f"  Manual: {impression_manual}")
    print(f"  Match: {abs(impression_duck - impression_manual) < 1e-9}")
    
    print(f"\nClick SUM (all NULL):")
    print(f"  DuckDB: {click_duck}")
    print(f"  Manual: {click_manual}")
    
    # DuckDB returns NULL for all-NULL sum, pandas returns 0
    # We need to handle this!
    if pd.isna(click_duck):
        print(f"  ⚠️  DuckDB returns NULL, pandas returns 0")
        print(f"  We must explicitly handle all-NULL case!")
        return False
    
    impression_match = abs(impression_duck - impression_manual) < 1e-9
    click_match = abs(click_duck - click_manual) < 1e-9
    
    if impression_match and click_match:
        print("\n✅ PASS: SUM with NULLs computed correctly")
        return True
    else:
        print("\n❌ FAIL: Mismatch in SUM computation!")
        return False


def test_count_star():
    """Test COUNT(*) vs COUNT(column)"""
    print("\n" + "="*80)
    print("TEST: COUNT(*) vs COUNT(column)")
    print("="*80)
    
    test_data = pd.DataFrame({
        'type': ['impression', 'impression', 'impression'],
        'bid_price': [1.5, None, 2.5]
    })
    
    print("\nTest data:")
    print(test_data)
    
    # DuckDB
    conn = duckdb.connect()
    conn.register('test_data', test_data)
    result_duckdb = conn.execute("""
        SELECT 
            COUNT(*) as count_star,
            COUNT(bid_price) as count_bid
        FROM test_data
    """).df()
    
    print("\n✅ DuckDB result:")
    print(result_duckdb)
    
    # Manual
    count_star = len(test_data)
    count_bid = test_data['bid_price'].notna().sum()
    
    print(f"\n✅ Our manual result:")
    print(f"  COUNT(*): {count_star}")
    print(f"  COUNT(bid_price): {count_bid}")
    
    duck_star = result_duckdb['count_star'].values[0]
    duck_bid = result_duckdb['count_bid'].values[0]
    
    print(f"\nComparison:")
    print(f"  COUNT(*): {duck_star} vs {count_star} - {'✅' if duck_star == count_star else '❌'}")
    print(f"  COUNT(column): {duck_bid} vs {count_bid} - {'✅' if duck_bid == count_bid else '❌'}")
    
    if duck_star == count_star and duck_bid == count_bid:
        print("\n✅ PASS: COUNT handled correctly")
        return True
    else:
        print("\n❌ FAIL: COUNT mismatch!")
        return False


def test_empty_groups():
    """Test behavior with empty result sets"""
    print("\n" + "="*80)
    print("TEST: Empty Groups")
    print("="*80)
    
    test_data = pd.DataFrame({
        'type': ['impression', 'impression'],
        'bid_price': [1.5, 2.5]
    })
    
    print("\nTest data (no 'purchase' events):")
    print(test_data)
    
    # DuckDB - query for purchase events that don't exist
    conn = duckdb.connect()
    conn.register('test_data', test_data)
    result_duckdb = conn.execute("""
        SELECT type, SUM(bid_price) as total
        FROM test_data
        WHERE type = 'purchase'
        GROUP BY type
    """).df()
    
    print("\n✅ DuckDB result (WHERE type='purchase'):")
    print(result_duckdb)
    print(f"  Rows: {len(result_duckdb)}")
    
    # Manual
    filtered = test_data[test_data['type'] == 'purchase']
    if len(filtered) > 0:
        result_manual = filtered.groupby('type')['bid_price'].sum().reset_index()
    else:
        result_manual = pd.DataFrame(columns=['type', 'total'])
    
    print(f"\n✅ Our manual result:")
    print(result_manual)
    print(f"  Rows: {len(result_manual)}")
    
    if len(result_duckdb) == len(result_manual) == 0:
        print("\n✅ PASS: Empty groups handled correctly")
        return True
    else:
        print("\n❌ FAIL: Empty groups mismatch!")
        return False


def test_float_precision():
    """Test float precision edge cases"""
    print("\n" + "="*80)
    print("TEST: Float Precision")
    print("="*80)
    
    # Test with very small and very large numbers
    test_data = pd.DataFrame({
        'type': ['impression'] * 5,
        'bid_price': [0.000001, 1000000.0, 0.000002, 999999.9, 0.000003]
    })
    
    print("\nTest data (mixed magnitudes):")
    print(test_data)
    
    # DuckDB
    conn = duckdb.connect()
    conn.register('test_data', test_data)
    result_duckdb = conn.execute("""
        SELECT SUM(bid_price) as total
        FROM test_data
    """).df()
    
    duck_sum = result_duckdb['total'].values[0]
    
    # Manual
    manual_sum = test_data['bid_price'].sum()
    
    print(f"\nDuckDB sum: {duck_sum:.10f}")
    print(f"Manual sum:  {manual_sum:.10f}")
    print(f"Difference:  {abs(duck_sum - manual_sum):.2e}")
    
    # Allow 1e-9 tolerance for float
    if abs(duck_sum - manual_sum) < 1e-6:  # More lenient for float precision
        print("\n✅ PASS: Float precision acceptable")
        return True
    else:
        print("\n❌ FAIL: Significant float precision error!")
        return False


def main():
    print("="*80)
    print("NUMERIC CORRECTNESS VALIDATION")
    print("="*80)
    print("\nThis test validates that our aggregations match DuckDB exactly.")
    print("Any mismatch here will cause wrong query results (-5% per wrong query).")
    
    results = []
    
    # Run all tests
    results.append(("AVG with NULLs", test_avg_with_nulls()))
    results.append(("SUM with NULLs", test_sum_with_nulls()))
    results.append(("COUNT(*) vs COUNT(col)", test_count_star()))
    results.append(("Empty groups", test_empty_groups()))
    results.append(("Float precision", test_float_precision()))
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    total = len(results)
    passed = sum(1 for _, p in results if p)
    
    print(f"\nPassed: {passed}/{total}")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED")
        print("Your aggregate logic matches DuckDB!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} TEST(S) FAILED")
        print("You MUST fix these before proceeding!")
        print("\nCritical issues to address:")
        if not results[1][1]:  # SUM test
            print("  - Handle all-NULL SUM (should return NULL, not 0)")
        return 1


if __name__ == '__main__':
    sys.exit(main())
