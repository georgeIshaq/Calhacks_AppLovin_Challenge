#!/usr/bin/env python3
"""
End-to-end test: Query Router + Executor

This tests the complete query execution pipeline:
1. Parse query
2. Route to rollup
3. Execute query
4. Return results

Uses test rollups from rollups_test/
"""

from pathlib import Path
from src.core import QueryRouter, QueryExecutor, RollupLoader
import polars as pl
import time

print('='*60)
print('End-to-End Query Execution Test')
print('='*60)

# Setup test rollups
test_dir = Path('rollups_test')

# Create test rollups if they don't exist
if not (test_dir / 'day_type.arrow').exists():
    print('\nCreating test rollups...')
    test_dir.mkdir(exist_ok=True)
    
    # Create day_type rollup (366 days × 4 types = 1,464 rows)
    data = []
    for day_num in range(1, 367):  # 2024 leap year
        day_str = f'2024-{(day_num-1)//31 + 1:02d}-{(day_num-1)%31 + 1:02d}'
        for event_type in ['impression', 'click', 'purchase', 'serve']:
            # Vary values to make aggregates interesting
            multiplier = {'impression': 10, 'click': 2, 'purchase': 1, 'serve': 5}[event_type]
            data.append({
                'day': day_str,
                'type': event_type,
                'bid_price_sum': 100.0 * multiplier * day_num,
                'bid_price_count': 10 * multiplier,
                'bid_price_min': 5.0 * multiplier,
                'bid_price_max': 50.0 * multiplier,
                'total_price_sum': 500.0 * multiplier * day_num,
                'total_price_count': 10 * multiplier,
                'total_price_min': 10.0 * multiplier,
                'total_price_max': 100.0 * multiplier,
                'row_count': 1000 * multiplier,
            })
    
    df = pl.DataFrame(data)
    df.write_ipc(test_dir / 'day_type.arrow', compression='lz4')
    print(f'  Created day_type: {len(df)} rows')
    
    # Create country_type rollup (4 countries × 4 types = 16 rows)
    country_data = []
    for country in ['US', 'JP', 'GB', 'DE']:
        for event_type in ['impression', 'click', 'purchase', 'serve']:
            multiplier = {'impression': 10, 'click': 2, 'purchase': 1, 'serve': 5}[event_type]
            country_mult = {'US': 3, 'JP': 2, 'GB': 1.5, 'DE': 1}[country]
            country_data.append({
                'country': country,
                'type': event_type,
                'bid_price_sum': 100000.0 * multiplier * country_mult,
                'bid_price_count': 1000 * multiplier,
                'bid_price_min': 5.0 * multiplier,
                'bid_price_max': 50.0 * multiplier,
                'total_price_sum': 500000.0 * multiplier * country_mult,
                'total_price_count': 1000 * multiplier,
                'total_price_min': 10.0 * multiplier,
                'total_price_max': 100.0 * multiplier,
                'row_count': 10000 * multiplier,
            })
    
    country_df = pl.DataFrame(country_data)
    country_df.write_ipc(test_dir / 'country_type.arrow', compression='lz4')
    print(f'  Created country_type: {len(country_df)} rows')

# Initialize components
print('\nInitializing query system...')
loader = RollupLoader(test_dir)
router = QueryRouter()
executor = QueryExecutor(loader)

# Test queries
queries = [
    {
        'name': 'Q1: Daily impressions',
        'query': {
            'select': ['day', {'SUM': 'bid_price'}],
            'where': [{'col': 'type', 'op': 'eq', 'val': 'impression'}],
            'group_by': ['day'],
        }
    },
    {
        'name': 'Q2: Country purchases',
        'query': {
            'select': ['country', {'AVG': 'total_price'}],
            'where': [{'col': 'type', 'op': 'eq', 'val': 'purchase'}],
            'group_by': ['country'],
            'order_by': [{'col': 'AVG(total_price)', 'dir': 'desc'}]
        }
    },
    {
        'name': 'Q3: Type breakdown',
        'query': {
            'select': ['type', {'COUNT': '*'}],
            'group_by': ['type'],
            'order_by': [{'col': 'COUNT(*)', 'dir': 'desc'}]
        }
    },
]

print('\n' + '='*60)
print('Running Queries')
print('='*60)

total_time = 0

for test in queries:
    print(f'\n{test["name"]}:')
    query = test['query']
    
    # Route query
    start = time.perf_counter()
    rollup_name, pattern = router.route_query(query)
    route_time = (time.perf_counter() - start) * 1000
    
    print(f'  Routed to: {rollup_name} ({route_time:.3f}ms)')
    
    # Execute query
    start = time.perf_counter()
    cols, rows = executor.execute(rollup_name, pattern)
    exec_time = (time.perf_counter() - start) * 1000
    
    query_time = route_time + exec_time
    total_time += query_time
    
    print(f'  Execution: {exec_time:.3f}ms')
    print(f'  Total: {query_time:.3f}ms')
    print(f'  Result: {len(rows)} rows')
    
    # Show first 3 rows
    if rows:
        print(f'  Columns: {cols}')
        for i, row in enumerate(rows[:3], 1):
            print(f'    Row {i}: {row}')
        if len(rows) > 3:
            print(f'    ... ({len(rows)-3} more rows)')

print('\n' + '='*60)
print('PERFORMANCE SUMMARY')
print('='*60)
print(f'Total query time: {total_time:.3f}ms')
print(f'Average per query: {total_time/len(queries):.3f}ms')
print()

if total_time < 100:  # All 3 queries under 100ms
    print('✅ EXCELLENT! Well under 1s target')
    print('   Actual queries on real rollups should be similar speed')
else:
    print('⚠️  Slower than expected, but test rollups are small')
    print('   Real rollups may be faster due to pre-loading')

print('='*60)
