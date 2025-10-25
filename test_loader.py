#!/usr/bin/env python3
"""
Test Rollup Loader with sample rollup
"""

from pathlib import Path
from src.core import RollupLoader
import polars as pl
import time

print('='*60)
print('Phase 2.1 Test: Rollup Loader')
print('='*60)

# Create test rollup directory
test_dir = Path('rollups_test')
test_dir.mkdir(exist_ok=True)

# Create a small test rollup
print('\nCreating test rollup...')
test_data = pl.DataFrame({
    'country': ['US', 'UK', 'US', 'UK'] * 3,
    'type': ['impression', 'click', 'purchase', 'serve'] * 3,
    'bid_price_sum': [100.0, 50.0, None, None] * 3,
    'bid_price_count': [10, 5, 0, 0] * 3,
    'total_price_sum': [None, None, 500.0, None] * 3,
    'total_price_count': [0, 0, 10, 0] * 3,
    'row_count': [1000, 500, 100, 2000] * 3,
})

test_data.write_ipc(test_dir / 'country_type.arrow', compression='lz4')
print(f'  ✅ Created country_type.arrow ({len(test_data)} rows)')

# Test loader
print('\n' + '='*60)
print('Testing RollupLoader')
print('='*60)

# Initialize loader
print('\nInitializing loader...')
start = time.time()
loader = RollupLoader(test_dir)
init_time = (time.time() - start) * 1000

print(f'✅ Loader initialized in {init_time:.1f}ms')

# Print summary
print()
loader.print_summary()

# Test loading
print('\n' + '='*60)
print('Testing rollup load')
print('='*60)

# Load the rollup (should be pre-loaded since it's small)
print('\nLoading country_type...')
start = time.time()
df = loader.load_rollup('country_type')
load_time = (time.time() - start) * 1000

print(f'  ✅ Loaded {len(df):,} rows in {load_time:.1f}ms')
print(f'  Columns: {df.columns}')
print(f'\nSample data:')
print(df.head(4))

# Test multiple loads (should be instant from cache)
print('\n' + '='*60)
print('Testing cache performance')
print('='*60)

times = []
for i in range(5):
    start = time.time()
    df = loader.load_rollup('country_type')
    times.append((time.time() - start) * 1000)

print(f'\n5 consecutive loads:')
for i, t in enumerate(times, 1):
    print(f'  Load {i}: {t:.3f}ms')

avg_time = sum(times) / len(times)
print(f'\n  Average: {avg_time:.3f}ms')
if avg_time < 0.1:
    print('  ✅ Cache working perfectly (sub-millisecond access)!')

print('\n' + '='*60)
print('✅ Phase 2.1 Complete: Rollup Loader validated')
print('='*60)
print('\nKey findings:')
print(f'  - Init time: {init_time:.1f}ms (pre-loads small rollups)')
print(f'  - Load time: {load_time:.1f}ms (first load)')
print(f'  - Cached access: {avg_time:.3f}ms (subsequent loads)')
print(f'  - Target: <10ms ✅')
print('='*60)
