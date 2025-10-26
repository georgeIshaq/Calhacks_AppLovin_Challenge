#!/usr/bin/env python3
"""Test the merge accumulator logic"""

import polars as pl

# Simulate two batches with overlapping keys
batch1 = pl.DataFrame({
    'advertiser_id': [1, 2, 3],
    'type': ['serve', 'serve', 'impression'],
    'row_count': [100, 200, 300],
})

batch2 = pl.DataFrame({
    'advertiser_id': [2, 3, 4],  # 2 and 3 overlap with batch1
    'type': ['serve', 'impression', 'serve'],
    'row_count': [50, 75, 125],
})

print("Batch 1:")
print(batch1)
print("\nBatch 2:")
print(batch2)

# Merge using outer join
keys = ['advertiser_id', 'type']
merged = batch1.join(batch2, on=keys, how="outer", suffix="_batch")
print("\nAfter outer join:")
print(merged)

# Sum the counts
result = merged.with_columns([
    (pl.col('row_count').fill_null(0) + pl.col('row_count_batch').fill_null(0)).alias('row_count')
]).select(keys + ['row_count'])

print("\nFinal result:")
print(result)
print(f"\nTotal rows: {result.height}")

# Expected:
# (1, serve): 100
# (2, serve): 200 + 50 = 250
# (3, impression): 300 + 75 = 375
# (4, serve): 125
# Total: 4 rows
