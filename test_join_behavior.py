import polars as pl

# Test outer join behavior with composite keys
batch1 = pl.DataFrame({
    'advertiser_id': [1, 2, 3],
    'type': ['serve', 'serve', 'click'],
    'row_count': [100, 200, 300]
})

batch2 = pl.DataFrame({
    'advertiser_id': [2, 3, 4],
    'type': ['serve', 'click', 'impression'],
    'row_count': [50, 75, 125]
})

print("Batch 1:")
print(batch1)
print("\nBatch 2:")
print(batch2)

# Join on BOTH keys
keys = ['advertiser_id', 'type']
merged = batch1.join(batch2, on=keys, how="full", suffix="_batch")

print("\nAfter FULL join on ['advertiser_id', 'type']:")
print(merged)
print(f"\nRows: {len(merged)}")

# The question: Does row (4, 'impression') from batch2 appear with NULL batch1 columns?
# Or does it appear with NULL keys?

# Let's check if any keys are NULL
print("\nRows with NULL advertiser_id:")
print(merged.filter(pl.col('advertiser_id').is_null()))

print("\nRows with NULL type:")
print(merged.filter(pl.col('type').is_null()))
