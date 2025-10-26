import polars as pl

# Simulate the exact merge_accumulator logic
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

keys = ['advertiser_id', 'type']
agg_cols = ['row_count']

# Outer join
merged = batch1.join(batch2, on=keys, how="full", suffix="_batch")
print("After outer join:")
print(merged)
print()

# Build merge expressions (with coalesce)
merge_exprs = []
for key in keys:
    key_batch = f"{key}_batch"
    if key_batch in merged.columns:
        print(f"✅ Coalescing {key} with {key_batch}")
        merge_exprs.append(
            pl.coalesce([pl.col(key), pl.col(key_batch)]).alias(key)
        )

for col in agg_cols:
    col_batch = f"{col}_batch"
    merge_exprs.append(
        (pl.col(col).fill_null(0) + pl.col(col_batch).fill_null(0)).alias(col)
    )

# Apply merges
result = merged.with_columns(merge_exprs).select(keys + agg_cols)

print("\n✅ Final result after coalesce:")
print(result)
print(f"\nRows: {len(result)}")
print(f"Rows with NULL advertiser_id: {result.filter(pl.col('advertiser_id').is_null()).height}")
print(f"Rows with NULL type: {result.filter(pl.col('type').is_null()).height}")
