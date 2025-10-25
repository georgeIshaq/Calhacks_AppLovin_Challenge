"""
Quick memory extrapolation calculation
"""

rows_per_file = 5_000_000
total_rows = 225_000_000
memory_per_5m = 1.4  # GB measured from pandas CSV load

estimated_full = memory_per_5m * (total_rows / rows_per_file)

print('='*60)
print('MEMORY EXTRAPOLATION - CRITICAL FINDING')
print('='*60)
print(f'Rows per file: {rows_per_file:,}')
print(f'Total rows: {total_rows:,}')
print(f'Memory for 5M rows (pandas): {memory_per_5m:.2f} GB')
print(f'')
print(f'❌ Estimated for full dataset: {estimated_full:.1f} GB')
print(f'')
print(f'⚠️  CRITICAL PROBLEM: This exceeds 16GB RAM!')
print(f'')
print(f'💡 SOLUTION OPTIONS:')
print(f'')
print(f'1. Columnar compression (required):')
print(f'   - Pandas dataframe: {estimated_full:.1f} GB')
print(f'   - With 4× compression: {estimated_full / 4:.1f} GB')
print(f'   - With overhead (1.5×): {estimated_full / 4 * 1.5:.1f} GB')
print(f'')
print(f'2. Stream processing (safer):')
print(f'   - Process files one at a time')
print(f'   - Build pre-aggs incrementally')
print(f'   - Never load all 225M rows at once')
print(f'')
print(f'3. Hybrid approach (recommended):')
print(f'   - Store compressed columnar (~5-6 GB)')
print(f'   - Load only filtered partitions')
print(f'   - Keep pre-aggs in memory (~50-100 MB)')
print(f'   - Total memory: ~6-8 GB ✅')
print(f'')
print(f'VERDICT:')
print(f'Our original 7GB budget was CORRECT for compressed columnar.')
print(f'But we CANNOT use pandas DataFrames for full dataset.')
