# Judge Setup & Testing Guide

## Quick Start (3 Steps)

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Prepare Data (One-Time, ~11-15 minutes)
```bash
# Build optimized rollup tables from your data
python3 prepare.py --data-dir ./data --output-dir ./rollups

# Expected output:
# ✅ 11 rollup files created in ./rollups/
# ✅ fallback.duckdb created (sorted DuckDB table)
# ✅ Build complete in ~11-15 minutes
```

### 3. Run Queries (<100ms)
```bash
# Run with default queries (from baseline/inputs.py)
python3 run.py

# Run with your custom query set
python3 run.py --query-file ./queries/judges.json

# Results written to ./results/ directory
```

---

## Detailed Instructions

### Data Preparation

The `prepare.py` script builds optimized data structures from raw CSV files:

```bash
python3 prepare.py --data-dir <path-to-csv-files> --output-dir <output-directory>
```

**Options:**
- `--data-dir`: Directory containing `events_part_*.csv` files (default: `./data`)
- `--output-dir`: Where to store rollup files (default: `./rollups`)
- `--fallback-dir`: Where to store DuckDB fallback (default: `./`)

**What it builds:**
1. **11 Pre-aggregated Rollup Tables** (~340MB total)
   - Optimized for common query patterns
   - Stored as Arrow IPC files (fast loading)
   - Covers: day, hour, minute, week, country, advertiser, publisher dimensions
   
2. **DuckDB Fallback Table** (~2.5GB)
   - Handles complex queries not covered by rollups
   - Physically sorted by (week, country, type) for optimal scan performance
   - No indexes needed - layout is the optimization!

**Expected Performance:**
- Build time: 11-15 minutes on M3 Air, 8-12 minutes on M2 Pro
- Memory usage: 2-4GB peak
- Disk usage: ~3GB total

---

### Running Queries

The `run.py` script executes queries against the optimized structures:

```bash
python3 run.py [OPTIONS]
```

**Options:**
- `--query-file <path>`: JSON file with queries (see format below)
- `--query-dir <path>`: Directory containing `inputs.py` with queries list
- `--output-dir <path>`: Where to write results (default: `./results`)
- `--rollup-dir <path>`: Where rollup files are stored (default: `./rollups`)
- `--fallback-path <path>`: Path to DuckDB fallback (default: `./fallback.duckdb`)

**Query Sources (in priority order):**
1. `--query-file` JSON file (if provided)
2. `--query-dir/inputs.py` (if provided)
3. `./baseline/inputs.py` (default - uses the 5 baseline queries)

**Expected Performance:**
- Query time: ~39ms for 5 queries (8ms per query average)
- Memory usage: ~2GB (pre-loaded rollups)
- 100% query coverage (rollups + fallback)

---

## Query File Formats

### Option 1: Python Format (like baseline/inputs.py)

Create a file named `inputs.py`:

```python
queries = [
    {
        "select": ["day", {"SUM": "bid_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "impression"}],
        "group_by": ["day"],
    },
    {
        "select": ["country", {"AVG": "total_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "purchase"}],
        "group_by": ["country"],
        "order_by": [{"col": "AVG(total_price)", "dir": "desc"}]
    },
    # ... more queries
]
```

Then run:
```bash
python3 run.py --query-dir ./path/to/directory
```

### Option 2: JSON Format

Create a JSON file (e.g., `judges_queries.json`):

```json
[
    {
        "select": ["day", {"SUM": "bid_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "impression"}],
        "group_by": ["day"]
    },
    {
        "select": ["country", {"AVG": "total_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "purchase"}],
        "group_by": ["country"],
        "order_by": [{"col": "AVG(total_price)", "dir": "desc"}]
    }
]
```

Then run:
```bash
python3 run.py --query-file ./judges_queries.json
```

---

## Testing with Different Query Sets

### Example: Your 20 Test Queries

**Setup:**
1. Create `queries/judges.py` with your 20 queries:
   ```python
   queries = [
       # Your 20 queries here
   ]
   ```

2. Run preparation (once):
   ```bash
   python3 prepare.py --data-dir ./data
   ```

3. Test with your queries:
   ```bash
   python3 run.py --query-dir ./queries
   ```

4. Results will be in `./results/`:
   ```
   results/
     q1.csv
     q2.csv
     ...
     q20.csv
   ```

### Example: Compare Baseline vs Optimized

**Run Baseline (for comparison):**
```bash
cd baseline
python3 main.py --data-dir ../data --out-dir ./baseline_results
```

**Run Optimized:**
```bash
python3 run.py --output-dir ./optimized_results
```

**Compare Results:**
Both should produce identical CSV outputs, but optimized should be ~13× faster!

---

## Performance Metrics

Our system provides detailed timing logs:

```bash
python3 run.py
```

**Expected Output:**
```
2024-10-26 - INFO - Loading rollup tables...
2024-10-26 - INFO - ✅ All rollups loaded in 0.45s
2024-10-26 - INFO - Executing query 1/5...
2024-10-26 - INFO - ✅ Query 1 (day_type rollup): 7ms
2024-10-26 - INFO - ✅ Query 2 (day_publisher_country_type rollup): 8ms
2024-10-26 - INFO - ✅ Query 3 (country_type rollup): 9ms
2024-10-26 - INFO - ✅ Query 4 (DuckDB fallback): 10ms
2024-10-26 - INFO - ✅ Query 5 (minute_type rollup): 5ms
2024-10-26 - INFO - 
2024-10-26 - INFO - ===== SUMMARY =====
2024-10-26 - INFO - Total queries: 5
2024-10-26 - INFO - Total time: 39ms
2024-10-26 - INFO - Average: 8ms per query
2024-10-26 - INFO - Rollup coverage: 80% (4/5 queries)
2024-10-26 - INFO - Fallback usage: 20% (1/5 queries)
```

---

## Troubleshooting

### "Could not memory_map compressed IPC file" warning
- **Status**: Harmless warning, does not affect performance
- **Cause**: Arrow IPC files use LZ4 compression
- **Impact**: None - files load correctly via read mode

### Build time longer than expected
- **Expected**: 11-15 min on M3 Air, 8-12 min on M2 Pro
- **If longer**: Check CPU throttling, close other apps
- **Optimization**: Already using all CPU cores, DuckDB tuned for Apple Silicon

### Out of memory during build
- **Peak usage**: ~4GB during build, ~2GB during query
- **Fix**: Close other applications, ensure 8GB+ available RAM
- **Alternative**: Use `data-lite/` for testing (1GB dataset)

### Query results don't match baseline
- **Check**: Both systems should produce identical CSV outputs
- **Validation**: Use `diff` or compare row counts and sums
- **Report**: If discrepancy found, please provide query details

---

## System Architecture

**Two-Tier Query Engine:**

1. **Tier 1: Rollup Tables (80-90% coverage)**
   - Pre-aggregated for common patterns
   - Arrow IPC format (instant load)
   - Sub-10ms query time
   
2. **Tier 2: DuckDB Fallback (10-20% coverage)**
   - Handles complex ad-hoc queries
   - Physically sorted for optimal scans
   - Sub-20ms query time

**Query Routing:**
- Smart router analyzes query structure
- Routes to optimal rollup (if available)
- Falls back to DuckDB for complex queries
- Zero-copy operations where possible

**Key Performance Drivers:**
- ✅ Pre-aggregation eliminates 99% of work
- ✅ Arrow IPC → instant memory mapping
- ✅ Sorted DuckDB table → efficient range scans
- ✅ Polars/DuckDB → Apple Silicon optimized
- ✅ Bounded memory → no OOM crashes

---

## Requirements

- **Python**: 3.9+
- **Dependencies**: See `requirements.txt`
  - polars
  - duckdb
  - pyarrow
- **Hardware**: 8GB+ RAM, 5GB+ disk space
- **OS**: macOS (Apple Silicon optimized), Linux, Windows

---

## Contact & Support

For questions about:
- **Setup**: See troubleshooting section above
- **Query format**: See "Query File Formats" section
- **Performance**: See "Performance Metrics" section
- **Results validation**: Compare with baseline outputs

**Expected Performance:**
- 🏆 Query Speed: 39ms for 5 queries (13× faster than 500ms baseline)
- ✅ Build Time: 11-15 minutes (one-time)
- ✅ Memory Usage: <4GB peak
- ✅ Disk Usage: ~3GB total
