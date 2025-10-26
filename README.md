# CalHacks AppLovin Database Challenge - Optimized Solution

**🏆 1,600× Faster than Baseline on the 5 example queries** - 39ms vs 62 seconds

Pre-aggregated rollup tables + intelligent query routing = sub-100ms query execution.

---

## Quick Start

```bash
pip install -r requirements.txt
python3 prepare.py --data-dir ./data    # One-time build: ~11-15 min on m3 air, should be faster on m2 pro
python3 run.py                           # Query execution on 5 example queries: ~40ms
```

---

## For Judges: Testing Your Queries

```bash
# Step 1: Prepare
python3 prepare.py --data-dir <your-data-path>

# Step 2: Run with your 20 queries (queries should be a json array)
python3 run.py --query-file ./your_queries.json

# Results appear in ./results/q1.csv through q20.csv
```

**Your JSON file should be an array of query objects:**
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
    "group_by": ["country"]
  }
  // ... your remaining 18 queries
]
```

**Expected output:**
- Execution time: <200ms for 20 queries
- Summary showing which rollup handled each query
- Individual CSV files for each query result

📖 See [queries/README.md](./queries/README.md) for complete query format reference.

---

## Performance

| Metric | Baseline (M3 Air) | Optimized | Speedup |
|--------|-------------------|-----------|---------|
| **5 queries** | 61.9s | 39ms | **1,600×** |
| Per query avg | 12.4s | 8ms | **1,550×** |
| Memory | ~4GB | ~10GB build, ~2-4GB query | Within limits |
| Disk | ~20GB raw | ~3GB | 85% smaller |

**Query breakdown m3 mac air:**
- Q1: 13.5s → 7ms (day aggregation)
- Q2: 11.4s → 8ms (publisher-country)
- Q3: 11.0s → 9ms (country average)
- Q4: 11.7s → 10ms (multi-group)
- Q5: 14.2s → 5ms (minute-level)

---

## Architecture

**Two-tier system:**
1. **Rollup tables** (11 pre-aggregated) → handles 80-90% of queries in <10ms
2. **DuckDB fallback** (sorted layout) → handles complex queries in <20ms

**Smart router** → analyzes query → routes to optimal data structure

---

## System Requirements

- Python 3.9+, 12GB+ RAM, 5GB disk
- Dependencies: `polars`, `duckdb`, `pyarrow`
- Apple Silicon optimized (works on Linux/Windows too)

---

## Project Structure

```
prepare.py              # Build rollups (11-15 min)
run.py                  # Execute queries (<100ms)
src/core/               # Query engine
  ├── rollup_builder.py
  ├── query_router.py
  ├── query_executor.py
  └── fallback_executor.py
rollups/                # 11 pre-aggregated tables (~340MB)
fallback.duckdb         # Sorted DuckDB table (~2.5GB)
queries/                # Example query templates
baseline/               # Original baseline for comparison
```

---

## Validation

```bash
python3 validate_setup.py           # Check setup

# Compare with baseline
cd baseline && python3 main.py --data-dir ../data --out-dir ./baseline_results
cd .. && python3 run.py --output-dir ./optimized_results
diff -r baseline/baseline_results optimized_results    # Should be identical
```

---

## Key Optimizations

1. **Pre-aggregation** - 11 rollup tables eliminate 99.9% of data scanning
2. **Incremental folding** - Process data in batches (~10GB peak during build)
3. **Sorted DuckDB** - Physical layout optimization (no indexes needed)
4. **Smart routing** - Automatic selection of optimal rollup per query
5. **Apple Silicon** - Multi-threaded Polars (8 cores) + DuckDB (7 cores)

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| "Rollup directory not found" | Run `python3 prepare.py` first |
| Build >10 min | Normal on M3 Air (11-15 min typical), should be faster on M2 pro|
| Out of memory | Close other apps during build (needs ~10GB free RAM) |
| Wrong results | Run `validate_setup.py` |

---

## Documentation

- [queries/README.md](./queries/README.md) - Query format reference

---

## Architecture Deep Dive

### Data Flow

```
Raw CSV (20GB, 245M rows)
    ↓
prepare.py (11-15 min build)
    ↓
    ├─→ 11 Rollup Tables (Arrow IPC, ~340MB)
    │   ├─ day_type, hour_type, minute_type, week_type
    │   ├─ country_type, advertiser_type, publisher_type
    │   └─ day_country_type, day_advertiser_type, hour_country_type, day_publisher_country_type
    │
    └─→ DuckDB Fallback (sorted table, ~2.5GB)
        Sorted by: (week, country, type)
    ↓
run.py (<100ms execution)
    ↓
    ├─→ Query Router → analyze dimensions & filters
    │
    ├─→ 80-90% queries → Rollup Executor (<10ms)
    │   - Direct lookup in pre-aggregated table
    │   - Apply additional filters
    │   - Return results
    │
    └─→ 10-20% queries → DuckDB Fallback (<400ms)
        - Scan sorted table (efficient range queries)
        - Complex multi-dimensional aggregations
        - Return results
```

### Rollup Table Design

Each rollup pre-aggregates data by specific dimensions and event type:

| Rollup Name | Dimensions | Aggregates | Rows | Use Case |
|-------------|------------|------------|------|----------|
| day_type | day, type | bid_price, total_price, count | ~1,500 | Daily trends (Q1) |
| hour_type | hour, type | bid_price, total_price, count | ~100 | Hourly patterns |
| minute_type | minute, type | bid_price, total_price, count | ~240 | Minute-level (Q5) |
| week_type | week, type | bid_price, total_price, count | ~220 | Weekly trends |
| country_type | country, type | bid_price, total_price, count | ~100 | Geographic (Q3) |
| advertiser_type | advertiser_id, type | bid_price, total_price, count | ~4K | Advertiser performance |
| publisher_type | publisher_id, type | bid_price, total_price, count | ~4K | Publisher performance |
| day_country_type | day, country, type | bid_price, total_price, count | ~40K | Daily geo trends |
| day_advertiser_type | day, advertiser_id, type | bid_price, total_price, count | ~1M | Daily advertiser stats |
| hour_country_type | hour, country, type | bid_price, total_price, count | ~2K | Hourly geo patterns |
| day_publisher_country_type | day, publisher_id, country, type | bid_price, total_price, count | ~900K | Publisher geo (Q2) |

**Aggregates stored per dimension combination:**
- `sum_bid_price`, `sum_total_price` (for SUM queries)
- `count` (for COUNT queries)
- `avg_bid_price`, `avg_total_price` (for AVG queries)
- `min/max` values (for MIN/MAX queries)

### Query Routing Logic

**Decision tree for query routing:**

```python
1. Parse query dimensions and filters
2. Extract: group_by dimensions + WHERE filter columns
3. Match against rollup definitions:
   
   If group_by == ["day"] and "type" in filters:
       → Use day_type rollup
   
   If group_by == ["day", "publisher_id", "country"] and "type" in filters:
       → Use day_publisher_country_type rollup
   
   If no exact match:
       → Use DuckDB fallback
       
4. Execute query on selected data source
5. Apply additional filters (date ranges, etc.)
6. Return results
```

### Build Process Details

**Phase 1: Rollup Building (10-11 min)**

```python
For each CSV file (49 files):
    1. Read batch with PyArrow (256MB blocks, 8 threads)
    2. Add time dimensions (day, hour, minute, week)
    3. Aggregate by all 11 rollup dimensions simultaneously
    4. Every 50 batches:
        - Merge with accumulator (incremental folding)
        - Keep memory bounded (~10GB peak)
    5. Write final rollups as Arrow IPC (LZ4 compression)
```

**Phase 2: DuckDB Fallback (4-5 min)**

```python
1. Read all CSVs into DuckDB table
2. Add time dimensions (day, hour, minute, week)
3. Sort by (week, country, type) - physical layout optimization
4. Write to fallback.duckdb (~2.5GB)
```

**Why this is fast:**
- Single pass through data (read once)
- Parallel processing (8 threads for CSV reading)
- Incremental folding prevents memory explosion
- Arrow IPC = zero-copy reads during query time

### Storage Format

**Arrow IPC (Rollups):**
- Columnar format (only read columns needed)
- LZ4 compression (3:1 ratio, 3GB/s decompression)
- Memory-mappable (instant load, no deserialization)
- Total size: ~340MB for all 11 rollups

**DuckDB (Fallback):**
- Row-oriented sorted table
- Sorted by (week, country, type) enables range scans
- No indexes needed (physical sort is the optimization)
- Total size: ~2.5GB

### Query Execution

**Rollup Query (80-90% of queries, <10ms):**

```python
1. Load rollup from disk (memory-mapped, instant)
2. Filter by type (if specified)
3. Filter by date range (if specified)
4. Filter by country/advertiser/publisher (if specified)
5. Select aggregates (already computed)
6. Apply ORDER BY (if specified)
7. Return results

Example Q1 (day aggregation):
- Load day_type rollup (~1,500 rows)
- Filter: type == "impression"
- Select: day, sum_bid_price
- Result: 366 rows, 7ms
```

**DuckDB Query (10-20% of queries, <400ms):**

```python
1. Connect to fallback.duckdb
2. Execute SQL query with WHERE clauses
3. Leverage sorted layout for efficient scans
4. Return results

Example Q4 (multi-group):
- Scan fallback table (sorted helps)
- Group by: advertiser_id, type
- Aggregate: count(*)
- Order by: count DESC
- Result: 6,616 rows, 10ms
```

### Performance Analysis

**Why 1,600× faster than baseline:**

| Factor | Baseline | Optimized | Speedup |
|--------|----------|-----------|---------|
| Data scanned | 245M rows | 1.5K-900K rows | 100-1000× less |
| Aggregation | On-demand | Pre-computed | Instant |
| I/O | Full CSV scan | Memory-mapped rollups | 100× faster |
| Parsing | Parse 20GB | Parse 340MB | 60× less |
| **Total** | **62 seconds** | **39ms** | **1,600×** |

**Per-query breakdown:**

- **Q1** (13.5s → 7ms): Scan 245M rows → lookup 366 rows in day_type
- **Q2** (11.4s → 8ms): Scan 245M rows → lookup ~1K rows in day_publisher_country_type
- **Q3** (11.0s → 9ms): Scan 245M rows → lookup 12 rows in country_type
- **Q4** (11.7s → 10ms): Full scan + group → DuckDB on sorted 245M (still faster)
- **Q5** (14.2s → 5ms): Scan 245M rows → lookup 1,440 rows in minute_type

### Design Decisions

**1. Why 11 rollups instead of more/fewer?**
- Analyzed query patterns from baseline
- Covered 80-90% with pre-aggregation
- Diminishing returns beyond 11 (more = slower build, marginal query benefit)
- Total storage: 340MB (well under disk limits)

**2. Why Arrow IPC instead of Parquet?**
- Memory-mappable (no deserialization overhead)
- Faster load time (450ms vs 2-3s for Parquet)
- Zero-copy reads (direct pointer to mmap'd memory)
- LZ4 compression still gives 3:1 ratio

**3. Why DuckDB fallback instead of more rollups?**
- Some queries too complex for pre-aggregation (multi-group, no type filter)
- DuckDB with sorted layout is "fast enough" (<400ms)
- Simpler system (11 rollups + fallback vs 50+ rollups)

**4. Why sort DuckDB by (week, country, type)?**
- Week: coarse partitioning (52 values)
- Country: mid-level partitioning (~30 values)
- Type: fine-grained partitioning (4 values)
- Enables efficient range scans for date/country filters
- No indexes needed (physical layout is the index)

**5. Why incremental folding during build?**
- Without: Memory grows unbounded (OOM on large datasets)
- With: Fold every 50 batches, keep memory at ~10GB peak
- Slight performance cost (merging overhead) but stays within memory limits

### Constraints Met

| Constraint | Requirement | Our System | Status |
|------------|-------------|------------|--------|
| RAM | ≤16 GB | ~10GB build, ~2GB query | ✅ Within |
| Disk | ≤100 GB | ~3GB (rollups + DuckDB) | ✅ Within |
| Network | No access during run | None (all local) | ✅ Compliant |
| Query time | <1s target | 39ms (0.039s) | ✅ 25× under |

---

**Built for CalHacks 2025** | [Polars](https://pola.rs/) + [DuckDB](https://duckdb.org/) + [Arrow](https://arrow.apache.org/)
