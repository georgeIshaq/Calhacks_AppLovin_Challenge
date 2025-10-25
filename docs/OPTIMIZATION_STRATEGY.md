# Optimization Strategy & Analysis

## Executive Summary

**Target**: Beat 65s baseline → achieve <1s total query time (65-650× speedup)

**Core Thesis**: Baseline is CPU-bound doing full table scans. Pre-aggregation + columnar storage + partitioning will achieve 80-300× speedup realistically.

---

## Baseline Performance Analysis

### Current Performance (DuckDB on 19GB, 49 CSV parts)

| Query | Description | Rows | Time | Observation |
|-------|-------------|------|------|-------------|
| Q1 | Daily revenue (all time) | 366 | **13.56s** | Full year aggregation |
| Q2 | Publisher revenue (JP, Oct 20-23) | 1,114 | **12.39s** | Geographic + time filter |
| Q3 | Avg purchase by country | 12 | **11.41s** | Smallest result set |
| Q4 | Event counts by advertiser/type | 6,616 | **12.46s** | High cardinality group |
| Q5 | Minutely spend (June 1) | 1,440 | **15.37s** | Most granular time |

**Total: 65.18 seconds** (~13s per query average)

### Key Observations

1. **Consistent 11-15s regardless of result size** → I/O or full-scan bound
2. **No correlation between output rows and time** → Not result-materialization bound
3. **CPU saturation during queries** → CPU-bound full table scans
4. **All queries filter by time and/or type** → High optimization leverage

---

## Workload Characteristics

### Query Constraints (CRITICAL)

**Supported Operations:**
- `SELECT`: columns, SUM, AVG, COUNT(*)
- `WHERE`: eq, neq, in, between (AND-combined only)
- `GROUP BY`: arbitrary column subsets
- `ORDER BY`: asc/desc on any column

**NOT Supported:**
- LIMIT, HAVING, JOIN, CTE, nested SELECT, DISTINCT
- OR conditions, NOT conditions

**Conclusion**: Pure OLAP fact-table analytics. Classic ad-tech shape.

### Data Schema

```
ts              long (Unix ms)      - Event timestamp
type            ENUM                - serve, impression, click, purchase
auction_id      UUID                - Auction identifier
advertiser_id   int                 - Advertiser ID
publisher_id    int                 - Publisher ID
bid_price       float               - Bid price USD (impressions only)
user_id         int                 - User identifier
total_price     float               - Purchase amount (purchases only)
country         string (ISO)        - Country code
```

### Critical Facts

- **Single table** (no joins) → Denormalized OLAP
- **Process persistence allowed** → Warm caches, memory state
- **19GB raw data** → Must compress/optimize
- **16GB RAM limit** → ~12GB realistically usable
- **100GB disk limit** → Room for 5× data expansion max

---

## Optimization Strategy

### 1. Type-Based Partitioning ⭐⭐⭐

**Rationale**: 80%+ of queries filter by `type` (4 values: serve, impression, click, purchase)

```
data/
  type=impression/
  type=click/
  type=purchase/
  type=serve/
```

**Impact**: Skip 75% of data immediately for most queries

**Test**: Measure `type` predicate frequency in representative queries

### 2. Columnar Storage + Compression ⭐⭐⭐

**Format**: Custom binary, one file per column, 4-16MB blocks

**Encodings per column:**
- `type`: Dictionary + RLE (tiny)
- `day`: Int32 days-since-epoch + Delta-RLE
- `minute/hour`: Uint16 + RLE
- `country`: Dictionary (12 values)
- `advertiser_id`, `publisher_id`: Dictionary or compressed int
- `bid_price`, `total_price`: Float32 + LZ4

**Why**: Queries project small column subsets. Read only needed bytes.

**Test**: Build sample column files, measure bytes read vs CSV

### 3. Sorted Layout + Zone Maps ⭐⭐⭐

**Sort key**: `(day, advertiser_id, type)` within each partition

**Zone maps**: Per-block metadata (min/max/count for day, bid_price, etc.)

**Why**: 
- Binary search on day ranges (all queries filter by time)
- Skip blocks via zone map intersection
- Clustering improves cache locality

**Impact**: Reduce scanned blocks by 95% for typical BETWEEN queries

**Test**: Build sorted layout, measure blocks skipped on date ranges

### 4. Pre-Computed Materialized Aggregates ⭐⭐⭐

**Target aggregates** (pick 4-8 high-leverage combos):

```sql
-- Agg 1: Time + Advertiser + Type
(day, advertiser_id, type) → sum_bid, sum_total, count

-- Agg 2: Time + Publisher + Type  
(day, publisher_id, type) → sum_bid, sum_total, count

-- Agg 3: Time + Country + Type
(day, country, type) → sum_bid, sum_total, count

-- Agg 4: Minute + Type (for granular time queries)
(minute, type) → sum_bid, sum_total, count

-- Agg 5: Advertiser + Type (rollup)
(advertiser_id, type) → sum_bid, sum_total, count

-- Agg 6: Country + Type (small, ~50 rows)
(country, type) → sum_bid, sum_total, count
```

**Storage**: 
- Columnar format (tiny - MB to tens of MB each)
- Keep in-memory hash indexes: `key → row_offset`

**Coverage**: These 6 aggregates cover ~80% of expected query shapes

**Fallback**: For non-matching queries, use optimized columnar scan

**Critical**: AVG = sum/count_non_null (exact DuckDB parity for NULLs)

### 5. Bitmap Indexes ⭐⭐

**Columns**: `type`, `country` (low cardinality)

**Format**: Roaring bitmaps (compressed)

**Why**: Fast AND intersection for multi-predicate WHERE

**Memory**: ~100-500MB for all bitmaps

**Test**: Measure bitmap memory + intersection time

### 6. Query Planning & Routing ⭐⭐

**Decision tree:**

```python
1. Parse JSON → canonical query plan
2. Normalize GROUP BY columns
3. Check if pre-agg table matches:
   - Exact match → use pre-agg
   - Superset match → aggregate pre-agg further
   - No match → check selectivity
4. If WHERE is very selective (est <0.1% rows):
   - Use columnar scan with zone maps
5. Else:
   - Try partial pre-agg + merge
6. Apply ORDER BY in-memory (result sets are small)
```

**Cost model**: `estimated_cost = blocks_to_scan * block_size * cpu_per_byte`

---

## Expected Performance

### Projected Query Times

| Query | Baseline | Projected | Strategy |
|-------|----------|-----------|----------|
| Q1 | 13.5s | **50-200ms** | Pre-agg (day, type) |
| Q2 | 12.4s | **20-100ms** | Pre-agg + country bitmap |
| Q3 | 11.4s | **5-20ms** | Pre-agg (country, type) - tiny |
| Q4 | 12.5s | **100-300ms** | Pre-agg (advertiser, type) |
| Q5 | 15.4s | **30-150ms** | Pre-agg (minute, type) |

**Total: 65s → 200-800ms** (80-300× speedup)

**Conservative target**: <1s total (65× speedup minimum)

---

## Critical Risks & Mitigations

### Risk 1: Pre-Agg Key Mismatch on Holdout ⚠️⚠️⚠️

**Problem**: Holdout queries have different GROUP BY combinations

**Likelihood**: Medium-High

**Mitigation**:
1. Make fallback path fast (optimized columnar + zone maps)
2. Sparse multi-dimensional bitmap index for ad-hoc intersections
3. Partial pre-agg reuse: if query needs `(day, publisher)` but we have `(day, publisher, type)`, aggregate it down
4. Adaptive plan: estimate selectivity, choose scan vs pre-agg dynamically

### Risk 2: Memory Pressure on M2 ⚠️⚠️

**Problem**: 16GB shared RAM, OS overhead, other processes

**Likelihood**: Medium

**Mitigation**:
1. Budget **8GB max** for process (leaves 8GB for OS/cache)
2. Use perfect hashing for dictionaries (50% memory reduction)
3. Keep only hot pre-aggs in memory (~1-2GB)
4. Mmap compressed column blocks (OS handles paging)
5. Monitor RSS during development

**Memory budget:**
- Dictionaries: ~500MB
- Bitmaps: ~500MB
- Pre-agg indexes: ~1GB
- Query execution buffers: ~2GB
- Column block cache: ~4GB
- **Total: ~8GB**

### Risk 3: Numeric Parity (AVG/NULL handling) ⚠️⚠️

**Problem**: AVG computed incorrectly, NULL handling mismatch

**Likelihood**: Low but **catastrophic** (-5% per wrong query)

**Mitigation**:
1. Always compute `AVG = sum_non_null / count_non_null`
2. Test edge cases:
   - All NULLs in group
   - Mixed NULL/non-NULL
   - Empty result sets
   - Float precision (sum of disparate magnitudes)
3. Automated comparison: run every query against DuckDB baseline, assert exact match (tolerance 1e-9 for float rounding only)

### Risk 4: Cold Start Performance ⚠️

**Problem**: Judges restart process between queries

**Likelihood**: Medium

**Mitigation**:
1. Mmap all data files (OS warms cache automatically)
2. Keep small "hot cache" (~500MB) of most-filtered blocks
3. Load dictionaries + zone maps on startup (<1s)
4. Optimize first-query latency separately

---

## Implementation Plan

### Phase 1: Analysis & Validation (Day 1)

**Priority 1 - Predicate Frequency Analysis:**
```bash
python analysis/predicate_stats.py
```
- Count WHERE column frequency across example queries
- Measure cardinality of each column (sample)
- Validate: `type` appears in >60% queries, `day` in >80%

**Priority 2 - Compression Microbenchmark:**
```bash
python analysis/compression_bench.py
```
- Test Dictionary + RLE on `type`, `country`
- Test LZ4 on float columns
- Measure decompress throughput (GB/s)

**Priority 3 - Zone Map Skip Test:**
```bash
python analysis/zone_map_test.py
```
- Build sorted day blocks
- Simulate BETWEEN queries
- Count blocks skipped vs scanned

### Phase 2: Core Engine (Day 2)

**Columnar Converter:**
- Parse CSV → columnar binary format
- Implement encodings (dictionary, RLE, LZ4)
- Write zone maps per block
- Sort by (day, advertiser_id, type)

**Type Partitioning:**
- Split data by type into separate directories
- Measure size reduction per partition

**Pre-Aggregation Builder:**
- Compute 6 key pre-agg tables
- Store in columnar format
- Build in-memory hash indexes

### Phase 3: Query Engine (Day 3)

**Query Parser & Planner:**
- Parse JSON → query plan
- Route to pre-agg or columnar scan
- Cost-based decision

**Execution Engine:**
- Vectorized aggregation over blocks
- Bitmap intersection for WHERE
- In-memory sort for ORDER BY

**Correctness Harness:**
- Compare every query vs DuckDB baseline
- Assert exact result match

### Phase 4: Tuning & Documentation (Judging Day)

**Performance tuning:**
- Profile hotspots
- Adjust block size, compression
- Memory budget verification

**Documentation:**
- Architecture diagram
- Performance comparison table
- Design rationale for each choice

---

## Testing Checklist

### Correctness Tests (MUST PASS)

- [ ] All 5 example queries match DuckDB results exactly
- [ ] AVG with NULLs handled correctly
- [ ] Empty result sets (no rows match WHERE)
- [ ] Extreme values (very large sums, tiny averages)
- [ ] Float precision within 1e-9 tolerance
- [ ] All country codes present (no dictionary misses)

### Performance Tests

- [ ] Each example query <500ms (target <200ms)
- [ ] Total time <1s (target <800ms)
- [ ] Memory usage <8GB RSS
- [ ] Disk usage <80GB total
- [ ] Cold start <2s

### Stress Tests

- [ ] 100 queries in sequence (check for memory leaks)
- [ ] Queries with no WHERE clause (full table aggregation)
- [ ] Queries with 10+ GROUP BY columns
- [ ] Concurrent query execution (if rules allow)

---

## Contingency Plans

### If type partitioning doesn't help:

→ Drop partitioning, focus on aggressive compression + larger sequential blocks

### If memory tight:

→ Keep only dictionaries in memory, stream decompress blocks on-demand

### If representative ≠ holdout:

→ Allow dynamic cache: build pre-aggs on-the-fly for new patterns (first query slower, rest fast)

### If I/O bound:

→ Reduce block size, increase compression (ZSTD level 3), measure sweet spot

### If pre-agg coverage insufficient:

→ Build 2-3 additional pre-aggs during "prepare" phase based on first few holdout queries (if rules allow process to learn)

---

## Success Criteria

### Minimum Viable Win:
- Total time <5s (13× speedup)
- All queries correct
- Clear documentation

### Target Performance:
- Total time <1s (65× speedup)
- All queries <300ms individually
- <8GB memory

### Stretch Goal:
- Total time <500ms (130× speedup)
- All queries <100ms
- Adaptive query planner demo

---

## Key Files & Structure

```
src/
  optimized/
    columnar.py           # Column storage format
    compressor.py         # Encoding/compression
    partitioner.py        # Type-based partitioning
    aggregator.py         # Pre-agg builder
    query_planner.py      # Route queries to pre-aggs
    executor.py           # Vectorized execution
    zone_maps.py          # Block metadata
    bitmaps.py            # Bitmap indexes
    
  analysis/
    predicate_stats.py    # Analyze query patterns
    compression_bench.py  # Test compression perf
    zone_map_test.py      # Test skip efficiency
    
  utils/
    correctness.py        # Compare vs DuckDB
    profiler.py           # Memory/CPU monitoring

data_optimized/
  type=impression/
    day.bin
    advertiser_id.bin
    bid_price.bin
    ...
    
  type=click/
    ...
    
  pre_agg/
    day_advertiser_type.bin
    day_publisher_type.bin
    ...
    
  metadata/
    zone_maps.bin
    dictionaries.bin
    bitmaps.bin
```

---

## References & Notes

- DuckDB baseline: 65.18s total, 11-15s per query
- Dataset: 19GB raw, 49 CSV parts, ~500M rows estimated
- Hardware: M2 MacBook, 16GB RAM, 100GB disk limit
- Query shapes: OLAP aggregations, time-series heavy
- Success metric: Speed + Correctness + Technical Depth + Creativity + Documentation

**Last updated**: Oct 25, 2025
