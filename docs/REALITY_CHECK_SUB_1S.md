# Reality Check: Achieving Sub-1s Performance

## THE BRUTAL TRUTH

**Current baseline**: 65.18s  
**Target**: <1s total (65× speedup required)  
**Realistic with current approach**: 2-3s (21-32× speedup)  
**Gap**: We're 2-3× TOO SLOW

---

## MAIN PROBLEMS 🚨

### Problem #1: Decompression is NOT Free
```
Bottleneck: Q1 and Q5 scan 73M rows
- Compressed size: ~2GB
- Decompression @ 3GB/s: 700ms
- Aggregation overhead: 100-200ms
- TOTAL: ~900ms PER QUERY

❌ 900ms × 2 queries = 1.8s already exceeds budget!
```

### Problem #2: Full Scans Kill Performance
```
Queries that CANNOT hit pre-aggs:
- Q1: Daily revenue by type (366 days × 4 types = 1,464 groups)
  → Must scan 73M serve/purchase rows
  → Time: ~900ms

- Q5: Minutely spend (1,440 minutes × 4 types = 5,760 groups)
  → Must scan 73M serve rows  
  → Time: ~900ms

Combined: 1.8s just for these 2 queries!
```

### Problem #3: Build Time is Slow
```
Sequential CSV reading:
- 49 files × 7.4s each = 363s (6 minutes)
- With processing overhead: 12 minutes

Even with 4-core parallelization: 3 minutes
Even with Polars (10× faster): 1 minute

⚠️ Still might get penalized if >1 minute
```

### Problem #4: NULL Handling Bug
```
Pandas: SUM(all-NULL) = 0
DuckDB: SUM(all-NULL) = NULL

Impact: -5% per wrong query
Must fix with explicit checks (adds overhead)
```

---

## KEY INSIGHTS 💡

### Insight #1: Pre-Aggs Are NOT Enough
```
Pre-agg hit rate by query:
✅ Q2: 100% hit (country, type) → <10ms ✅
✅ Q3: 100% hit (country, type) → <5ms ✅
✅ Q4: 100% hit (advertiser, type) → <50ms ✅
❌ Q1: 0% hit (daily granularity too fine) → 900ms ❌
❌ Q5: 0% hit (minute granularity too fine) → 900ms ❌

3/5 queries are fast, but 2/5 dominate total time!
```

### Insight #2: The Real Enemy is Data Volume
```
DuckDB is slow because:
- Loads 19GB CSV into memory: ~20s
- Full table scans: ~10s per query
- No indexing on CSV data

Our approach is slow because:
- Decompression overhead: ~700ms per 2GB
- Still doing aggregations: ~100-200ms
- Limited by CPU/memory bandwidth

We're 3× faster than DuckDB, but not 65× faster!
```

### Insight #3: Type Partitioning Has Limited Value
```
Type distribution:
- serve: 65% (146M rows)
- impression: 33% (74M rows)  
- click: 1.6% (3.6M rows)
- purchase: 0.01% (22K rows)

Partitioning saves:
- Q1 (serve+purchase): Reads 146M instead of 225M (35% reduction)
- Q5 (serve only): Reads 146M instead of 225M (35% reduction)

But 35% reduction on 900ms = 585ms still > budget!
```

### Insight #4: Memory is Actually THE Constraint
```
What we learned:
❌ Cannot load 225M rows in pandas: 63GB required
✅ Must use columnar compression: ~6GB achievable
✅ 7GB memory budget is TIGHT but possible

Implication: Cannot keep everything in memory
Must stream/decompress on-demand
```

---

## CRITICAL DATA PROPERTIES 📊

### Property #1: High Temporal Skew
```
Queries love time dimensions:
- day: 80% of queries (366 unique values)
- minute: 20% of queries (1,440 values)
- week: Possible future query (52 values)

But: Too fine-grained for pre-aggs to help!
```

### Property #2: Extreme Type Skew
```
serve: 65.6% (dominated by high-volume)
impression: 32.8%
click: 1.6%
purchase: 0.01%

Filtering by type gives 35-99% reduction
But Q1/Q5 need serve (65%) so still big!
```

### Property #3: Sparse Numeric Columns
```
bid_price: Only present for serve/impression
total_price: Only present for purchase (0.01% of rows)

Nullability matters:
- SUM(bid_price) WHERE type='click' → all NULL → must return NULL!
- Adds conditional overhead to every aggregation
```

### Property #4: Pre-Agg Size Explosion
```
Cardinality analysis:
- (day, type): 366 × 4 = 1,464 ✅ tiny
- (country, type): 12 × 4 = 48 ✅ tiny
- (advertiser, type): 1,654 × 4 = 6,616 ✅ small
- (minute, type): 1,440 × 4 = 5,760 ✅ small
- (publisher, type): 1,114 × 4 = 4,456 ✅ small

BUT:
- (hour, day, type): 24 × 366 × 4 = 35,136 ⚠️ getting big
- (minute, day, type): 1,440 × 366 × 4 = 2.1M ❌ too big!

Cannot pre-agg everything without explosion!
```

---

## WHY WE'RE STUCK AT 2-3s

### Query-by-Query Breakdown:

**Q1: Daily revenue by type**
- Needs: 73M rows (serve + purchase)
- Current approach:
  - Decompress 2GB: 700ms
  - Aggregate 366 groups: 100ms
  - Total: 800ms
- **Cannot improve much without pre-computed daily rollups**

**Q2: Publisher spend in Japan**
- Pre-agg hit: (country='JP', type='serve')
- Lookup + aggregate: <10ms ✅
- **Already optimal!**

**Q3: Avg purchase price by country**  
- Pre-agg hit: (country, type='purchase')
- Lookup: <5ms ✅
- **Already optimal!**

**Q4: Advertiser event counts**
- Pre-agg hit: (advertiser_id, type)
- Lookup 6,616 rows: 50ms ✅
- **Already optimal!**

**Q5: Minutely spend**
- Needs: 146M rows (serve only)
- Current approach:
  - Decompress 1.5GB: 500ms
  - Aggregate 1,440 groups: 200ms
  - Total: 700ms
- **Cannot improve without pre-computed minutely rollups**

**TOTAL: 800 + 10 + 5 + 50 + 700 = 1,565ms**

---

## WHAT WOULD IT TAKE TO HIT <1s?

### Option 1: Pre-Compute EVERYTHING (Memory Explosion)
```
Build pre-aggs for:
- (day, type): 1.5KB ✅
- (minute, type): 6KB ✅  
- (day, minute, type): 2.1MB ❌ too big!

Problem: Combinatorial explosion
Can't pre-compute all possible groupings
```

### Option 2: Use Faster Compression (CPU Bottleneck)
```
LZ4: 3GB/s decompression
ZSTD: 1GB/s (slower)
Snappy: 4GB/s (marginal gain)
Uncompressed: 10GB disk I/O bottleneck

Switching to Snappy:
- 700ms → 525ms (saves 175ms)
- Still: 800ms + 700ms = 1.5s ❌

Not enough!
```

### Option 3: Use Columnar Engine (Abandon Pandas)
```
DuckDB scan: 10s per query
Polars scan: ~1s per query
Arrow/Parquet: ~500ms per query

Could use:
- Parquet files with pre-computed statistics
- Column pruning + predicate pushdown
- Dictionary encoding on read

But: This defeats the purpose of "beating DuckDB"!
```

### Option 4: Incremental Pre-Agg Strategy (THE ANSWER?)
```
Observation: Q1 needs (day, type), Q5 needs (minute, type)

What if we pre-agg at MULTIPLE granularities?
- Daily rollups: 1.5KB
- Minutely rollups: 6KB
- Hourly rollups: 0.5KB

Total memory: <10KB
Build time: +30s (acceptable)

Query performance:
- Q1: Lookup daily rollup → <5ms ✅
- Q5: Lookup minutely rollup → <10ms ✅

NEW TOTAL: 5 + 10 + 5 + 50 + 10 = 80ms ✅✅✅
```

---

## THE WINNING STRATEGY 🎯

### Core Insight:
**Don't scan data at query time. Pre-compute at MULTIPLE temporal granularities.**

### New Architecture:

```
Pre-Aggregation Layers:
=======================

Layer 1: Raw Columnar (type-partitioned, LZ4)
- serve/: 146M rows, 1.5GB compressed
- impression/: 74M rows, 800MB compressed  
- click/: 3.6M rows, 40MB compressed
- purchase/: 22K rows, 200KB compressed

Layer 2: Temporal Pre-Aggs (in-memory, <1MB total)
- daily_by_type: (day, type) → 1,464 rows
- minutely_by_type: (minute, type) → 5,760 rows
- hourly_by_type: (hour, type) → 96 rows

Layer 3: Dimensional Pre-Aggs (in-memory, <1MB total)
- country_by_type: (country, type) → 48 rows
- advertiser_by_type: (advertiser_id, type) → 6,616 rows
- publisher_by_type: (publisher_id, type) → 4,456 rows

Layer 4: Hybrid Pre-Aggs (for complex queries, <5MB total)
- daily_country_type: (day, country, type) → 17,568 rows
- daily_advertiser_type: (day, advertiser, type) → 241,176 rows
```

### Query Routing Logic:

```python
def route_query(query):
    if has_time_grouping(query) and has_type_filter(query):
        # Q1: GROUP BY day + type filter
        return lookup_temporal_preagg('daily_by_type')
        # Time: <5ms ✅
    
    elif has_minute_grouping(query):
        # Q5: GROUP BY minute
        return lookup_temporal_preagg('minutely_by_type')
        # Time: <10ms ✅
    
    elif has_country_grouping(query) or has_country_filter(query):
        # Q2, Q3: country in query
        return lookup_dimensional_preagg('country_by_type')
        # Time: <5ms ✅
    
    elif has_advertiser_grouping(query):
        # Q4: GROUP BY advertiser_id
        return lookup_dimensional_preagg('advertiser_by_type')
        # Time: <50ms ✅
    
    else:
        # Fallback: scan columnar data
        return scan_columnar_partitions(query)
        # Time: 500-1000ms (rare case)
```

### Build Process (Optimized):

```
Phase 1: Parallel CSV Reading (4 cores)
- Read 49 files in parallel: 7.4s / 4 = 2s
- Parse with Polars (10× faster): 0.2s per batch
- Total: ~10s

Phase 2: Columnar Conversion + Type Partitioning
- Write LZ4 compressed columns: 5s
- Build dictionaries: 1s
- Total: ~6s

Phase 3: Pre-Aggregation (all layers)
- Stream through data once: 10s
- Build all pre-aggs simultaneously: +5s
- Total: ~15s

Phase 4: Index Creation
- Build lookup indexes: 1s

TOTAL BUILD TIME: ~32s ✅ (well under 1 minute)
```

---

## REVISED PERFORMANCE TARGETS

### Conservative (90% confidence):
```
Q1: 5ms (daily pre-agg)
Q2: 10ms (country pre-agg)
Q3: 5ms (country pre-agg)
Q4: 50ms (advertiser pre-agg, 6K rows)
Q5: 10ms (minutely pre-agg)

TOTAL: 80ms ✅ beats <1s by 12×!
```

### If Pre-Agg Misses (fallback):
```
Q1: 800ms (scan 73M rows)
Q2: 10ms (pre-agg hit)
Q3: 5ms (pre-agg hit)
Q4: 50ms (pre-agg hit)
Q5: 700ms (scan 146M rows)

TOTAL: 1,565ms ≈ 1.6s (still 41× speedup)
```

### Best Case (all pre-agg hits):
```
All queries: <10ms each
TOTAL: 50ms ✅ beats <1s by 20×!
```

---

## CRITICAL SUCCESS FACTORS

### Must Have:
1. ✅ Multi-granularity pre-aggregations (daily, minutely, hourly)
2. ✅ Smart query router (pattern matching)
3. ✅ NULL-safe aggregation functions
4. ✅ Fast build process (<1 minute)
5. ✅ Memory budget adherence (<8GB)

### Nice to Have:
1. Columnar compression (fallback for misses)
2. Zone maps (skip partitions)
3. Bitmap indexes (fast filtering)
4. Parallel query execution

### Can Skip:
1. Complex query optimization
2. Cost-based query planning
3. Adaptive indexing
4. Caching layers

---

## FINAL VERDICT

### Can we hit <1s? **YES! ✅**

**How?**
- Pre-aggregate at ALL temporal granularities (daily, minutely, hourly)
- Build dimensional pre-aggs (country, advertiser, publisher)
- Intelligent query routing (pattern matching)
- Fallback to columnar scans for edge cases

**Expected performance:**
- **Best case**: 50ms (100× faster than 5s) ✅
- **Average case**: 80ms (61× faster than 5s) ✅  
- **Worst case**: 1.6s (3× slower than target) but still 41× speedup ✅

**Risk level:** 🟢🟢🟢🟡⚪ (Medium)
- High confidence for known query patterns
- Medium confidence for holdout queries (might not hit pre-aggs)
- Low risk of catastrophic failure (fallback always works)

**The key insight**: Stop thinking about "optimizing scans" and start thinking about "eliminating scans entirely" through aggressive pre-computation.
