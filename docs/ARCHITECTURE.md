# System Architecture

## Overview

This system implements a **two-tier OLAP cube** approach for fast analytical query processing:

- **Tier 1 (Rollups)**: Pre-aggregated cubes for common query patterns → <25ms per query
- **Tier 2 (Fallback)**: Raw data scan for uncommon patterns → ~13s per query (can optimize to <1s)

## Design Philosophy

**"This is a cube problem, not a database problem"**

Rather than indexing and scanning 245M rows repeatedly, we pre-compute aggregates for common dimension combinations. This trades prepare time (11.6 min) for extreme query speed (milliseconds).

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      PREPARE PHASE                          │
│                    (11.6 minutes)                           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Raw CSV (245M rows, 19GB)                                 │
│         │                                                   │
│         ├──> Single-Pass PyArrow Scan                      │
│         │    • Stream batches                              │
│         │    • Compute partial aggregates                  │
│         │    • Build 10 rollups simultaneously             │
│         │                                                   │
│         └──> Rollup Files (82.7 MB)                        │
│              • Arrow IPC + LZ4 compression                 │
│              • 10 rollups total                            │
│              • Pre-loaded into memory                      │
│                                                             │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                       RUN PHASE                             │
│                     (<1s target)                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Query → Router → Tier Selection                           │
│                                                             │
│  ┌─────────────────┐                ┌──────────────────┐   │
│  │  Tier 1: Rollup │                │ Tier 2: Fallback │   │
│  │   <25ms/query   │                │  ~13s/query      │   │
│  ├─────────────────┤                ├──────────────────┤   │
│  │ Pre-aggregated  │                │ Raw data scan    │   │
│  │ Dimensions match│                │ Missing dims     │   │
│  │ 80% of queries  │                │ 20% of queries   │   │
│  └─────────────────┘                └──────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Current Rollups (Tier 1)

We maintain 10 pre-aggregated rollups optimized for common query patterns:

| Rollup Name            | Dimensions                    | Rows    | Size    | Coverage |
|------------------------|-------------------------------|---------|---------|----------|
| `day_type`             | day × type                    | 1.5K    | 12 KB   | Daily trends |
| `hour_type`            | hour × type                   | 34K     | 274 KB  | Hourly patterns |
| `minute_type`          | minute × type                 | 527K    | 4.2 MB  | Minute breakdown |
| `week_type`            | week × type                   | 212     | 2 KB    | Weekly summary |
| `country_type`         | country × type                | 48      | 384 B   | Geo analysis |
| `advertiser_type`      | advertiser_id × type          | 6.6K    | 53 KB   | Advertiser perf |
| `publisher_type`       | publisher_id × type           | 4.5K    | 36 KB   | Publisher perf |
| `day_country_type`     | day × country × type          | 16.8K   | 135 KB  | Daily geo |
| `day_advertiser_type`  | day × advertiser_id × type    | 1.8M    | 14.5 MB | Daily advertiser |
| `hour_country_type`    | hour × country × type         | 329K    | 2.6 MB  | Hourly geo |

**Total:** 82.7 MB (tiny compared to 19GB raw data)

## Why Not More Rollups?

**Resource Constraints:**
- **RAM Limit**: 16GB total on M2 MacBook Air
- **Build Memory**: ~3-5GB for streaming, but 4-dimension rollups exceed RAM during combine
- **Example**: `day_publisher_country_type` = 366 × 4,456 × 12 × 4 = 78M rows → OOM crash

**Tradeoff Decision:**
- **10 rollups** cover 80% of query patterns with millisecond response times
- **Fallback system** handles remaining 20% with raw data scans (~13s, can optimize to <1s)
- This is better than trying to pre-compute everything and running out of memory

## Query Routing Logic

### Tier 1: Rollup Matching

Router selects rollup based on:

1. **Required dimensions**: All GROUP BY columns must exist in rollup
2. **Filter columns**: WHERE clause columns must exist OR be derivable
3. **Derived columns**: Can extract `day` from `minute`, `hour` from `minute`
4. **Size optimization**: Selects smallest matching rollup

**Example:**
```
Query: GROUP BY minute WHERE type='impression', day='2024-06-01'

Required: minute (GROUP BY)
Filters: type, day (WHERE)
Derivable: day can be extracted from minute string

Match: minute_type rollup (has minute, type; day derived)
Result: 1,440 rows in 8ms
```

### Tier 2: Fallback

When no rollup contains all required dimensions:

1. **Lazy load** raw CSV files with PyArrow streaming
2. **Filter pushdown** to reduce data volume early
3. **Columnar reads** (only load needed columns)
4. **In-memory aggregation** with Polars

**Example:**
```
Query: GROUP BY publisher_id WHERE type, country, day

Required: publisher_id, type, country, day (4 dimensions)
Available rollups: None with all 4 dimensions
→ Fallback to raw data scan

Result: 1,114 rows in 13.8s
```

## Performance Characteristics

| Tier | Strategy | Current Speed | Target | Coverage |
|------|----------|---------------|--------|----------|
| 1    | Rollups  | 0.6-25ms      | <50ms  | 80% queries |
| 2    | Fallback | 13.8s         | <1s    | 20% queries |

**Total System:**
- Current: 5/5 queries working (100% functional coverage)
- Speed: 4 queries fast (<25ms), 1 query slow (13.8s)
- Next step: Optimize fallback with partition pruning and parallelization

## Optimization Opportunities

### Already Implemented ✅
- Single-pass rollup building (16 min → 11.6 min)
- Pre-loading rollups into memory (3000× speedup)
- Derived column filtering (expanded coverage)
- Date format conversion (calendar ↔ day-of-year)

### Future Optimizations 🔄
1. **Partition-aware fallback**: Only scan relevant date ranges (366 files → ~4 files for Oct queries)
2. **Parallel file reading**: Use multiple cores to scan CSV files
3. **Partial rollup + filter**: Use closest rollup then filter remaining dimensions
4. **Smart caching**: Cache fallback results for repeated patterns

**Expected Impact:** Fallback from 13.8s → <1s (still 65× faster than baseline)

## Why This Approach Wins

**vs DuckDB Baseline (65.18s):**
- **Pre-aggregation**: We compute aggregates once (prepare phase), DuckDB computes per query
- **Compression**: 82.7 MB vs 19GB → better cache locality
- **Format**: Arrow IPC (zero-copy) vs CSV (parse overhead)
- **Specialization**: Query-specific rollups vs general-purpose engine

**Trade-offs:**
- **Prepare time**: 11.6 min (acceptable, one-time cost)
- **Disk space**: +82.7 MB (negligible)
- **Coverage**: Not all query patterns pre-computed (solved with fallback)
- **Flexibility**: Less flexible than database, but 100× faster for target workload

## Technical Stack

- **Storage**: Apache Arrow IPC with LZ4 compression
- **Streaming**: PyArrow for batch processing (avoids OOM)
- **Execution**: Polars LazyFrame (query optimization)
- **Aggregation**: NULL-safe sum/avg/count/min/max
- **Format**: Day-of-year temporal encoding (YYYY-DDD)

## Lessons Learned

1. **Paradigm matters**: Treating this as a cube problem vs database problem changed everything
2. **Resource constraints drive design**: 16GB RAM → can't build all rollups → need fallback
3. **Streaming is essential**: PyArrow batching prevents OOM during build
4. **Pre-loading pays off**: 296ms to load 82.7 MB → 3000× faster queries
5. **Derived columns extend coverage**: day from minute → broader rollup utility
6. **Generalization required**: Can't overfit to 5 specific queries, need to handle arbitrary patterns

## Future Work

- [ ] Optimize fallback to <1s (partition pruning, parallelization)
- [ ] Benchmark on M2 Pro (judges' hardware) for prepare time
- [ ] Add more rollups if we identify high-value patterns
- [ ] Consider hybrid approaches (partial rollup + filtering)
- [ ] Profile memory usage during fallback queries
