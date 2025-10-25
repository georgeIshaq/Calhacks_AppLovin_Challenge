# Implementation Checklist - Hybrid OLAP Approach

**Strategy**: Rollups for fast path (~95% queries) + Optimized fact table for fallback (~5% queries)

---

## Phase 1: Rollup Builder (Core Infrastructure)

**Goal**: Build 12-15 pre-aggregated rollups during prepare phase

### 1.1 Data Loader (src/core/data_loader.py)
- [ ] Create Polars-based CSV streaming reader
- [ ] Handle 49 CSV files with lazy loading
- [ ] Extract time dimensions (day, hour, minute, week) from timestamp
- [ ] Memory-efficient: stream not load all at once
- [ ] Test: Can process all 225M rows without OOM

**Expected time**: 30 min to code, 2 min to run

---

### 1.2 Rollup Aggregator (src/core/rollup_builder.py)
- [ ] Implement streaming group-by aggregation
- [ ] NULL-safe aggregates:
  - `bid_price_sum = SUM(bid_price WHERE NOT NULL)`
  - `bid_price_count = COUNT(bid_price WHERE NOT NULL)`
  - `total_price_sum = SUM(total_price WHERE NOT NULL)`
  - `total_price_count = COUNT(total_price WHERE NOT NULL)`
  - `row_count = COUNT(*)`
- [ ] Build 7 core rollups:
  - `day_type` (1,464 rows)
  - `hour_type` (35,136 rows)
  - `minute_type` (2.1M rows) - **PARTITION BY DAY!**
  - `week_type` (208 rows)
  - `country_type` (48 rows)
  - `advertiser_type` (6,616 rows)
  - `publisher_type` (4,456 rows)
- [ ] Build 5 combo rollups:
  - `day_country_type` (17,568 rows)
  - `day_advertiser_type` (2.4M rows)
  - `day_publisher_type` (1.6M rows)
  - `week_country_type` (2,496 rows)
  - `hour_country_type` (421K rows)

**CRITICAL**: minute_type must be partitioned into 366 files (one per day)!

**Expected time**: 1 hour to code, 2-3 min to run

---

### 1.3 Storage Writer (src/core/storage.py)
- [ ] Write rollups as Arrow IPC files with LZ4 compression
- [ ] Directory structure:
  ```
  rollups/
    day_type.arrow
    hour_type.arrow
    minute_type/
      day_001.arrow
      day_002.arrow
      ...
      day_366.arrow
    week_type.arrow
    country_type.arrow
    ...
  ```
- [ ] Verify file sizes (<100KB for small rollups, ~50KB per minute partition)
- [ ] Test: Load rollups in <5ms each

**Expected time**: 20 min to code

---

## Phase 2: Query Router (Smart Dispatch)

**Goal**: Route queries to best rollup or fallback to fact table

### 2.1 Query Parser (src/query/parser.py)
- [ ] Parse SQL to extract:
  - SELECT columns
  - Aggregate functions (SUM, AVG, COUNT, MIN, MAX)
  - GROUP BY dimensions
  - WHERE filters (column, operator, value)
  - ORDER BY
- [ ] Handle edge cases:
  - Multiple WHERE conditions (AND/OR)
  - IN clauses
  - BETWEEN clauses
  - CASE statements
- [ ] Test with Q1-Q5 to ensure correct parsing

**Expected time**: 45 min to code

---

### 2.2 Rollup Selector (src/query/router.py)
- [ ] Implement routing logic:
  1. Extract needed dimensions from GROUP BY + WHERE
  2. Match to available rollups
  3. Select smallest matching rollup
  4. If no match, return None (use fact table)
- [ ] Handle special cases:
  - Partitioned rollups (extract partition key from WHERE)
  - Time dimension equivalence (day ≈ date)
  - Numeric range filters → fact table (bid_price > 5.0)
  - LIKE clauses → fact table
- [ ] Test: 100% correct routing for Q1-Q5
- [ ] Test: <0.01ms routing overhead

**Expected time**: 30 min to code

---

### 2.3 Rollup Query Executor (src/query/rollup_executor.py)
- [ ] Load Arrow IPC file for selected rollup
- [ ] Apply WHERE filters using Polars
- [ ] Apply GROUP BY aggregations:
  - `SUM(col) = WHEN(count > 0, sum, NULL)` - NULL-safe!
  - `AVG(col) = sum / count`
  - `COUNT(col) = count.fill_null(0)` - Fix for Test 2!
  - `MIN(col) = min_val`
  - `MAX(col) = max_val`
- [ ] Apply ORDER BY
- [ ] Return results
- [ ] Test: Q1-Q5 return correct results in <15ms each

**Expected time**: 45 min to code

---

## Phase 3: Fallback System (Optimized Fact Table)

**Goal**: Handle queries that don't match rollups (safety net)

### 3.1 Fact Table Optimizer (src/query/fact_executor.py)
- [ ] Convert 49 CSVs to Parquet format (columnar, compressed)
  - Parquet is 5-10× faster to scan than CSV
  - Can pushdown predicates
- [ ] Implement Polars lazy query execution:
  - `scan_parquet()` not `read_parquet()` (streaming!)
  - Predicate pushdown (filter before loading)
  - Projection pushdown (select only needed columns)
- [ ] Test: Can execute Q1-Q5 in <5s each (fallback mode)

**Expected time**: 30 min to code, 1 min to convert CSVs to Parquet

---

### 3.2 Unified Query Interface (src/query/executor.py)
- [ ] Main entry point: `execute_query(sql) -> DataFrame`
- [ ] Flow:
  ```python
  def execute_query(sql):
      pattern = parse_query(sql)
      rollup = select_best_rollup(pattern)
      
      if rollup:
          # Fast path: <15ms
          return query_rollup(rollup, pattern)
      else:
          # Fallback: <5s
          return query_fact_table(pattern)
  ```
- [ ] Add logging to track rollup hit rate
- [ ] Test: Q1-Q5 all use fast path (<15ms)

**Expected time**: 20 min to code

---

## Phase 4: Integration & Testing

### 4.1 End-to-End Integration
- [ ] Wire up prepare.py:
  ```python
  def prepare():
      load_data_streaming()
      build_all_rollups()
      write_rollups_to_disk()
      convert_fact_table_to_parquet()  # fallback
  ```
- [ ] Wire up run.py:
  ```python
  def run(query):
      return execute_query(query)
  ```
- [ ] Test: Full pipeline works

**Expected time**: 30 min

---

### 4.2 Correctness Testing
- [ ] Run Q1-Q5 through our system
- [ ] Run Q1-Q5 through DuckDB baseline
- [ ] Compare results (must match 100%)
- [ ] Fix any discrepancies

**Expected time**: 30 min

---

### 4.3 Performance Benchmarking
- [ ] Time prepare phase (target: <3 min)
- [ ] Time each query individually (target: <15ms avg)
- [ ] Time all 5 queries together (target: <100ms total)
- [ ] Verify rollup hit rate (target: 100% for Q1-Q5)

**Expected time**: 15 min

---

### 4.4 Edge Case Testing
- [ ] Test with missing data (sparse dates)
- [ ] Test with NULL values everywhere
- [ ] Test with extreme values (very large numbers)
- [ ] Test with empty results (WHERE filters nothing)

**Expected time**: 30 min

---

## Phase 5: Optimization & Polish

### 5.1 Performance Tuning (if needed)
- [ ] Profile query execution (find bottlenecks)
- [ ] Optimize slow rollups (if any >20ms)
- [ ] Consider parallel rollup loading (if needed)

**Expected time**: 30 min

---

### 5.2 Error Handling
- [ ] Graceful handling of malformed SQL
- [ ] Informative error messages
- [ ] Fallback to fact table on rollup errors

**Expected time**: 20 min

---

### 5.3 Documentation
- [ ] README with usage instructions
- [ ] Architecture diagram
- [ ] Performance report
- [ ] Trade-offs discussion (for judges)

**Expected time**: 30 min

---

## Summary Timeline

| Phase | Description | Coding Time | Run Time |
|-------|-------------|-------------|----------|
| **Phase 1** | Rollup Builder | 2 hours | 3 min |
| **Phase 2** | Query Router | 2 hours | <0.1ms |
| **Phase 3** | Fallback System | 1 hour | <5s |
| **Phase 4** | Integration & Testing | 2 hours | - |
| **Phase 5** | Optimization & Polish | 1.5 hours | - |
| **TOTAL** | | **8-9 hours** | **<100ms** |

---

## Validation Checklist (Must Pass)

### Correctness
- [ ] Q1 results match DuckDB 100%
- [ ] Q2 results match DuckDB 100%
- [ ] Q3 results match DuckDB 100%
- [ ] Q4 results match DuckDB 100%
- [ ] Q5 results match DuckDB 100%
- [ ] NULL handling matches DuckDB (including COUNT edge case)

### Performance
- [ ] Prepare phase: <10 minutes (target: 2-3 min)
- [ ] Q1 query time: <50ms (target: <10ms)
- [ ] Q2 query time: <50ms (target: <15ms)
- [ ] Q3 query time: <50ms (target: <5ms)
- [ ] Q4 query time: <50ms (target: <15ms)
- [ ] Q5 query time: <50ms (target: <20ms)
- [ ] Total Q1-Q5: <1s (target: <100ms)

### Robustness
- [ ] Handles 100% of Q1-Q5 via rollups (no fallback needed)
- [ ] Fallback system works for edge cases (<5s)
- [ ] No memory issues (stays under 16GB)
- [ ] No disk issues (stays under 100GB)

---

## Risk Mitigation

### High Risk Items
1. **Minute rollup partitioning** - Test 4 validated this works (0.7ms)
2. **NULL handling** - Test 2 showed need for `.fill_null(0)` on COUNT
3. **Query parser robustness** - Keep simple, test thoroughly
4. **Fallback correctness** - Must match DuckDB exactly

### Contingency Plans
- If rollup approach fails → pure Polars/Parquet streaming (still 10× faster than DuckDB)
- If memory issues → reduce rollup count (keep only 7 core rollups)
- If correctness issues → add extensive logging and fix systematically

---

## Success Criteria

**Minimum Viable (Must Have)**
- ✅ Q1-Q5 return correct results (100% match with DuckDB)
- ✅ Q1-Q5 total time <1s
- ✅ Prepare time <10 minutes

**Target (Should Have)**
- ✅ Q1-Q5 average time <20ms each
- ✅ Rollup hit rate 100% for Q1-Q5
- ✅ Prepare time <3 minutes

**Stretch (Nice to Have)**
- ✅ Q1-Q5 average time <15ms each
- ✅ 95% coverage for unseen queries
- ✅ Fallback <5s for edge cases

---

## Next Steps

**Ready to start?** Begin with **Phase 1.1** (Data Loader).

Would you like me to start implementing, or do you have questions about any phase?
