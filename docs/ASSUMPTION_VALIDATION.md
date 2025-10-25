# Assumption Validation: OLAP Cube Strategy

## ⚠️ CRITICAL QUESTION
**Before we build a multi-grain OLAP cube, do the rules allow it?**

---

## ASSUMPTIONS WE'RE MAKING

### Assumption #1: Pre-Aggregation is Allowed ✅
**What we assume**: We can pre-compute aggregates in Prepare phase

**Evidence from spec**:
```
Phase 1: Prepare (Data Loading & Optimization)
1. Load raw CSV data
2. Design optimal storage layout (partitioning, compression, etc.)
3. Build indexes and pre-compute aggregations  ← EXPLICITLY ALLOWED!
4. Store optimized data structures
```

**Verdict**: ✅ **CONFIRMED** - Spec explicitly says "pre-compute aggregations"

---

### Assumption #2: We Can Store Multiple Rollups ⚠️
**What we assume**: We can store 7+ separate pre-aggregated tables

**Evidence from spec**:
- Max 100 GB disk usage ✅
- Our rollups: ~128MB total ✅ (well under limit)

**Potential issue**:
- Spec doesn't explicitly say "you can store multiple materialized views"
- But it says "store optimized data structures" (plural)

**Verdict**: ✅ **LIKELY SAFE** - "data structures" (plural) implies multiple tables allowed

---

### Assumption #3: No SQL Parser Required ⚠️
**What we assume**: JSON queries have predictable patterns we can pattern-match

**Evidence from spec**:
```json
Supported operations:
- SELECT: columns and aggregates (SUM, COUNT, AVG)
- WHERE: eq, neq, in, between (AND-combined)
- GROUP BY: multiple columns
- ORDER BY: asc/desc sorting
- Time dimensions: day, week, hour, minute
```

**Known queries (from baseline/inputs.py)**:
1. Q1: `GROUP BY day` + `WHERE type='impression'`
2. Q2: `GROUP BY publisher_id` + `WHERE type='impression' AND country='JP' AND day BETWEEN`
3. Q3: `GROUP BY country` + `WHERE type='purchase'` + `ORDER BY AVG DESC`
4. Q4: `GROUP BY advertiser_id, type` (no WHERE)
5. Q5: `GROUP BY minute` + `WHERE type='impression' AND day='2024-06-01'`

**Potential issue**:
- Holdout queries might have patterns we can't match
- Example: `GROUP BY week, country` (we don't have this rollup!)
- Example: `GROUP BY hour, advertiser_id` (we don't have this!)

**Verdict**: ⚠️ **RISK** - We need fallback for unmatchable queries

---

### Assumption #4: Holdout Queries Use Same Patterns 🚨
**What we assume**: Secret queries follow similar patterns to Q1-Q5

**Evidence**:
- ❌ We have ZERO information about holdout queries
- They could test:
  - `GROUP BY week` ← we planned for this ✅
  - `GROUP BY hour` ← we planned for this ✅
  - `GROUP BY week, country` ← we DON'T have this! ❌
  - `GROUP BY hour, advertiser_id` ← we DON'T have this! ❌
  - `GROUP BY day, publisher_id, type` ← we DON'T have this! ❌

**Critical blind spot**:
```
Our rollups:
✅ day × type
✅ minute × type  
✅ hour × type
✅ week × type
✅ country × type
✅ advertiser × type
✅ publisher × type

Missing combinations:
❌ day × country × type
❌ day × advertiser × type
❌ day × publisher × type
❌ week × country × type
❌ hour × country × type
... (many more)
```

**Verdict**: 🚨 **MAJOR RISK** - Combinatorial explosion if we try to cover all patterns

---

### Assumption #5: "Beating DuckDB" Means Faster Runtime ✅
**What we assume**: Performance is measured on Run phase only

**Evidence from spec**:
```
Evaluation Criteria:
- Performance & Accuracy (40%): Speed + correctness
```

**Baseline timings**:
- Prepare: ~20s (loading CSVs)
- Run: 65.18s (5 queries)

**Our strategy**:
- Prepare: ~30s (slower by 10s)
- Run: ~50-80ms (816× faster!)

**Potential issue**:
- Does "Speed" include Prepare time?
- If yes: (30s prepare + 0.08s run) = 30.08s vs 85s baseline → 2.8× speedup
- If no: 0.08s vs 65s → 816× speedup

**Verdict**: ✅ **LIKELY SAFE** - Spec says "Run phase: No network access" (implies Run is separate evaluation)

---

### Assumption #6: Memory-Mapped Files Are Allowed ✅
**What we assume**: We can mmap Arrow files for instant access

**Evidence from spec**:
- No restriction on mmap
- "Max 16 GB RAM" constraint applies to actual memory usage
- mmap doesn't count as RAM (OS manages paging)

**Verdict**: ✅ **CONFIRMED** - No restriction, common technique

---

### Assumption #7: Polars/Arrow Libraries Are Allowed ✅
**What we assume**: We can use Polars for faster CSV parsing

**Evidence from spec**:
- baseline uses DuckDB (not restricted to pandas)
- Spec says "demonstrate sound architectural choices"
- No library restrictions mentioned

**Verdict**: ✅ **CONFIRMED** - Any Python library is fine

---

## 🚨 THE BIG RISK: Combinatorial Explosion

### The Problem
If holdout queries use **composite GROUP BY** clauses:
```python
# Example holdout query that BREAKS our strategy:
{
  "select": ["day", "country", "type", {"SUM": "bid_price"}],
  "group_by": ["day", "country", "type"],
  # We don't have this rollup!
}
```

### Our Current Rollups Coverage
```
Single-dimension:
✅ day, type
✅ minute, type
✅ hour, type
✅ week, type
✅ country, type
✅ advertiser_id, type
✅ publisher_id, type

Two-dimension (missing!):
❌ day, country, type
❌ day, advertiser_id, type
❌ week, country, type
... (21+ combinations!)

Three-dimension (missing!):
❌ day, country, advertiser_id, type
... (35+ combinations!)
```

### Cardinality Explosion
```
day × country × type:
  366 × 12 × 4 = 17,568 rows (~5MB)
  
day × advertiser × type:
  366 × 1,654 × 4 = 2.4M rows (~100MB)
  
day × publisher × type:
  366 × 1,114 × 4 = 1.6M rows (~70MB)
  
Total for all 2-way combos: ~500MB
Total for all 3-way combos: ~5GB
```

**This is manageable!** 5GB < 100GB disk limit

---

## 💡 SOLUTION: Hybrid Strategy

### Option A: Build ALL 2-Way Combos (Conservative)
```
Build these additional rollups:
- day × country × type
- day × advertiser × type
- day × publisher × type
- week × country × type
- hour × country × type

Total size: ~500MB
Build time: +15s
Coverage: ~95% of possible queries
```

### Option B: Query Rewriter + Fallback (Aggressive)
```
If query doesn't match any rollup:
1. Check if we can REWRITE query to use existing rollup
   Example: GROUP BY day, country → post-filter country_by_type rollup
   
2. If not, FALLBACK to columnar scan
   Time: 500-1000ms (still better than 13s DuckDB per query!)
```

### Option C: Hybrid (RECOMMENDED)
```
Build most common 2-way combos:
- day × country × type (likely!)
- day × type (already have)
- week × country × type (likely!)

Total: ~250MB additional

For remaining queries:
- Intelligent query rewriting
- Fallback to columnar scan (500-1000ms)

Expected coverage:
- 80% queries: <50ms (rollup hit)
- 15% queries: 100-200ms (rewritten)
- 5% queries: 500-1000ms (fallback)

Average: ~100-150ms per query
```

---

## ✅ VALIDATED ASSUMPTIONS

1. ✅ Pre-aggregation explicitly allowed
2. ✅ Multiple data structures allowed (plural in spec)
3. ✅ Memory-mapped files allowed
4. ✅ Any Python libraries allowed
5. ✅ Run phase evaluated separately from Prepare

---

## ⚠️ RISKY ASSUMPTIONS

1. ⚠️ Holdout queries follow similar patterns
2. ⚠️ Single-dimension rollups will cover most queries
3. ⚠️ Query rewriting can handle gaps

---

## 🚨 CRITICAL GAPS

1. 🚨 **No 2-way combo rollups** (day × country, week × country, etc.)
   - **Risk**: Holdout query with composite GROUP BY → forced to scan
   - **Impact**: 50ms → 1000ms (20× slower than planned)
   - **Mitigation**: Build top 5-10 2-way combos (+250MB, +15s build)

2. 🚨 **No query rewriter** (yet)
   - **Risk**: Rollup near-miss → unnecessary scan
   - **Example**: GROUP BY week but we have day → could aggregate daily rollup!
   - **Impact**: 50ms → 1000ms
   - **Mitigation**: Build query rewriter (500 lines of code)

3. 🚨 **No fallback tested**
   - **Risk**: Fallback columnar scan might be >1s
   - **Impact**: Single slow query → total time >1s
   - **Mitigation**: Test columnar scan performance NOW

---

## 🎯 REVISED ACTION PLAN

### Phase 1: Validate Fallback Performance (30 min)
```bash
# Test: How fast is columnar scan for Q1 using Polars?
python src/test_columnar_scan.py
# Target: <500ms for 73M row scan
```

### Phase 2: Build Core Rollups (1 hour)
```python
# Build single-dimension rollups (as planned)
# Total: 7 rollups, ~128MB, +8s build time
```

### Phase 3: Add 2-Way Combos (1 hour)
```python
# Add critical 2-way combos:
# - day × country × type
# - day × advertiser × type  
# - week × country × type
# Total: +3 rollups, +250MB, +15s build time
```

### Phase 4: Build Query Router with Fallback (2 hours)
```python
# Pattern match → rollup
# If no match → fallback to columnar scan
# Expected: 80% rollup hit, 20% fallback
```

### Phase 5: Test on Full Dataset (30 min)
```bash
# Validate:
# - Q1-Q5: <50ms each ✅
# - Random composite queries: <500ms ✅
# - Total with worst-case holdout: <1s ✅
```

---

## 📊 UPDATED RISK ASSESSMENT

**Original plan (single-dimension only)**:
- Best case: 50ms ✅
- Worst case: 1500ms ❌ (forced to scan)
- Risk level: 🔴🔴⚪⚪⚪ HIGH

**Revised plan (+ 2-way combos + fallback)**:
- Best case: 50ms ✅
- Average case: 150ms ✅
- Worst case: 500ms ✅
- Risk level: 🟢🟢🟢🟡⚪ LOW-MEDIUM

---

## 🏁 FINAL RECOMMENDATION

### DO NOT implement pure single-dimension cube!

**Instead**:
1. ✅ Build single-dimension rollups (7 tables, ~128MB)
2. ✅ Build critical 2-way combos (3 tables, ~250MB)
3. ✅ Build query rewriter (handle near-misses)
4. ✅ Build columnar scan fallback (<500ms)
5. ✅ Test both known and synthetic composite queries

**Total resources**:
- Disk: ~400MB (< 100GB limit ✅)
- Memory: ~400MB loaded (< 16GB limit ✅)
- Build time: ~30s (acceptable ✅)
- Query time: 50-500ms depending on pattern (< 1s ✅)

**Confidence level**: 🟢🟢🟢🟢⚪ (4/5 - High)

This covers:
- ✅ All known query patterns (Q1-Q5)
- ✅ Most likely holdout patterns (2-way combos)
- ✅ Fallback for edge cases (<500ms still fast!)
- ✅ Within all resource constraints

**The key insight**: Build both aggressive pre-aggs AND a fast fallback. Best of both worlds.
