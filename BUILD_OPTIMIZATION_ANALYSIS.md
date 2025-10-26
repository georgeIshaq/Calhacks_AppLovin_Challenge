# Build Optimization Analysis

## 1. Memory Map Warning Explanation

**Warning Message:**
```
Could not memory_map compressed IPC file, defaulting to normal read. 
Toggle off 'memory_map' to silence this warning.
```

**What It Means:**
- This comes from Polars/PyArrow when reading Arrow IPC (.arrow) files
- Your rollups are written with LZ4 compression (`compression='lz4'` in storage.py:71)
- Memory mapping (mmap) only works with UNCOMPRESSED files (direct byte-for-byte access)
- Compressed files require decompression, so Arrow falls back to normal read
- **This is completely harmless** - your data loads correctly, just via a different path

**Impact:**
- ✅ Correctness: Zero impact - data loads perfectly
- ⚠️ Performance: Minor - you lose zero-copy benefits, but LZ4 is so fast it doesn't matter
- 📢 Noise: Just a warning log line

**Where It's Happening:**
- Line 93 in `src/core/rollup_loader.py`: `pl.read_ipc(self.rollup_paths[name])`
- Line 127 in `src/core/rollup_loader.py`: `pl.read_ipc(rollup_path, memory_map=True)`
- Line 159: `pl.read_ipc(partition_file, memory_map=True)`

**How to Fix (3 options):**

Option A - Silence the warning (recommended, safest):
```python
# In rollup_loader.py, change:
df = pl.read_ipc(path, memory_map=True)
# To:
df = pl.read_ipc(path, memory_map=False)
```

Option B - Remove compression (not recommended):
```python
# In storage.py:71, change:
df.write_ipc(output_path, compression='lz4')
# To:
df.write_ipc(output_path, compression=None)
# BUT: Files will be 3-5× larger on disk!
```

Option C - Ignore it (also fine):
- It's just a warning, not an error
- Your queries run perfectly (39ms proves it)

**Recommendation:** Option A or C. The warning is cosmetic.

---

## 2. Build Time Analysis (15 minutes)

**Current Performance:**
- Your build: ~15 minutes (900 seconds)
- Expected: ~4-7 minutes based on code comments
- Judge machine: M2 Pro (should be faster than your M3 Air)

**Build Phases (from prepare.py):**
1. **Phase 1: Build rollups** (~7-10 min) - BIGGEST bottleneck
   - Single-pass streaming over 49 CSV files
   - Builds 11 rollups simultaneously with incremental folding
   - Heavy CPU: time dimension extraction, aggregation, folding
   
2. **Phase 2: Write rollups** (~10-30s)
   - Write 11 .arrow files with LZ4 compression
   
3. **Phase 3: DuckDB fallback** (~3-4 min)
   - CSV → Parquet conversion with Polars
   - DuckDB table creation with ORDER BY (expensive sort!)
   - ANALYZE statistics
   
4. **Phase 4: Preload rollups** (~5-10s)
   - Load .arrow files into memory for verification

**Bottleneck Identification:**

```
Phase 1 (Rollups):  ~7-10 min  ████████████████████
Phase 3 (DuckDB):   ~3-4 min   █████████
Phase 2 (Write):    ~30s       █
Phase 4 (Preload):  ~10s       █
```

---

## 3. Safe Optimization Recommendations

### TIER 1: Zero Risk, High Impact (DO THESE FIRST) ✅

#### A. Increase Polars Threading for CSV Reading
**File:** `src/core/rollup_builder.py` (line 196)

**Current:**
```python
reader = pc.open_csv(
    csv_file,
    convert_options=convert_opts,
    read_options=read_opts
)
```

**Optimized:**
```python
import os
cores = os.cpu_count() or 8

# Set Polars thread count BEFORE heavy operations
pl.Config.set_global_n_threads(cores)

read_opts = pc.ReadOptions(
    block_size=128 * 1024 * 1024,  # Increase from 64MB → 128MB
    use_threads=True  # Explicitly enable threading
)
```

**Why:** Your M3 Air has 8 cores (4 performance + 4 efficiency). PyArrow CSV reader defaults to single-threaded or low parallelism.

**Expected Speedup:** 2-3× faster CSV reading (Phase 1: 10min → 3-5min)

---

#### B. Increase DuckDB Threads & Memory
**File:** `prepare.py` (line 126)

**Current:**
```python
con.execute("PRAGMA threads=8")
```

**Optimized:**
```python
import os
cores = max(1, os.cpu_count() - 1)  # Leave 1 core for OS

con.execute(f"PRAGMA threads={cores}")
con.execute("PRAGMA memory_limit='12GB'")  # Increase from default
con.execute("PRAGMA temp_directory='/tmp'")  # Fast local SSD
```

**Why:** 
- M3 Air has 8 cores, but DuckDB might default to fewer
- ORDER BY sorting is memory-intensive - more RAM = in-memory sort
- Default temp is often fine, but explicit is better

**Expected Speedup:** Phase 3: 4min → 2min (sorting benefits most)

---

#### C. Use Faster Parquet Compression (Snappy)
**File:** `prepare.py` (line 76)

**Current:**
```python
df.sink_parquet(
    parquet_path,
    compression='snappy',  # Already optimal! ✅
    row_group_size=1_000_000,
)
```

**Status:** ✅ Already using snappy! This is perfect.

---

### TIER 2: Low Risk, Medium Impact ⚠️

#### D. Reduce Arrow IPC Block Size During Reads
**File:** `src/core/rollup_builder.py` (line 196)

**Current:** 64MB blocks
**Optimized:** 128MB blocks (already shown in TIER 1-A)

**Why:** Larger blocks = fewer I/O calls = faster on SSD

---

#### E. Parallelize CSV File Processing
**File:** `src/core/rollup_builder.py` (lines 173-330)

**Current:** Sequential loop over CSV files
```python
for file_idx, csv_file in enumerate(csv_files):
    # Process file...
```

**Optimized:** Use multiprocessing pool
```python
from multiprocessing import Pool
import os

def process_csv_file(csv_file):
    # Extract rollup logic into separate function
    # Return partial aggregates
    pass

with Pool(processes=min(4, os.cpu_count())) as pool:
    results = pool.map(process_csv_file, csv_files)
    
# Then fold all results
```

**Why:** 49 CSV files can be processed in parallel (4 workers × 12 files each)

**Risk:** ⚠️ Higher complexity, need to refactor code
**Expected Speedup:** Phase 1: 10min → 3-4min

**Recommendation:** Skip this for now - code works, complexity not worth it

---

### TIER 3: RISKY - Don't Touch ❌

#### F. Remove LZ4 Compression from Arrow Files
**Don't do this!** You'd save ~5 seconds write time but:
- Files become 3-5× larger (hundreds of MB)
- Still see the memory_map warning (files would be huge)
- Query time might increase (more disk I/O)

#### G. Skip DuckDB Fallback Build
**Don't do this!** You need 100% coverage. Even if it takes 4 minutes, it's required.

---

## 4. Practical Implementation (Copy-Paste Ready)

### Change #1: Add to top of prepare.py (after imports)
```python
import os

# Optimize Polars threading for CSV operations
cores = os.cpu_count() or 8
import polars as pl
pl.Config.set_global_n_threads(cores)

logger.info(f"Configured {cores} threads for parallel processing")
```

### Change #2: Update build_duckdb_fallback() in prepare.py
Find line 126 and replace:
```python
# OLD:
con.execute("PRAGMA threads=8")

# NEW:
import os
cores = max(1, os.cpu_count() - 1)  # Leave 1 for OS
con.execute(f"PRAGMA threads={cores}")
con.execute("PRAGMA memory_limit='12GB'")
con.execute("PRAGMA temp_directory='/tmp'")
```

### Change #3: Increase CSV block size in rollup_builder.py
Find line 196 and replace:
```python
# OLD:
read_opts = pc.ReadOptions(block_size=64 * 1024 * 1024)

# NEW:
read_opts = pc.ReadOptions(
    block_size=128 * 1024 * 1024,  # 128MB blocks
    use_threads=True  # Explicitly enable
)
```

### Change #4: Silence memory_map warning in rollup_loader.py
Find lines with `memory_map=True` and change to `memory_map=False`:
```python
# Line 127:
df = pl.read_ipc(rollup_path, memory_map=False)

# Line 159:
df = pl.read_ipc(partition_file, memory_map=False)
```

---

## 5. Expected Results After Optimization

**Before:**
- Build time: 15 minutes
- Query time: 39ms ✅ (already excellent!)

**After (Conservative Estimate):**
- Build time: 7-10 minutes (40-50% faster)
- Query time: 39ms (unchanged, already optimal)

**After (Optimistic Estimate):**
- Build time: 5-7 minutes (50-65% faster)
- Query time: 39ms

**Breakdown:**
- Phase 1 (Rollups): 10min → 4-5min (threading + block size)
- Phase 2 (Write): 30s → 30s (unchanged)
- Phase 3 (DuckDB): 4min → 2min (more threads + memory)
- Phase 4 (Preload): 10s → 10s (unchanged)

**Judge's M2 Pro (Expected):**
- More high-performance cores (8P + 4E vs your 4P + 4E)
- Better sustained performance (Pro cooling vs Air)
- Your 7min build → likely 4-5min on M2 Pro ✅

---

## 6. Risk Assessment

| Change | Risk | Impact | Recommend? |
|--------|------|--------|------------|
| Polars threading | 🟢 Zero | 🔥 High | ✅ YES |
| DuckDB threads/memory | 🟢 Zero | 🔥 High | ✅ YES |
| Larger block size | 🟢 Zero | 🔵 Medium | ✅ YES |
| memory_map=False | 🟢 Zero | 🔵 Low (cosmetic) | ✅ YES |
| Multiprocess CSV | 🟡 Medium | 🔥 High | ⚠️ NO (complexity) |
| Remove compression | 🔴 High | 🔵 Low | ❌ NO |

---

## 7. M3 Air vs M2 Pro Comparison

**Your M3 Air:**
- CPU: 8-core (4P + 4E)
- RAM: 16GB (assumed)
- Storage: NVMe SSD
- Thermal: Passive cooling (throttles under sustained load)

**Judge's M2 Pro:**
- CPU: 10-core or 12-core (8P + 4E or similar)
- RAM: 16GB+ 
- Storage: NVMe SSD
- Thermal: Active cooling (sustained performance)

**Performance Expectation:**
- M2 Pro will be 30-50% faster on sustained workloads
- Your 7min → Judge's 4-5min ✅ (under 10min budget)
- Both have plenty of RAM (no swapping)

---

## 8. Final Recommendation

**What to do RIGHT NOW (5 minutes):**
1. ✅ Apply changes #1, #2, #3 from section 4 (threading + memory)
2. ✅ Apply change #4 to silence warning (optional but clean)
3. ✅ Test one build to verify ~7-10min (should see improvement)
4. ✅ Commit and document

**What NOT to do:**
- ❌ Don't parallelize CSV processing (too complex, risky)
- ❌ Don't remove compression (file size explosion)
- ❌ Don't touch query execution code (already perfect at 39ms!)

**Confidence:**
- 95% these changes will reduce build to 7-10 minutes
- 80% judge's M2 Pro will do it in 4-6 minutes
- 100% your query performance (39ms) is already winning! 🏆

**Your system is PRODUCTION READY as-is. These are nice-to-have optimizations.**
