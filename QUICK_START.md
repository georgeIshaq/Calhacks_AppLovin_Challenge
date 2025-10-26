# Quick Reference Card - Judge Testing

## 🚀 Quick Start (3 Commands)

```bash
# 1. Install
pip install -r requirements.txt

# 2. Build (one-time, ~11-15 min)
python3 prepare.py --data-dir ./data

# 3. Run queries (<100ms)
python3 run.py
```

---

## 📝 Testing with Your 20 Queries

### Method 1: JSON File (Recommended)

**1. Create your query file: `judges_queries.json`**
```json
[
    {
        "select": ["day", {"SUM": "bid_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "impression"}],
        "group_by": ["day"]
    },
    // ... your other 19 queries
]
```

**2. Run queries:**
```bash
python3 run.py --query-file ./judges_queries.json
```

**3. Check results:**
```bash
ls results/  # q1.csv, q2.csv, ..., q20.csv
```

### Method 2: Python File

**1. Create `queries/judges.py`:**
```python
queries = [
    {"select": ["day", {"SUM": "bid_price"}], ...},
    # ... your 20 queries
]
```

**2. Run:**
```bash
python3 run.py --query-dir ./queries
```

---

## 📊 Query Format Quick Reference

```python
{
    "select": ["col1", "col2", {"AGG": "col3"}],
    "from": "events",
    "where": [
        {"col": "column", "op": "eq", "val": "value"},
        {"col": "date", "op": "between", "val": ["2024-06-01", "2024-06-30"]},
        {"col": "type", "op": "in", "val": ["impression", "click"]}
    ],
    "group_by": ["col1", "col2"],
    "order_by": [{"col": "col1", "dir": "asc"}]
}
```

**Operators:** `eq`, `gt`, `lt`, `gte`, `lte`, `between`, `in`

**Aggregates:** `SUM`, `AVG`, `COUNT`, `MIN`, `MAX`

**Columns:** `ts`, `type`, `country`, `advertiser_id`, `publisher_id`, `bid_price`, `total_price`, `user_id`, `day`, `hour`, `minute`, `week`

---

## ✅ Validation

```bash
# Verify setup is correct
python3 validate_setup.py
```

Expected output:
```
✅ Found 11 rollup files
✅ All 11 expected rollups found
✅ DuckDB fallback found
✅ Query files found
✅ Rollup loaded successfully
✅ All checks passed!
```

---

## 📁 Output Structure

```
results/
  q1.csv    # Query 1 results
  q2.csv    # Query 2 results
  ...
  q20.csv   # Query 20 results
```

Each CSV has:
- Header row with column names
- Data rows with query results

---

## 🎯 Expected Performance

| Metric | Value |
|--------|-------|
| Query time (5 queries) | ~39ms |
| Query time (20 queries) | <200ms |
| Average per query | <10ms |
| Build time (one-time) | 11-15 min |

---

## 🔧 All Command Options

### prepare.py
```bash
python3 prepare.py \
  --data-dir <csv-directory> \
  --output-dir <rollup-output> \
  --fallback-dir <duckdb-output>
```

### run.py
```bash
python3 run.py \
  --query-file <queries.json> \
  --query-dir <query-directory> \
  --output-dir <results-directory> \
  --rollup-dir <rollup-directory> \
  --fallback-path <fallback.duckdb>
```

---

## 🐛 Troubleshooting

| Problem | Solution |
|---------|----------|
| "Rollup directory not found" | Run `python3 prepare.py` first |
| Build takes >20 min | Expected on M3 Air, close other apps |
| Query fails | Check query format, see examples in `queries/` |
| Results differ from baseline | Validate with `diff` command |

---

## 📚 Full Documentation

- **JUDGE_SETUP.md** - Complete setup guide with examples
- **queries/README.md** - Query format documentation
- **README.md** - System overview and architecture
- **docs/ARCHITECTURE.md** - Technical details

---

## 🏆 Performance Comparison

```bash
# Baseline
cd baseline
python3 main.py --data-dir ../data --out-dir ./baseline_results
# Expected: ~500ms for 5 queries

# Optimized
cd ..
python3 run.py --output-dir ./optimized_results
# Expected: ~39ms for 5 queries

# Compare results
diff -r baseline/baseline_results optimized_results
# Should be identical
```

**Result: 13× faster!** 🚀
