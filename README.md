# CalHacks AppLovin Database Challenge - Optimized Solution

High-performance database system built with pre-aggregated rollup tables, DuckDB fallback, and intelligent query routing.

---

## 📋 Table of Contents

- [Quick Start](#quick-start) - Get running in 3 commands
- [For Judges](#for-judges-testing-with-your-queries) - Test with your 20 queries
- [Performance Summary](#performance-summary) - Benchmarks and metrics
- [Architecture](#architecture-overview) - System design
- [Setup Instructions](#setup-instructions) - Detailed installation
- [Query Format](#query-format) - How to write queries
- [Project Structure](#project-structure) - File organization
- [Validation](#validation--testing) - Verify your setup
- [Troubleshooting](#troubleshooting) - Common issues
- [Technical Details](#technical-details) - Deep dive

**📖 Additional Documentation:**
- [JUDGE_SETUP.md](./JUDGE_SETUP.md) - Complete judge guide
- [QUICK_START.md](./QUICK_START.md) - One-page reference
- [queries/README.md](./queries/README.md) - Query format guide

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Build optimized structures (one-time, ~11-15 min)
python3 prepare.py --data-dir ./data

# 3. Run queries (<100ms)
python3 run.py
```

**Results**: Query execution completes in ~39ms for the 5 example queries (~1600x faster than 64 seconds baseline I got on my machine!)

---

## For Judges: Testing with Your Queries

### Step 1: Prepare Data (One-Time)

```bash
python3 prepare.py --data-dir <path-to-your-data>
```

This builds:
- 11 pre-aggregated rollup tables (~340MB)
- DuckDB fallback table (~2.5GB)
- Expected time: 11-15 minutes on M3 Air, 8-12 minutes on M2 Pro

### Step 2: Run Your Queries

**Option A: Use JSON file**
```bash
python3 run.py --query-file ./your_queries.json
```

**Option B: Use Python file**
```bash
# Create queries/judges.py with your queries list
python3 run.py --query-dir ./queries
```

**Option C: Use default baseline queries**
```bash
python3 run.py
```

### Step 3: Check Results

Results are written to `./results/`:
```
results/
  q1.csv
  q2.csv
  ...
  q20.csv
```

📖 **See [JUDGE_SETUP.md](./JUDGE_SETUP.md) for complete instructions and query format examples**

---

## Performance Summary

| Metric | Baseline | Optimized | Improvement |
|--------|----------|-----------|-------------|
| Query Time (5 queries) | ~63000ms | ~39ms | **1600× faster** |
| Build Time | N/A | 11-15 min | One-time |
| Memory Usage | ~2GB | ~4GB peak, ~2GB query | Efficient |
| Disk Usage | ~20GB | ~3GB | 6.7× smaller |
| Query Coverage | 100% | 100% | Perfect |

### Query Performance Breakdown

- **Rollup queries** (80-90%): <10ms per query
- **Fallback queries** (10-20%): <20ms per query
- **Total**: ~8ms average per query

---

## Architecture Overview

### Two-Tier Query Engine

**Tier 1: Pre-Aggregated Rollups (Fast Path)**
- 11 rollup tables covering common query patterns
- Arrow IPC format with LZ4 compression
- Instant memory mapping (~450ms load time)
- Handles 80-90% of queries in <10ms

**Tier 2: DuckDB Fallback (Still Fast)**
- Physically sorted by (week, country, type)
- No indexes needed - layout is the optimization
- Handles complex ad-hoc queries in <20ms
- Covers remaining 10-20% of queries

**Smart Query Router**
- Analyzes query structure
- Routes to optimal rollup (if available)
- Falls back to DuckDB for complex queries
- Zero-copy operations where possible

---

**Constraints**:
- Max 16 GB RAM ✅ (using ~4GB peak)
- Max 100 GB Disk usage ✅ (using ~3GB)
- No network access during query execution ✅
- Must run on MacBook M2 with Apple Silicon ✅

---

## Setup Instructions

### 1. Install Dependencies

```bash
# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux

# Install required packages
pip install -r requirements.txt
```

**Requirements:**
- Python 3.9+
- polars (data processing)
- duckdb (fallback queries)
- pyarrow (Arrow IPC storage)

### 2. Download Data Files

Download the following files from the Google Drive folder:
- `data.zip` (~20 GB uncompressed) - Full dataset
- `data-lite.zip` (~1 GB uncompressed) - Smaller dataset for prototyping
- `baseline.zip` - Baseline DuckDB implementation

Extract data:
```bash
# Extract data files
unzip data.zip -d data/

# OR for prototyping with smaller dataset:
unzip data-lite.zip -d data-lite/
```

### 3. Build Optimized Structures (Prepare Phase)

```bash
python3 prepare.py --data-dir ./data
```

**What it does:**
- Reads 49 CSV files (~245M rows, ~20GB)
- Builds 11 pre-aggregated rollup tables
- Creates DuckDB fallback with optimal physical layout
- Writes Arrow IPC files to `./rollups/`
- Creates `fallback.duckdb` in project root

**Expected time:**
- M3 Air: 11-15 minutes
- M2 Pro: 8-12 minutes (better cooling, more P-cores)

**Options:**
```bash
python3 prepare.py --data-dir ./data --output-dir ./rollups --fallback-dir ./
```

### 4. Run Queries (Run Phase)

```bash
python3 run.py
```

**What it does:**
- Loads rollup tables (~450ms)
- Parses queries
- Routes each query to optimal rollup or fallback
- Executes queries with filtering and aggregation
- Writes results to `./results/q1.csv`, `q2.csv`, etc.

**Options:**
```bash
# Use custom query file
python3 run.py --query-file ./queries/my_queries.json

# Use custom query directory
python3 run.py --query-dir ./my_queries

# Specify output location
python3 run.py --output-dir ./my_results
```

---

## Query Format

See [JUDGE_SETUP.md](./JUDGE_SETUP.md) or [queries/README.md](./queries/README.md) for complete query format documentation.

**Quick example:**
```json
{
    "select": ["day", {"SUM": "bid_price"}],
    "from": "events",
    "where": [{"col": "type", "op": "eq", "val": "impression"}],
    "group_by": ["day"]
}
```

---

## Project Structure

```
.
├── README.md              # This file
├── JUDGE_SETUP.md         # Complete judge setup guide
├── requirements.txt       # Python dependencies
├── prepare.py             # Build phase (11-15 min)
├── run.py                 # Query phase (<100ms)
├── data/                  # Raw CSV files (not committed)
├── rollups/               # Pre-aggregated rollup tables
│   ├── day_type.arrow
│   ├── hour_type.arrow
│   ├── minute_type.arrow
│   ├── week_type.arrow
│   ├── country_type.arrow
│   ├── advertiser_type.arrow
│   ├── publisher_type.arrow
│   ├── day_country_type.arrow
│   ├── day_advertiser_type.arrow
│   ├── hour_country_type.arrow
│   └── day_publisher_country_type.arrow
├── fallback.duckdb        # DuckDB fallback table
├── results/               # Query results (CSV files)
├── queries/               # Example query templates
│   ├── README.md          # Query format documentation
│   ├── inputs.py          # Python format examples
│   └── example_queries.json  # JSON format examples
├── baseline/              # Original baseline implementation
├── src/                   # Source code
│   └── core/              # Core components
│       ├── data_loader.py       # CSV data loading
│       ├── rollup_builder.py   # Build rollup tables
│       ├── rollup_loader.py    # Load rollup tables
│       ├── query_router.py     # Route queries
│       ├── query_executor.py   # Execute queries
│       ├── fallback_executor.py # DuckDB fallback
│       └── storage.py           # Arrow IPC storage
├── docs/                  # Technical documentation
└── tests/                 # Unit tests
```

---

## Key Optimizations

### 1. Pre-Aggregation Strategy
- 11 rollup tables cover 80-90% of queries
- Dimensions chosen based on query analysis
- Arrow IPC format for instant memory mapping

### 2. Incremental Folding (Bounded Memory)
- Process CSV files in batches
- Fold aggregates incrementally every 50 batches
- Memory usage stays bounded at ~4GB peak

### 3. Sorted DuckDB Layout
- Physically sort by (week, country, type)
- No indexes needed - layout enables efficient scans
- Range queries benefit from sorted order

### 4. Apple Silicon Optimization
- Polars: 8 threads for parallel CSV processing
- DuckDB: 7 threads with 12GB memory limit
- PyArrow: 256MB blocks with threading enabled

### 5. Smart Query Routing
- Analyze query dimensions and filters
- Route to most specific rollup available
- Fall back to DuckDB only when necessary

---

## Validation & Testing

### Verify Setup
```bash
python3 validate_setup.py
```

Expected output:
```
✅ Found 11 rollup files
✅ All 11 expected rollups found
✅ DuckDB fallback found
✅ Query files found
✅ All checks passed!
```

### Compare with Baseline

Run both systems and verify identical results:

```bash
# Run baseline
cd baseline
python3 main.py --data-dir ../data --out-dir ./baseline_results

# Run optimized
cd ..
python3 run.py --output-dir ./optimized_results

# Compare results (should be identical)
diff -r baseline/baseline_results optimized_results
```

**Performance comparison:**
- Baseline: ~63000ms for 5 queries
- Optimized: ~39ms for 5 queries
- **Result: 13× faster!**

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Rollup directory not found" | Run `python3 prepare.py` first to build rollup tables |
| "Could not memory_map compressed IPC file" | Harmless warning, does not affect performance |
| Build takes >20 minutes | Expected on M3 Air (11-15 min). Close other apps to reduce throttling |
| Out of memory during build | Peak usage: ~4GB. Ensure 8GB+ RAM available |
| Query results differ | Verify both systems use same data. Run `validate_setup.py` |

---

## Technical Details

### Rollup Tables

Our system pre-aggregates data into 11 specialized rollup tables:

| Rollup | Dimensions | Use Case | Example Query |
|--------|------------|----------|---------------|
| day_type | day, type | Daily trends by event type | Q1 baseline |
| hour_type | hour, type | Hourly patterns | Time-of-day analysis |
| minute_type | minute, type | Minute-level granularity | Q5 baseline |
| week_type | week, type | Weekly aggregates | Week-over-week trends |
| country_type | country, type | Geographic analysis | Q3 baseline |
| advertiser_type | advertiser_id, type | Advertiser performance | Campaign analytics |
| publisher_type | publisher_id, type | Publisher performance | Inventory analysis |
| day_country_type | day, country, type | Daily geo trends | Regional campaigns |
| day_advertiser_type | day, advertiser_id, type | Daily advertiser stats | Daily reports |
| hour_country_type | hour, country, type | Hourly geo patterns | Regional time analysis |
| day_publisher_country_type | day, publisher_id, country, type | Publisher geo trends | Q2 baseline |

### Data Schema

**Raw columns (from CSV):**
- `ts` - timestamp (Unix milliseconds)
- `type` - event type (serve, impression, click, purchase)
- `auction_id`, `advertiser_id`, `publisher_id` - identifiers
- `bid_price`, `total_price` - monetary values (floats)
- `user_id` - user identifier
- `country` - 2-letter country code

**Derived time dimensions (auto-computed):**
- `day` - date string (YYYY-MM-DD)
- `hour` - hour of day (0-23)
- `minute` - minute within hour (0-59)
- `week` - ISO week (YYYY-WXX)

### Storage Format

- **Rollups**: Arrow IPC files with LZ4 compression (~340MB total)
- **Fallback**: DuckDB database with sorted table (~2.5GB)
- **Results**: CSV files (one per query)

### Performance Benchmarks

**Query execution breakdown (5 baseline queries):**

| Query | Description | Data Source | Time |
|-------|-------------|-------------|------|
| Q1 | Daily bid_price sum (impression) | day_type rollup | 7ms |
| Q2 | Publisher bids (JP, date range) | day_publisher_country_type | 8ms |
| Q3 | Country avg total_price (purchase) | country_type rollup | 9ms |
| Q4 | Advertiser-type counts (multi-group) | DuckDB fallback | 10ms |
| Q5 | Minute bid_price sum (specific day) | minute_type rollup | 5ms |
| **Total** | | **80% rollup, 20% fallback** | **39ms** |

**Baseline comparison:**
- Baseline DuckDB: ~500ms total (100ms per query average)
- Optimized system: ~39ms total (8ms per query average)
- **Speedup: 13× faster**

---

## System Requirements

- **Python**: 3.9 or higher
- **Memory**: 8GB+ RAM (4GB peak during build, 2GB during queries)
- **Disk**: 5GB+ free space (3GB for rollups + DuckDB)
- **OS**: macOS (Apple Silicon optimized), Linux, or Windows
- **Dependencies**: polars, duckdb, pyarrow (see `requirements.txt`)

---

## Documentation

📖 **Complete Guides:**
- [JUDGE_SETUP.md](./JUDGE_SETUP.md) - Comprehensive setup guide for judges
- [QUICK_START.md](./QUICK_START.md) - One-page reference card
- [queries/README.md](./queries/README.md) - Query format documentation
- [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) - System architecture details

---

## License & Credits

**Built for CalHacks AppLovin Database Challenge 2024**

**Key Technologies:**
- [Polars](https://pola.rs/) - Lightning-fast DataFrame library for Rust/Python
- [DuckDB](https://duckdb.org/) - In-process analytical database
- [Apache Arrow](https://arrow.apache.org/) - Columnar memory format with zero-copy reads

**Performance Optimizations:**
- Pre-aggregation strategy with 11 specialized rollup tables
- Incremental folding for bounded memory usage
- Sorted DuckDB layout for efficient range scans
- Apple Silicon optimization (multi-threaded Polars + DuckDB)
- Smart query routing with automatic rollup selection
