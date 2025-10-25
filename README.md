# CalHacks AppLovin Database Challenge

Optimize a database system to retrieve and process data smarter and faster than a provided DuckDB baseline.

## Challenge Overview

**Goal**: Build a high-performance database system that can:
- Load and optimize ~20 GB of ad event data
- Execute SQL-like queries faster than the DuckDB baseline
- Demonstrate sound architectural and design choices

**Constraints**:
- Max 16 GB RAM
- Max 100 GB Disk usage
- No network access during query execution (Run phase)
- Must run on MacBook M2 with Apple Silicon

## Setup Instructions

### 1. Download Data Files

Download the following files from the Google Drive folder:
- `data.zip` (~20 GB uncompressed) - Full dataset
- `data-lite.zip` (~1 GB uncompressed) - Smaller dataset for prototyping
- `baseline.zip` - Baseline DuckDB implementation
- Result CSV files - Expected outputs for benchmark queries

Place the downloaded files in the project root directory.

### 2. Extract Files

```bash
# Extract baseline code
unzip baseline.zip

# Extract data (choose one based on your needs)
# For prototyping:
unzip data-lite.zip -d data-lite/

# For final testing (requires ~20 GB space):
unzip data.zip -d data/
```

### 3. Set Up Python Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On macOS/Linux
# or
# venv\Scripts\activate  # On Windows

# Install dependencies
pip install -r requirements.txt
```

### 4. Run Baseline

```bash
# Run the baseline DuckDB solution
python src/baseline_runner.py

# Or run with lite data for testing
python src/baseline_runner.py --data-dir data-lite
```

## Project Structure

```
.
├── README.md              # This file
├── requirements.txt       # Python dependencies
├── .gitignore            # Git ignore rules
├── data/                 # Full dataset (20 GB, not committed)
├── data-lite/            # Lite dataset (1 GB, not committed)
├── queries/              # Query JSON files
├── results/              # Expected query results (CSV files)
├── src/                  # Source code
│   ├── baseline_runner.py    # Run baseline DuckDB solution
│   ├── query_parser.py       # Parse JSON queries
│   ├── optimized/            # Your optimized implementation
│   └── utils/                # Helper utilities
├── docs/                 # Documentation
│   ├── architecture.md       # System architecture
│   ├── benchmarks.md         # Performance benchmarks
│   └── design_decisions.md   # Design rationale
└── tests/                # Test files

```

## Data Schema

The dataset contains ad event data with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `ts` | long (Unix ms) | Event timestamp |
| `type` | ENUM | Event type: serve, impression, click, purchase |
| `auction_id` | UUID | Unique auction identifier |
| `advertiser_id` | int | Advertiser identifier |
| `publisher_id` | int | Publisher identifier |
| `bid_price` | float | Bid price in USD (impression events only) |
| `user_id` | int | Anonymized user identifier |
| `total_price` | float | Purchase amount in USD (purchase events only) |
| `country` | string | ISO 3166-1 alpha-2 country code |

## Query Format

Queries are provided as JSON with the following structure:

```json
{
  "select": ["column_name", {"AGG_FUNC": "column_name"}],
  "from": "events",
  "where": [
    {"col": "column_name", "op": "eq|neq|in|between", "val": "value"}
  ],
  "group_by": ["column_name"],
  "order_by": [{"col": "column_name", "dir": "asc|desc"}]
}
```

**Supported operations**:
- SELECT: columns and aggregates (SUM, COUNT, AVG)
- WHERE: eq, neq, in, between (AND-combined)
- GROUP BY: multiple columns
- ORDER BY: asc/desc sorting
- Time dimensions: day, week, hour, minute

## Development Workflow

### Phase 1: Prepare (Data Loading & Optimization)
1. Load raw CSV data
2. Design optimal storage layout (partitioning, compression, etc.)
3. Build indexes and pre-compute aggregations
4. Store optimized data structures

### Phase 2: Run (Query Execution)
1. Parse JSON query structure
2. Execute query against optimized data store
3. Return correct results
4. No network access allowed

## Evaluation Criteria

- **Performance & Accuracy (40%)**: Speed + correctness
- **Technical Depth (30%)**: System quality + architecture
- **Creativity (20%)**: Novel/elegant approaches
- **Documentation (10%)**: Clear presentation

## Analysis & Planning

📊 **Preliminary Test Results**: See [`docs/PRELIMINARY_TEST_RESULTS.md`](docs/PRELIMINARY_TEST_RESULTS.md)  
📋 **Optimization Strategy**: See [`docs/OPTIMIZATION_STRATEGY.md`](docs/OPTIMIZATION_STRATEGY.md)

### Key Findings
- **225M rows**, 19GB raw data, 366 days (2024)
- **Type partitioning**: 80% of queries filter by type → 65-99% data skip
- **Pre-aggregations**: Reduce to 1.5K-6K rows (vs 225M) → 1000-10000× speedup
- **LZ4 compression**: 3GB/s decompression on M2, 3× compression ratio
- **Expected total speedup**: 260-1300× (baseline 65s → target <500ms)

### Analysis Tools
```bash
# Analyze query patterns and data characteristics
python src/analysis/predicate_stats.py

# Benchmark compression strategies
python src/analysis/compression_bench.py

# Analyze data distribution and selectivity
python src/analysis/data_distribution.py --sample-ratio 0.2
```

## Next Steps

1. ✅ Extract and explore the baseline code
2. ✅ Run baseline benchmarks and record timings (65.18s total)
3. ✅ Analyze query patterns and data characteristics
4. ⏭️ Design your optimized system architecture
5. ⏭️ Implement, test, and benchmark your solution
6. ⏭️ Document your approach and results

## Resources

- [DuckDB Documentation](https://duckdb.org/docs/)
- Baseline code in `baseline.zip`
- Expected results in `results/` directory

Good luck! 🚀
