# Query Templates

This directory contains example query templates for testing the system.

## Files

- **`inputs.py`**: Python format with 10 example queries and documentation
- **`example_queries.json`**: JSON format with 5 example queries

## Usage

### Option 1: Python Format (Recommended)

Edit `inputs.py` and modify the `queries` list. Then run:

```bash
python3 run.py --query-dir ./queries
```

### Option 2: JSON Format

Create a JSON file with your queries:

```bash
python3 run.py --query-file ./queries/my_queries.json
```

### Option 3: Default Baseline

Run without arguments to use baseline queries:

```bash
python3 run.py
```

## Query Format

Each query is a dictionary with these fields:

```python
{
    "select": ["column1", {"AGGREGATE": "column2"}],
    "from": "events",
    "where": [
        {"col": "column", "op": "operator", "val": value}
    ],
    "group_by": ["column1"],
    "order_by": [{"col": "column", "dir": "asc|desc"}]
}
```

### Available Operators

- `"eq"`: equals
- `"gt"`: greater than
- `"lt"`: less than
- `"gte"`: greater than or equal
- `"lte"`: less than or equal
- `"between"`: between two values (val is array `[start, end]`)
- `"in"`: in list of values (val is array)

### Available Aggregates

- `SUM`, `AVG`, `COUNT`, `MIN`, `MAX`

### Available Columns

**Raw columns:**
- `ts`: timestamp (Unix milliseconds)
- `type`: event type (serve, impression, click, purchase)
- `auction_id`, `advertiser_id`, `publisher_id`
- `bid_price`, `total_price`: float values
- `user_id`: user identifier
- `country`: 2-letter country code

**Derived columns (automatically available):**
- `day`: date (YYYY-MM-DD)
- `hour`: hour of day (0-23)
- `minute`: minute within hour (0-59)
- `week`: ISO week (YYYY-WXX)

## For Judges: Adding Your 20 Test Queries

1. **Option A**: Edit `inputs.py` and replace the `queries` list with your 20 queries
2. **Option B**: Create `judges.json` with your queries in JSON format
3. Run: `python3 run.py --query-dir ./queries` or `python3 run.py --query-file ./queries/judges.json`
4. Results will be written to `./results/q1.csv` through `./results/q20.csv`

## Performance Expectations

- **Query time**: <10ms per query on average
- **Total time**: <100ms for 5 queries, <200ms for 20 queries
- **Coverage**: 80-90% queries use pre-aggregated rollups (ultra-fast)
- **Fallback**: 10-20% queries use DuckDB (still very fast)
