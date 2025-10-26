#!/usr/bin/env python3
"""
Test Q16 with baseline DuckDB to verify correctness
"""

import duckdb
import json
from pathlib import Path
import time

# Load Q16 query
with open('queries_test/test_q16_week_country_gap.json') as f:
    q16 = json.load(f)

print("Q16 Query:", json.dumps(q16, indent=2))
print()

# Create DuckDB connection and load data
con = duckdb.connect(':memory:')

print("Loading data into DuckDB...")
start = time.time()
con.execute("""
    CREATE TABLE events AS
    WITH raw AS (
      SELECT *
      FROM read_csv(
        'data-lite/events_part_*.csv',
        AUTO_DETECT = FALSE,
        HEADER = TRUE,
        union_by_name = TRUE,
        COLUMNS = {
          'ts': 'VARCHAR',
          'type': 'VARCHAR',
          'auction_id': 'VARCHAR',
          'advertiser_id': 'VARCHAR',
          'publisher_id': 'VARCHAR',
          'bid_price': 'VARCHAR',
          'user_id': 'VARCHAR',
          'total_price': 'VARCHAR',
          'country': 'VARCHAR'
        }
      )
    ),
    casted AS (
      SELECT
        to_timestamp(TRY_CAST(ts AS DOUBLE) / 1000.0) AS ts,
        type,
        TRY_CAST(advertiser_id AS INTEGER) AS advertiser_id,
        TRY_CAST(publisher_id AS INTEGER) AS publisher_id,
        NULLIF(bid_price, '')::DOUBLE AS bid_price,
        country
      FROM raw
    )
    SELECT
      ts,
      STRFTIME(ts, '%Y-W%V') AS week,
      DATE(ts) AS day,
      DATE_TRUNC('hour', ts) AS hour,
      STRFTIME(ts, '%Y-%m-%d %H:%M') AS minute,
      type,
      advertiser_id,
      publisher_id,
      bid_price,
      country
    FROM casted;
""")
load_time = time.time() - start
print(f"✅ Data loaded in {load_time:.2f}s")
print()

# Build SQL for Q16
sql = """
    SELECT week, country, SUM(bid_price) AS "SUM(bid_price)"
    FROM events
    WHERE type = 'impression'
    GROUP BY week, country
    ORDER BY "SUM(bid_price)" DESC
    LIMIT 20
"""

print("Executing Q16...")
print(sql)
print()

start = time.time()
result = con.execute(sql)
rows = result.fetchall()
query_time = time.time() - start

print(f"✅ Query complete: {len(rows)} rows in {query_time*1000:.1f}ms")
print()
print("Results (first 20 rows):")
print("week, country, SUM(bid_price)")
for row in rows:
    print(f"{row[0]}, {row[1]}, {row[2]:.2f}")

# Write to CSV for comparison
with open('baseline_q16_result.csv', 'w') as f:
    f.write('week,country,"SUM(bid_price)"\n')
    for row in rows:
        f.write(f'{row[0]},{row[1]},{row[2]}\n')

print()
print("✅ Results written to: baseline_q16_result.csv")
