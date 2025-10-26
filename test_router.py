#!/usr/bin/env python3
"""Test router to see why Q2 isn't matching"""

from src.core.query_router import QueryRouter
import json

router = QueryRouter()

# Load Q2
with open('queries/q2_publisher_japan.json') as f:
    q2 = json.load(f)

print('='*60)
print('Q2 Query:')
print('='*60)
print(json.dumps(q2, indent=2))
print()

print('='*60)
print('Router Catalog:')
print('='*60)
for name, (dims, rows) in router.catalog.items():
    print(f"  {name}: {dims} ({rows:,} rows)")
print()

print('='*60)
print('Routing Q2:')
print('='*60)
rollup_name, pattern = router.route_query(q2)
print()
print(f'✅ Result: {rollup_name}')
print(f'   Pattern: {pattern}')
