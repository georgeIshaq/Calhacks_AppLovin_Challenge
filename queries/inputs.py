#!/usr/bin/env python3
"""
Example Queries for Testing

This file demonstrates the query format expected by the system.
Judges can replace these with their own 20 test queries.

Query Format:
{
    "select": [<columns>, {<aggregate>: <column>}, ...],
    "from": "events",
    "where": [<filter_conditions>],
    "group_by": [<columns>],
    "order_by": [{"col": <column>, "dir": "asc|desc"}]
}

Filter Operations:
- "eq": equals
- "gt": greater than
- "lt": less than
- "gte": greater than or equal
- "lte": less than or equal
- "between": between two values
- "in": in list of values

Aggregate Functions:
- SUM, AVG, COUNT, MIN, MAX

Available Columns:
- ts: timestamp (Unix milliseconds)
- type: event type (serve, impression, click, purchase)
- auction_id: auction identifier
- advertiser_id: advertiser ID
- publisher_id: publisher ID
- bid_price: bid price (float)
- user_id: user ID
- total_price: total price (float)
- country: country code (2-letter)

Derived Columns (automatically available):
- day: date (YYYY-MM-DD)
- hour: hour of day (0-23)
- minute: minute within hour (0-59)
- week: ISO week (YYYY-WXX)
"""

# Example queries demonstrating various patterns
queries = [
    # Query 1: Simple aggregation by day
    {
        "select": ["day", {"SUM": "bid_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "impression"}],
        "group_by": ["day"],
    },
    
    # Query 2: Multi-dimensional aggregation with filters
    {
        "select": ["publisher_id", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "country", "op": "eq", "val": "JP"},
            {"col": "day", "op": "between", "val": ["2024-10-20", "2024-10-23"]}
        ],
        "group_by": ["publisher_id"],
    },
    
    # Query 3: Average with ordering
    {
        "select": ["country", {"AVG": "total_price"}],
        "from": "events",
        "where": [{"col": "type", "op": "eq", "val": "purchase"}],
        "group_by": ["country"],
        "order_by": [{"col": "AVG(total_price)", "dir": "desc"}]
    },
    
    # Query 4: Multi-group aggregation
    {
        "select": ["advertiser_id", "type", {"COUNT": "*"}],
        "from": "events",
        "group_by": ["advertiser_id", "type"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    
    # Query 5: Time-based aggregation (minute-level)
    {
        "select": ["minute", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "eq", "val": "2024-06-01"}
        ],
        "group_by": ["minute"],
        "order_by": [{"col": "minute", "dir": "asc"}]
    },
    
    # Query 6: Multiple event types
    {
        "select": ["day", "type", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "in", "val": ["impression", "click"]}
        ],
        "group_by": ["day", "type"],
        "order_by": [{"col": "day", "dir": "asc"}]
    },
    
    # Query 7: Week-based aggregation
    {
        "select": ["week", "country", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "serve"}
        ],
        "group_by": ["week", "country"],
    },
    
    # Query 8: Publisher performance by hour
    {
        "select": ["hour", "publisher_id", {"AVG": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "country", "op": "eq", "val": "US"}
        ],
        "group_by": ["hour", "publisher_id"],
    },
    
    # Query 9: Date range with multiple aggregates
    {
        "select": ["day", {"COUNT": "*"}, {"SUM": "total_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "purchase"},
            {"col": "day", "op": "between", "val": ["2024-06-01", "2024-06-30"]}
        ],
        "group_by": ["day"],
        "order_by": [{"col": "day", "dir": "asc"}]
    },
    
    # Query 10: Complex multi-dimensional query
    {
        "select": ["country", "advertiser_id", {"SUM": "bid_price"}, {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "click"},
            {"col": "day", "op": "gte", "val": "2024-10-01"}
        ],
        "group_by": ["country", "advertiser_id"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    }
]

# To add your own queries, follow the format above and append to the list:
# queries.append({
#     "select": [...],
#     "from": "events",
#     "where": [...],
#     "group_by": [...],
#     "order_by": [...]
# })
