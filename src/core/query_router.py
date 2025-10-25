#!/usr/bin/env python3
"""
Query Router - Pattern matching and rollup selection

This module analyzes query patterns and routes them to the optimal rollup.

Key responsibilities:
1. Parse query structure (SELECT, WHERE, GROUP BY, ORDER BY)
2. Match query dimensions to available rollups
3. Select the smallest/best rollup for execution
4. Handle dimension equivalences (minute ≈ day + time, hour ≈ day + hour)

Example routing:
- GROUP BY day + WHERE type='impression' → day_type rollup
- GROUP BY country + WHERE type='purchase' → country_type rollup
- GROUP BY advertiser_id, type → advertiser_type rollup
"""

import logging
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class QueryPattern:
    """Parsed query structure"""
    select_cols: List[str]          # Non-aggregate columns
    aggregates: List[Dict]           # Aggregate functions: [{"func": "SUM", "col": "bid_price"}, ...]
    group_by: List[str]              # GROUP BY dimensions
    where_filters: List[Dict]        # WHERE conditions: [{"col": "type", "op": "eq", "val": "impression"}, ...]
    order_by: List[Dict]             # ORDER BY: [{"col": "day", "dir": "asc"}, ...]
    
    def __str__(self):
        dims = ', '.join(self.group_by) if self.group_by else 'none'
        aggs = ', '.join(f"{a['func']}({a['col']})" for a in self.aggregates)
        filters = ', '.join(f"{f['col']}={f['val']}" for f in self.where_filters)
        return f"Pattern(dims=[{dims}], aggs=[{aggs}], filters=[{filters}])"


class QueryRouter:
    """
    Routes queries to optimal rollups based on query patterns.
    
    Available rollups (9 total):
    - day_type: day × type (1.5K rows)
    - hour_type: hour × type (34K rows)
    - week_type: week × type (212 rows)
    - country_type: country × type (48 rows)
    - advertiser_type: advertiser_id × type (6.6K rows)
    - publisher_type: publisher_id × type (4.5K rows)
    - day_country_type: day × country × type (16.8K rows)
    - day_advertiser_type: day × advertiser_id × type (1.8M rows)
    - hour_country_type: hour × country × type (329K rows)
    """
    
    # Map of rollup name -> (dimensions, estimated_rows)
    ROLLUP_CATALOG = {
        'day_type': (['day', 'type'], 1_464),
        'hour_type': (['hour', 'type'], 34_177),
        'minute_type': (['minute', 'type'], 527_040),
        'week_type': (['week', 'type'], 212),
        'country_type': (['country', 'type'], 48),
        'advertiser_type': (['advertiser_id', 'type'], 6_616),
        'publisher_type': (['publisher_id', 'type'], 4_456),
        'day_country_type': (['day', 'country', 'type'], 16_835),
        'day_advertiser_type': (['day', 'advertiser_id', 'type'], 1_834_876),
        'hour_country_type': (['hour', 'country', 'type'], 329_480),
    }
    
    # Dimension equivalences for temporal queries
    DIMENSION_EQUIVALENCES = {
        'minute': ['day', 'hour'],  # minute queries can use day or hour rollups
        'day': ['week'],            # day queries can use week rollups (with conversion)
    }
    
    # Columns that can be derived from other columns (don't need to be in rollup)
    DERIVABLE_COLUMNS = {
        'day': ['minute', 'hour'],  # day can be extracted from minute or hour
        'hour': ['minute'],         # hour can be extracted from minute
    }
    
    def __init__(self):
        """Initialize query router with rollup catalog."""
        self.catalog = self.ROLLUP_CATALOG.copy()
        logger.info(f"Query router initialized with {len(self.catalog)} rollups")
    
    def parse_query(self, query: Dict) -> QueryPattern:
        """
        Parse query JSON into structured pattern.
        
        Args:
            query: Query dictionary with 'select', 'where', 'group_by', etc.
        
        Returns:
            QueryPattern with extracted structure
        
        Example:
            Input: {
                "select": ["day", {"SUM": "bid_price"}],
                "where": [{"col": "type", "op": "eq", "val": "impression"}],
                "group_by": ["day"]
            }
            
            Output: QueryPattern(
                select_cols=["day"],
                aggregates=[{"func": "SUM", "col": "bid_price"}],
                group_by=["day"],
                where_filters=[{"col": "type", "op": "eq", "val": "impression"}],
                order_by=[]
            )
        """
        # Parse SELECT clause
        select_cols = []
        aggregates = []
        
        for item in query.get('select', []):
            if isinstance(item, str):
                # Regular column
                select_cols.append(item)
            elif isinstance(item, dict):
                # Aggregate function: {"SUM": "bid_price"} or {"COUNT": "*"}
                for func, col in item.items():
                    aggregates.append({
                        'func': func.upper(),
                        'col': col
                    })
        
        # Parse GROUP BY
        group_by = query.get('group_by', [])
        
        # Parse WHERE filters
        where_filters = query.get('where', [])
        
        # Parse ORDER BY
        order_by = query.get('order_by', [])
        
        pattern = QueryPattern(
            select_cols=select_cols,
            aggregates=aggregates,
            group_by=group_by,
            where_filters=where_filters,
            order_by=order_by
        )
        
        logger.debug(f"Parsed query: {pattern}")
        return pattern
    
    def extract_filter_columns(self, filters: List[Dict]) -> Set[str]:
        """
        Extract ALL columns referenced in WHERE filters.
        
        These columns must exist in the rollup for filtering to work.
        
        Args:
            filters: List of WHERE conditions
        
        Returns:
            Set of column names used in filters
        
        Example:
            Input: [
                {"col": "type", "op": "eq", "val": "impression"},
                {"col": "country", "op": "eq", "val": "JP"},
                {"col": "day", "op": "between", "val": ["2024-10-20", "2024-10-23"]}
            ]
            Output: {"type", "country", "day"}
        """
        filter_cols = set()
        
        for f in filters:
            col = f.get('col')
            if col:
                filter_cols.add(col)
        
        return filter_cols
    
    def find_best_rollup(self, pattern: QueryPattern) -> Tuple[Optional[str], Optional[List[str]]]:
        """
        Find the best rollup for the query pattern.
        Returns None if no suitable rollup exists (fallback to raw data scan).
        
        Selection criteria:
        1. Must contain all GROUP BY dimensions (or equivalents)
        2. Must contain all WHERE filter columns (for filtering to work)
        3. Prefer smallest rollup (fewer rows = faster)
        
        Args:
            pattern: Parsed query pattern
        
        Returns:
            Tuple of (rollup_name, dimensions_in_rollup)
        
        Raises:
            ValueError: If no suitable rollup found
        
        Example:
            Query: GROUP BY day WHERE type='impression' AND country='JP'
            Required dimensions: {day}
            Filter columns: {type, country}
            
            Candidates:
            - day_type (day × type): ❌ Missing country column
            - day_country_type (day × country × type): ✅ Has all columns
            
            Best: day_country_type
        """
        required_dims = set(pattern.group_by)
        filter_cols = self.extract_filter_columns(pattern.where_filters)
        
        # Check which filter columns can be derived from rollup dimensions
        # For example: if rollup has 'minute', we can filter by 'day' by parsing the minute string
        derivable_filters = set()
        for filter_col in filter_cols:
            if filter_col in self.DERIVABLE_COLUMNS:
                # Check if rollup has a column this can be derived from
                source_cols = self.DERIVABLE_COLUMNS[filter_col]
                derivable_filters.add(filter_col)
        
        # Columns that MUST be in rollup (can't be derived)
        must_have_cols = required_dims | (filter_cols - derivable_filters)
        
        logger.debug(f"Finding rollup for: group_by={required_dims}, filters={filter_cols}, must_have={must_have_cols}, derivable={derivable_filters}")
        
        # Find all candidate rollups
        candidates = []
        
        for rollup_name, (rollup_dims, row_count) in self.catalog.items():
            rollup_dim_set = set(rollup_dims)
            
            # Check if rollup contains all must-have columns
            if must_have_cols.issubset(rollup_dim_set):
                # Check if derivable filters can actually be derived
                can_derive_all = True
                for derivable_col in derivable_filters:
                    source_cols = self.DERIVABLE_COLUMNS[derivable_col]
                    if not any(src in rollup_dim_set for src in source_cols):
                        can_derive_all = False
                        break
                
                if can_derive_all:
                    # This rollup can answer the query!
                    candidates.append((rollup_name, rollup_dims, row_count))
                    logger.debug(f"  ✅ {rollup_name}: dims={rollup_dims}, rows={row_count:,}")
                else:
                    logger.debug(f"  ❌ {rollup_name}: can't derive {derivable_filters}")
            else:
                # Missing required dimensions
                missing = must_have_cols - rollup_dim_set
                logger.debug(f"  ❌ {rollup_name}: missing {missing}")
        
        if not candidates:
            # Try dimension equivalences (e.g., minute → day/hour)
            candidates = self._try_dimension_equivalences(pattern)
            
            if not candidates:
                logger.warning(
                    f"No rollup found for query pattern: {pattern}\n"
                    f"Required dimensions: {required_dims}\n"
                    f"Filter columns: {filter_cols}\n"
                    f"Will use fallback to raw data scan"
                )
                return None, None
        
        # Select best candidate (smallest row count)
        best = min(candidates, key=lambda x: x[2])  # x[2] is row_count
        rollup_name, rollup_dims, row_count = best
        
        logger.info(f"Selected rollup: {rollup_name} ({row_count:,} rows)")
        return rollup_name, rollup_dims
    
    def _try_dimension_equivalences(self, pattern: QueryPattern) -> List[Tuple]:
        """
        Try to match query using dimension equivalences.
        
        For example:
        - Query GROUP BY minute → Use day_type rollup + filter by day
        - Query GROUP BY day → Use week_type rollup + expand weeks to days
        
        Args:
            pattern: Query pattern
        
        Returns:
            List of candidate rollups: [(name, dims, row_count), ...]
        """
        candidates = []
        required_dims = set(pattern.group_by)
        
        # Try substituting each required dimension with equivalents
        for dim in required_dims:
            if dim in self.DIMENSION_EQUIVALENCES:
                equivalent_dims = self.DIMENSION_EQUIVALENCES[dim]
                
                for equiv_dim in equivalent_dims:
                    # Replace original dim with equivalent
                    modified_dims = (required_dims - {dim}) | {equiv_dim}
                    
                    # Check if any rollup matches
                    for rollup_name, (rollup_dims, row_count) in self.catalog.items():
                        rollup_dim_set = set(rollup_dims)
                        
                        if modified_dims.issubset(rollup_dim_set):
                            candidates.append((rollup_name, rollup_dims, row_count))
                            logger.debug(f"  ✅ {rollup_name} (via {dim}→{equiv_dim}): "
                                       f"dims={rollup_dims}, rows={row_count:,}")
        
        return candidates
    
    def route_query(self, query: Dict) -> Tuple[Optional[str], QueryPattern]:
        """
        Route query to optimal rollup.
        
        This is the main entry point for query routing.
        Returns None for rollup_name if no suitable rollup exists (fallback needed).
        
        Args:
            query: Query dictionary
        
        Returns:
            Tuple of (rollup_name, parsed_pattern)
        
        Example:
            >>> router = QueryRouter()
            >>> query = {
            ...     "select": ["day", {"SUM": "bid_price"}],
            ...     "where": [{"col": "type", "op": "eq", "val": "impression"}],
            ...     "group_by": ["day"]
            ... }
            >>> rollup_name, pattern = router.route_query(query)
            >>> print(rollup_name)
            'day_type'
        """
        # Parse query structure
        pattern = self.parse_query(query)
        
        # Find best rollup
        rollup_name, rollup_dims = self.find_best_rollup(pattern)
        
        logger.info(f"Routed query to: {rollup_name}")
        logger.info(f"  Pattern: {pattern}")
        
        return rollup_name, pattern


def main():
    """Test query router with sample queries."""
    from baseline.inputs import queries
    
    router = QueryRouter()
    
    print("="*60)
    print("Query Router Test")
    print("="*60)
    
    for i, query in enumerate(queries, 1):
        print(f"\n{'='*60}")
        print(f"Query {i}:")
        print(f"{'='*60}")
        print(f"GROUP BY: {query.get('group_by', [])}")
        print(f"WHERE: {query.get('where', [])}")
        
        try:
            rollup_name, pattern = router.route_query(query)
            print(f"\n✅ Routed to: {rollup_name}")
            print(f"   Pattern: {pattern}")
        except ValueError as e:
            print(f"\n❌ ERROR: {e}")


if __name__ == "__main__":
    main()
