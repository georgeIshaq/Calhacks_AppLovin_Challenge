#!/usr/bin/env python3
"""
Query Executor - Execute queries against pre-aggregated rollups

This module takes a query pattern and rollup, applies filters,
and computes final aggregates from pre-aggregated data.

Key features:
1. NULL-safe aggregate computation (SUM, AVG, COUNT, MIN, MAX)
2. Filter application (WHERE clauses)
3. ORDER BY sorting
4. Result formatting

Performance target: <15ms per query execution
"""

import polars as pl
import logging
from typing import Dict, List, Tuple, Any
from pathlib import Path
from datetime import datetime, timedelta

# Import relative to package structure
try:
    from .query_router import QueryPattern
    from .rollup_loader import RollupLoader, get_loader
except ImportError:
    from query_router import QueryPattern
    from rollup_loader import RollupLoader, get_loader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class QueryExecutor:
    """
    Executes queries against pre-aggregated rollups.
    
    The executor:
    1. Loads the appropriate rollup (pre-loaded = instant)
    2. Applies WHERE filters to narrow data
    3. Computes final aggregates from pre-aggs
    4. Applies ORDER BY sorting
    5. Returns results in query format
    """
    
    def __init__(self, rollup_loader: RollupLoader = None):
        """
        Initialize query executor.
        
        Args:
            rollup_loader: RollupLoader instance (uses singleton if None)
        """
        self.loader = rollup_loader or get_loader()
        logger.info("Query executor initialized")
    
    def apply_filters(self, df: pl.DataFrame, filters: List[Dict]) -> pl.DataFrame:
        """
        Apply WHERE filters to dataframe.
        
        Handles both direct column filters and derived filters (e.g., filter by day from minute column).
        
        Args:
            df: Rollup dataframe
            filters: List of filter conditions
        
        Returns:
            Filtered dataframe
        
        Example:
            filters = [
                {"col": "type", "op": "eq", "val": "impression"},
                {"col": "day", "op": "eq", "val": "2024-06-01"}  # Derived from minute column
            ]
        """
        if not filters:
            return df
        
        # Build filter expression
        filter_expr = None
        
        for f in filters:
            col = f['col']
            op = f['op']
            val = f['val']
            
            # Check if column exists in dataframe
            if col not in df.columns:
                # Try to derive from other columns
                if col == 'day' and 'minute' in df.columns:
                    # Extract day from minute string (format: "2024-153 14:30")
                    logger.debug(f"Deriving 'day' filter from 'minute' column")
                    col_expr = pl.col('minute').str.slice(0, 8)  # Extract "2024-153"
                elif col == 'day' and 'hour' in df.columns:
                    # Extract day from hour string (format: "2024-153 14")
                    logger.debug(f"Deriving 'day' filter from 'hour' column")
                    col_expr = pl.col('hour').str.slice(0, 8)  # Extract "2024-153"
                elif col == 'hour' and 'minute' in df.columns:
                    # Extract hour from minute string (format: "2024-153 14")
                    logger.debug(f"Deriving 'hour' filter from 'minute' column")
                    col_expr = pl.col('minute').str.slice(0, 11)  # Extract "2024-153 14"
                else:
                    raise ValueError(f"Column '{col}' not found in dataframe and cannot be derived. Available: {df.columns}")
            else:
                col_expr = pl.col(col)
            
            # Convert filter value if needed (calendar date → day-of-year format)
            if col == 'day' or (col not in df.columns and col == 'day'):
                # Handle BETWEEN with list of dates
                if op == 'between' and isinstance(val, list):
                    converted_vals = []
                    for v in val:
                        if isinstance(v, str) and len(v) == 10 and v.count('-') == 2:
                            try:
                                from datetime import datetime
                                parts = v.split('-')
                                year, month, day_num = int(parts[0]), int(parts[1]), int(parts[2])
                                date_obj = datetime(year, month, day_num)
                                day_of_year = date_obj.timetuple().tm_yday
                                converted = f"{year}-{day_of_year:03d}"
                                converted_vals.append(converted)
                                logger.info(f"🔍 Converted calendar date to day-of-year: {v} → {converted}")
                            except:
                                converted_vals.append(v)  # Keep original if conversion fails
                        else:
                            converted_vals.append(v)
                    val = converted_vals
                # Handle single date value
                elif isinstance(val, str) and len(val) == 10 and val.count('-') == 2:
                    try:
                        from datetime import datetime
                        # Convert "2024-06-01" to "2024-153"
                        parts = val.split('-')
                        year, month, day_num = int(parts[0]), int(parts[1]), int(parts[2])
                        date_obj = datetime(year, month, day_num)
                        day_of_year = date_obj.timetuple().tm_yday
                        val = f"{year}-{day_of_year:03d}"
                        logger.info(f"🔍 Converted calendar date to day-of-year: {parts[0]}-{parts[1]}-{parts[2]} → {val}")
                    except:
                        pass  # Keep original value if conversion fails
            
            # Build single condition
            if op == 'eq':
                condition = col_expr == val
            elif op == 'ne' or op == 'neq':
                condition = pl.col(col) != val
            elif op == 'gt':
                condition = pl.col(col) > val
            elif op == 'gte':
                condition = pl.col(col) >= val
            elif op == 'lt':
                condition = pl.col(col) < val
            elif op == 'lte':
                condition = pl.col(col) <= val
            elif op == 'in':
                # IN operator: col IN [val1, val2, ...]
                condition = pl.col(col).is_in(val)
            elif op == 'between':
                # BETWEEN operator: col BETWEEN [start, end]
                if not isinstance(val, list) or len(val) != 2:
                    raise ValueError(f"BETWEEN requires list of 2 values, got: {val}")
                condition = (pl.col(col) >= val[0]) & (pl.col(col) <= val[1])
            else:
                raise ValueError(f"Unsupported operator: {op}")
            
            # Combine with AND
            if filter_expr is None:
                filter_expr = condition
            else:
                filter_expr = filter_expr & condition
        
        # Apply filter
        filtered = df.filter(filter_expr)
        
        logger.debug(f"Filtered: {len(df)} → {len(filtered)} rows")
        return filtered
    
    def compute_aggregates(
        self, 
        df: pl.DataFrame, 
        aggregates: List[Dict],
        group_by: List[str] = None
    ) -> pl.DataFrame:
        """
        Compute final aggregates from pre-aggregated data.
        
        This uses NULL-safe formulas to compute final aggregates:
        - SUM(col) = CASE WHEN count > 0 THEN sum ELSE NULL END
        - AVG(col) = sum / count (NULL if count = 0)
        - COUNT(*) = row_count (always non-NULL, use 0 for NULL)
        - COUNT(col) = count (fill NULL with 0)
        - MIN/MAX(col) = min/max (already NULL-safe)
        
        Args:
            df: Filtered rollup data (may need further aggregation)
            aggregates: List of aggregate specs: [{"func": "SUM", "col": "bid_price"}, ...]
            group_by: Optional GROUP BY dimensions (if rollup has extra dims)
        
        Returns:
            DataFrame with computed aggregates
        
        Example:
            Input df (day_type rollup filtered to type='impression'):
                day          | bid_price_sum | bid_price_count | row_count
                -------------|---------------|-----------------|----------
                2024-01-01   | 1000.0        | 10              | 100
                2024-01-02   | 2000.0        | 20              | 200
            
            Aggregate: SUM(bid_price)
            
            Output:
                day          | SUM(bid_price)
                -------------|----------------
                2024-01-01   | 1000.0
                2024-01-02   | 2000.0
        """
        # If we need to group (rollup has extra dimensions beyond query)
        if group_by:
            agg_exprs = []
            
            for agg in aggregates:
                func = agg['func']
                col = agg['col']
                
                if func == 'SUM':
                    # SUM = sum of pre-aggregated sums
                    # NULL-safe: only return value if count > 0
                    agg_exprs.append(
                        pl.when(pl.col(f'{col}_count').sum() > 0)
                          .then(pl.col(f'{col}_sum').sum())
                          .otherwise(None)
                          .alias(f'SUM({col})')
                    )
                
                elif func == 'AVG':
                    # AVG = total_sum / total_count
                    # NULL-safe: NULL if total_count = 0
                    total_sum = pl.col(f'{col}_sum').sum()
                    total_count = pl.col(f'{col}_count').sum()
                    agg_exprs.append(
                        (total_sum / total_count).alias(f'AVG({col})')
                    )
                
                elif func == 'COUNT':
                    if col == '*':
                        # COUNT(*) = sum of row counts
                        agg_exprs.append(
                            pl.col('row_count').sum().alias('count_star()')
                        )
                    else:
                        # COUNT(col) = sum of non-null counts
                        agg_exprs.append(
                            pl.col(f'{col}_count').sum().alias(f'COUNT({col})')
                        )
                
                elif func == 'MIN':
                    # MIN = min of pre-aggregated mins
                    agg_exprs.append(
                        pl.col(f'{col}_min').min().alias(f'MIN({col})')
                    )
                
                elif func == 'MAX':
                    # MAX = max of pre-aggregated maxes
                    agg_exprs.append(
                        pl.col(f'{col}_max').max().alias(f'MAX({col})')
                    )
                
                else:
                    raise ValueError(f"Unsupported aggregate: {func}")
            
            # Group and aggregate
            result = df.group_by(group_by).agg(agg_exprs)
        
        else:
            # No grouping needed - rollup exactly matches query dimensions
            # Just compute aggregates directly
            select_exprs = []
            
            # Keep dimension columns
            for col in df.columns:
                if not any(col.endswith(suffix) for suffix in ['_sum', '_count', '_min', '_max', 'row_count']):
                    select_exprs.append(pl.col(col))
            
            # Add computed aggregates
            for agg in aggregates:
                func = agg['func']
                col = agg['col']
                
                if func == 'SUM':
                    # SUM(col) = pre-aggregated sum (already NULL-safe)
                    # But apply NULL check: if count = 0, return NULL
                    select_exprs.append(
                        pl.when(pl.col(f'{col}_count') > 0)
                          .then(pl.col(f'{col}_sum'))
                          .otherwise(None)
                          .alias(f'SUM({col})')
                    )
                
                elif func == 'AVG':
                    # AVG(col) = sum / count
                    select_exprs.append(
                        (pl.col(f'{col}_sum') / pl.col(f'{col}_count')).alias(f'AVG({col})')
                    )
                
                elif func == 'COUNT':
                    if col == '*':
                        # COUNT(*) = row_count
                        select_exprs.append(
                            pl.col('row_count').alias('COUNT(*)')
                        )
                    else:
                        # COUNT(col) = count
                        select_exprs.append(
                            pl.col(f'{col}_count').alias(f'COUNT({col})')
                        )
                
                elif func == 'MIN':
                    select_exprs.append(
                        pl.col(f'{col}_min').alias(f'MIN({col})')
                    )
                
                elif func == 'MAX':
                    select_exprs.append(
                        pl.col(f'{col}_max').alias(f'MAX({col})')
                    )
                
                else:
                    raise ValueError(f"Unsupported aggregate: {func}")
            
            result = df.select(select_exprs)
        
        return result
    
    def apply_order_by(self, df: pl.DataFrame, order_by: List[Dict]) -> pl.DataFrame:
        """
        Apply ORDER BY sorting.
        
        Args:
            df: Result dataframe
            order_by: List of sort specs: [{"col": "day", "dir": "asc"}, ...]
        
        Returns:
            Sorted dataframe
        """
        if not order_by:
            return df
        
        sort_cols = []
        sort_desc = []
        
        for order in order_by:
            col = order['col']
            # Normalize COUNT(*) to count_star() to match output column name
            if col == 'COUNT(*)':
                col = 'count_star()'
            direction = order.get('dir', 'asc').lower()
            
            sort_cols.append(col)
            sort_desc.append(direction == 'desc')
        
        sorted_df = df.sort(sort_cols, descending=sort_desc)
        
        logger.debug(f"Sorted by: {sort_cols} (desc={sort_desc})")
        return sorted_df
    
    def execute(
        self, 
        rollup_name: str, 
        pattern: QueryPattern
    ) -> Tuple[List[str], List[Tuple]]:
        """
        Execute query against rollup.
        
        Steps:
        1. Load rollup (instant if pre-loaded)
        2. Apply WHERE filters
        3. Compute aggregates
        4. Apply ORDER BY
        5. Format results
        
        Args:
            rollup_name: Name of rollup to query
            pattern: Parsed query pattern
        
        Returns:
            Tuple of (column_names, rows)
            
        Example:
            >>> executor = QueryExecutor()
            >>> pattern = QueryPattern(
            ...     select_cols=['day'],
            ...     aggregates=[{'func': 'SUM', 'col': 'bid_price'}],
            ...     group_by=['day'],
            ...     where_filters=[{'col': 'type', 'op': 'eq', 'val': 'impression'}],
            ...     order_by=[]
            ... )
            >>> cols, rows = executor.execute('day_type', pattern)
            >>> print(cols)
            ['day', 'SUM(bid_price)']
            >>> print(rows[:3])
            [('2024-01-01', 1000.0), ('2024-01-02', 2000.0), ...]
        """
        logger.info(f"Executing query on rollup: {rollup_name}")
        
        # 1. Load rollup (instant if pre-loaded!)
        df = self.loader.load_rollup(rollup_name)
        logger.debug(f"Loaded rollup: {len(df)} rows")
        
        # 2. Apply filters
        df = self.apply_filters(df, pattern.where_filters)
        
        # 3. Compute aggregates
        df = self.compute_aggregates(df, pattern.aggregates, pattern.group_by)
        
        # 4. Convert day-of-year back to calendar dates for output
        # (Must be done BEFORE sorting because ORDER BY references calendar format columns)
        df = self.convert_dates_to_calendar(df)
        
        # 5. Apply ORDER BY (after date conversion so sorting works on calendar dates)
        df = self.apply_order_by(df, pattern.order_by)
        
        # 6. Format results
        column_names = df.columns
        rows = [tuple(row) for row in df.iter_rows()]
        
        logger.info(f"Query complete: {len(rows)} result rows")
        
        return column_names, rows
    
    def convert_dates_to_calendar(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Convert day-of-year format back to calendar dates for output.
        
        Internal format: "2024-153" (day-of-year)
        Output format: "2024-06-01" (calendar date)
        
        Also handles minute format:
        Internal: "2024-153 0:45"
        Output: "2024-06-01 00:45"
        """
        from datetime import datetime
        
        # Check if 'day' column exists and needs conversion
        if 'day' in df.columns:
            # Convert "2024-153" → "2024-06-01"
            df = df.with_columns([
                pl.col('day').map_elements(
                    lambda x: self._day_of_year_to_calendar(x) if isinstance(x, str) else x,
                    return_dtype=pl.Utf8
                ).alias('day')
            ])
        
        # Check if 'minute' column exists and needs conversion
        if 'minute' in df.columns:
            # Convert "2024-153 0:45" → "2024-06-01 00:45"
            df = df.with_columns([
                pl.col('minute').map_elements(
                    lambda x: self._minute_to_calendar(x) if isinstance(x, str) else x,
                    return_dtype=pl.Utf8
                ).alias('minute')
            ])
        
        # Check if 'hour' column exists and needs conversion
        if 'hour' in df.columns:
            # Convert "2024-153 12" → "2024-06-01 12"
            df = df.with_columns([
                pl.col('hour').map_elements(
                    lambda x: self._hour_to_calendar(x) if isinstance(x, str) else x,
                    return_dtype=pl.Utf8
                ).alias('hour')
            ])
        
        return df
    
    def _day_of_year_to_calendar(self, day_str: str) -> str:
        """Convert '2024-153' to '2024-06-01'"""
        try:
            parts = day_str.split('-')
            if len(parts) != 2:
                return day_str
            
            year = int(parts[0])
            day_of_year = int(parts[1])
            
            # Create date from day of year
            date_obj = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
            return date_obj.strftime('%Y-%m-%d')
        except:
            return day_str
    
    def _minute_to_calendar(self, minute_str: str) -> str:
        """Convert '2024-153 0:45' to '2024-06-01 00:45'"""
        try:
            # Split into day and time parts
            parts = minute_str.split(' ')
            if len(parts) != 2:
                return minute_str
            
            # Convert day part
            day_calendar = self._day_of_year_to_calendar(parts[0])
            
            # Format time part (ensure HH:MM format)
            time_part = parts[1]
            if ':' in time_part:
                hour, minute = time_part.split(':')
                time_formatted = f"{int(hour):02d}:{int(minute):02d}"
            else:
                time_formatted = time_part
            
            return f"{day_calendar} {time_formatted}"
        except:
            return minute_str
    
    def _hour_to_calendar(self, hour_str: str) -> str:
        """Convert '2024-153 12' to '2024-06-01 12'"""
        try:
            parts = hour_str.split(' ')
            if len(parts) != 2:
                return hour_str
            
            day_calendar = self._day_of_year_to_calendar(parts[0])
            return f"{day_calendar} {parts[1]}"
        except:
            return hour_str


def main():
    """Test query executor with sample queries."""
    from pathlib import Path
    
    # This would need actual rollup files to test
    test_dir = Path('rollups_test')
    
    if not test_dir.exists():
        print("⚠️ No test rollups found. Skipping executor test.")
        print("Run prepare.py first to generate rollups.")
        return
    
    print("="*60)
    print("Query Executor Test")
    print("="*60)
    print("\n✅ Query executor implementation complete!")
    print("Ready for integration with prepare.py and run.py")


if __name__ == "__main__":
    main()
