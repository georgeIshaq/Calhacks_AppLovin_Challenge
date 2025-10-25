"""
Query Parser for AppLovin Challenge
Parses JSON query structure into executable components
"""

import json
from typing import Dict, List, Any, Optional
from datetime import datetime


class QueryParser:
    """Parse and validate JSON queries according to challenge spec"""
    
    VALID_OPS = {'eq', 'neq', 'in', 'between'}
    VALID_AGG_FUNCS = {'SUM', 'COUNT', 'AVG'}
    TIME_DIMENSIONS = {'day', 'week', 'hour', 'minute'}
    
    def __init__(self, query: Dict[str, Any]):
        """Initialize parser with query dict"""
        self.query = query
        self.validate()
    
    def validate(self):
        """Validate query structure"""
        if 'select' not in self.query:
            raise ValueError("Query must contain 'select' clause")
        if 'from' not in self.query:
            raise ValueError("Query must contain 'from' clause")
        if self.query['from'] != 'events':
            raise ValueError("Only 'events' table is supported")
    
    @classmethod
    def from_file(cls, filepath: str) -> 'QueryParser':
        """Load query from JSON file"""
        with open(filepath, 'r') as f:
            query = json.load(f)
        return cls(query)
    
    @classmethod
    def from_string(cls, json_str: str) -> 'QueryParser':
        """Load query from JSON string"""
        query = json.loads(json_str)
        return cls(query)
    
    def get_select_columns(self) -> List[str]:
        """Get list of non-aggregate columns from SELECT"""
        columns = []
        for item in self.query['select']:
            if isinstance(item, str):
                columns.append(item)
        return columns
    
    def get_aggregates(self) -> List[Dict[str, str]]:
        """Get list of aggregate functions from SELECT"""
        aggregates = []
        for item in self.query['select']:
            if isinstance(item, dict):
                aggregates.append(item)
        return aggregates
    
    def get_where_conditions(self) -> List[Dict[str, Any]]:
        """Get WHERE clause conditions"""
        return self.query.get('where', [])
    
    def get_group_by(self) -> List[str]:
        """Get GROUP BY columns"""
        return self.query.get('group_by', [])
    
    def get_order_by(self) -> List[Dict[str, str]]:
        """Get ORDER BY specifications"""
        return self.query.get('order_by', [])
    
    def to_sql(self) -> str:
        """Convert query to SQL string (for DuckDB baseline)"""
        sql_parts = []
        
        # SELECT clause
        select_items = []
        for item in self.query['select']:
            if isinstance(item, str):
                # Handle time dimensions
                if item in self.TIME_DIMENSIONS:
                    select_items.append(self._time_dimension_to_sql(item))
                else:
                    select_items.append(item)
            elif isinstance(item, dict):
                # Aggregate function
                func = list(item.keys())[0]
                col = item[func]
                select_items.append(f"{func}({col})")
        
        sql_parts.append(f"SELECT {', '.join(select_items)}")
        
        # FROM clause
        sql_parts.append(f"FROM {self.query['from']}")
        
        # WHERE clause
        where_conditions = self.get_where_conditions()
        if where_conditions:
            where_clauses = []
            for cond in where_conditions:
                where_clauses.append(self._condition_to_sql(cond))
            sql_parts.append(f"WHERE {' AND '.join(where_clauses)}")
        
        # GROUP BY clause
        group_by = self.get_group_by()
        if group_by:
            group_items = []
            for col in group_by:
                if col in self.TIME_DIMENSIONS:
                    group_items.append(self._time_dimension_to_sql(col))
                else:
                    group_items.append(col)
            sql_parts.append(f"GROUP BY {', '.join(group_items)}")
        
        # ORDER BY clause
        order_by = self.get_order_by()
        if order_by:
            order_items = []
            for spec in order_by:
                col = spec['col']
                direction = spec['dir'].upper()
                order_items.append(f"{col} {direction}")
            sql_parts.append(f"ORDER BY {', '.join(order_items)}")
        
        return ' '.join(sql_parts)
    
    def _time_dimension_to_sql(self, dimension: str) -> str:
        """Convert time dimension to SQL expression"""
        timestamp_col = "ts"
        
        if dimension == 'day':
            return f"DATE_TRUNC('day', to_timestamp({timestamp_col}/1000)) AS day"
        elif dimension == 'week':
            return f"DATE_TRUNC('week', to_timestamp({timestamp_col}/1000)) AS week"
        elif dimension == 'hour':
            return f"DATE_TRUNC('hour', to_timestamp({timestamp_col}/1000)) AS hour"
        elif dimension == 'minute':
            return f"DATE_TRUNC('minute', to_timestamp({timestamp_col}/1000)) AS minute"
        else:
            return dimension
    
    def _condition_to_sql(self, condition: Dict[str, Any]) -> str:
        """Convert WHERE condition to SQL"""
        col = condition['col']
        op = condition['op']
        val = condition['val']
        
        # Handle time dimension columns
        if col in self.TIME_DIMENSIONS:
            col = self._time_dimension_to_sql(col).split(' AS ')[0]
        
        if op == 'eq':
            if isinstance(val, str):
                return f"{col} = '{val}'"
            else:
                return f"{col} = {val}"
        elif op == 'neq':
            if isinstance(val, str):
                return f"{col} != '{val}'"
            else:
                return f"{col} != {val}"
        elif op == 'in':
            if isinstance(val[0], str):
                vals = ', '.join([f"'{v}'" for v in val])
            else:
                vals = ', '.join([str(v) for v in val])
            return f"{col} IN ({vals})"
        elif op == 'between':
            if isinstance(val[0], str):
                return f"{col} BETWEEN '{val[0]}' AND '{val[1]}'"
            else:
                return f"{col} BETWEEN {val[0]} AND {val[1]}"
        else:
            raise ValueError(f"Unsupported operator: {op}")
    
    def __str__(self) -> str:
        """String representation"""
        return json.dumps(self.query, indent=2)
