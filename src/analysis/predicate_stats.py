"""
Predicate Frequency & Selectivity Analyzer

Analyzes query patterns to determine:
1. Which columns appear most frequently in WHERE clauses
2. Cardinality of each column (from sample data)
3. Selectivity of common predicates
4. GROUP BY patterns

This informs decisions about:
- Which columns to partition on
- Which columns need bitmap indexes
- Which pre-aggregations to build
"""

import json
import csv
import sys
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, List, Any


def analyze_queries(query_files: List[Path]) -> Dict[str, Any]:
    """Analyze query patterns from JSON files"""
    
    where_columns = Counter()
    groupby_columns = Counter()
    select_columns = Counter()
    aggregate_columns = Counter()
    orderby_columns = Counter()
    
    where_operators = Counter()
    groupby_patterns = []
    time_dimensions_used = Counter()
    
    queries = []
    
    for qf in query_files:
        with open(qf, 'r') as f:
            query = json.load(f)
            queries.append(query)
            
            # Analyze WHERE
            for cond in query.get('where', []):
                col = cond['col']
                op = cond['op']
                where_columns[col] += 1
                where_operators[op] += 1
                
                # Track time dimensions
                if col in ['day', 'week', 'hour', 'minute']:
                    time_dimensions_used[col] += 1
            
            # Analyze GROUP BY
            groupby = query.get('group_by', [])
            if groupby:
                groupby_patterns.append(tuple(sorted(groupby)))
                for col in groupby:
                    groupby_columns[col] += 1
                    if col in ['day', 'week', 'hour', 'minute']:
                        time_dimensions_used[col] += 1
            
            # Analyze SELECT
            for item in query.get('select', []):
                if isinstance(item, str):
                    select_columns[item] += 1
                    if item in ['day', 'week', 'hour', 'minute']:
                        time_dimensions_used[item] += 1
                elif isinstance(item, dict):
                    func = list(item.keys())[0]
                    col = item[func]
                    aggregate_columns[f"{func}({col})"] += 1
            
            # Analyze ORDER BY
            for spec in query.get('order_by', []):
                orderby_columns[spec['col']] += 1
    
    return {
        'total_queries': len(queries),
        'where_columns': where_columns,
        'groupby_columns': groupby_columns,
        'select_columns': select_columns,
        'aggregate_columns': aggregate_columns,
        'orderby_columns': orderby_columns,
        'where_operators': where_operators,
        'groupby_patterns': Counter(groupby_patterns),
        'time_dimensions': time_dimensions_used,
        'queries': queries
    }


def analyze_data_sample(csv_file: Path, sample_size: int = 1000000) -> Dict[str, Any]:
    """Analyze data characteristics from CSV sample"""
    
    print(f"\nAnalyzing sample data from {csv_file.name}...")
    
    column_values = defaultdict(set)
    null_counts = defaultdict(int)
    total_rows = 0
    type_distribution = Counter()
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        
        for i, row in enumerate(reader):
            if i >= sample_size:
                break
            
            total_rows += 1
            
            for col, val in row.items():
                if val == '' or val is None:
                    null_counts[col] += 1
                else:
                    column_values[col].add(val)
                    
                    if col == 'type':
                        type_distribution[val] += 1
    
    # Calculate cardinalities and null percentages
    cardinalities = {col: len(vals) for col, vals in column_values.items()}
    null_percentages = {col: (count / total_rows * 100) for col, count in null_counts.items()}
    
    return {
        'total_rows_sampled': total_rows,
        'cardinalities': cardinalities,
        'null_percentages': null_percentages,
        'type_distribution': type_distribution,
        'column_values': {col: list(vals)[:20] for col, vals in column_values.items()}  # First 20 values
    }


def estimate_selectivity(query_analysis: Dict, data_analysis: Dict) -> Dict[str, Any]:
    """Estimate selectivity of common WHERE predicates"""
    
    selectivity_estimates = {}
    
    # Type selectivity (assuming ~4 types evenly distributed)
    type_count = data_analysis['cardinalities'].get('type', 4)
    selectivity_estimates['type_eq'] = 1.0 / type_count
    
    # Country selectivity
    country_count = data_analysis['cardinalities'].get('country', 10)
    selectivity_estimates['country_eq'] = 1.0 / country_count
    
    # Day between (assuming typical range is 3-7 days out of 366)
    selectivity_estimates['day_between_typical'] = 5.0 / 366.0
    
    # Combined predicates (type + country + day)
    selectivity_estimates['type_country_day'] = (
        selectivity_estimates['type_eq'] * 
        selectivity_estimates['country_eq'] * 
        selectivity_estimates['day_between_typical']
    )
    
    return selectivity_estimates


def recommend_optimizations(query_analysis: Dict, data_analysis: Dict, 
                           selectivity: Dict) -> List[str]:
    """Generate optimization recommendations based on analysis"""
    
    recommendations = []
    total_queries = query_analysis['total_queries']
    
    # Type partitioning recommendation
    type_freq = query_analysis['where_columns'].get('type', 0)
    type_pct = (type_freq / total_queries * 100) if total_queries > 0 else 0
    
    if type_pct >= 60:
        recommendations.append(
            f"⭐⭐⭐ HIGH PRIORITY: Partition by 'type' (appears in {type_pct:.0f}% of queries, "
            f"{data_analysis['cardinalities'].get('type', 4)} distinct values)"
        )
    elif type_pct >= 30:
        recommendations.append(
            f"⭐⭐ MEDIUM PRIORITY: Consider partitioning by 'type' ({type_pct:.0f}% frequency)"
        )
    
    # Time dimension recommendations
    for time_dim in ['day', 'week', 'hour', 'minute']:
        freq = query_analysis['time_dimensions'].get(time_dim, 0)
        if freq > 0:
            pct = (freq / total_queries * 100)
            if pct >= 60:
                recommendations.append(
                    f"⭐⭐⭐ HIGH PRIORITY: Optimize '{time_dim}' dimension "
                    f"(appears in {pct:.0f}% of queries) - use sorted layout + zone maps"
                )
    
    # Bitmap index recommendations
    for col in ['type', 'country', 'publisher_id', 'advertiser_id']:
        cardinality = data_analysis['cardinalities'].get(col, float('inf'))
        where_freq = query_analysis['where_columns'].get(col, 0)
        
        if cardinality < 1000 and where_freq > 0:
            recommendations.append(
                f"⭐⭐ RECOMMENDED: Bitmap index on '{col}' "
                f"(cardinality={cardinality}, used in {where_freq} queries)"
            )
    
    # Pre-aggregation recommendations
    common_patterns = query_analysis['groupby_patterns'].most_common(5)
    if common_patterns:
        recommendations.append("\n⭐⭐⭐ HIGH PRIORITY PRE-AGGREGATIONS:")
        for pattern, count in common_patterns:
            recommendations.append(f"  - {pattern} (appears {count} times)")
    
    # Compression recommendations
    for col, null_pct in data_analysis['null_percentages'].items():
        if null_pct > 50:
            recommendations.append(
                f"⚠️  Column '{col}' is {null_pct:.1f}% NULL - use sparse encoding"
            )
    
    return recommendations


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze query patterns and data characteristics')
    parser.add_argument('--queries-dir', default='queries',
                       help='Directory containing JSON query files')
    parser.add_argument('--data-sample', default='data/data/events_part_00000.csv',
                       help='CSV file to sample for data analysis')
    parser.add_argument('--sample-size', type=int, default=1000000,
                       help='Number of rows to sample from data file')
    
    args = parser.parse_args()
    
    # Find query files
    queries_path = Path(args.queries_dir)
    if not queries_path.exists():
        print(f"Error: Query directory '{args.queries_dir}' not found")
        print("Please create some example queries first")
        return 1
    
    query_files = sorted(queries_path.glob("*.json"))
    if not query_files:
        print(f"No query files found in {args.queries_dir}")
        print("Please add example queries (q1.json, q2.json, etc.)")
        return 1
    
    print("=" * 80)
    print("PREDICATE FREQUENCY & SELECTIVITY ANALYSIS")
    print("=" * 80)
    
    # Analyze queries
    print(f"\nAnalyzing {len(query_files)} queries...")
    query_analysis = analyze_queries(query_files)
    
    print("\n" + "=" * 80)
    print("QUERY PATTERN ANALYSIS")
    print("=" * 80)
    
    print(f"\nTotal queries analyzed: {query_analysis['total_queries']}")
    
    print("\n📊 WHERE Clause Column Frequency:")
    for col, count in query_analysis['where_columns'].most_common():
        pct = (count / query_analysis['total_queries'] * 100)
        print(f"  {col:20s}: {count:2d} queries ({pct:5.1f}%)")
    
    print("\n📊 WHERE Operators Used:")
    for op, count in query_analysis['where_operators'].most_common():
        print(f"  {op:10s}: {count} times")
    
    print("\n📊 GROUP BY Column Frequency:")
    for col, count in query_analysis['groupby_columns'].most_common():
        pct = (count / query_analysis['total_queries'] * 100)
        print(f"  {col:20s}: {count:2d} queries ({pct:5.1f}%)")
    
    print("\n📊 Common GROUP BY Patterns:")
    for pattern, count in query_analysis['groupby_patterns'].most_common(5):
        print(f"  {pattern}: {count} times")
    
    print("\n📊 Time Dimensions Used:")
    for dim, count in query_analysis['time_dimensions'].most_common():
        pct = (count / query_analysis['total_queries'] * 100)
        print(f"  {dim:10s}: {count:2d} queries ({pct:5.1f}%)")
    
    print("\n📊 Aggregate Functions:")
    for agg, count in query_analysis['aggregate_columns'].most_common():
        print(f"  {agg}: {count} times")
    
    # Analyze data sample
    data_file = Path(args.data_sample)
    if data_file.exists():
        data_analysis = analyze_data_sample(data_file, args.sample_size)
        
        print("\n" + "=" * 80)
        print("DATA CHARACTERISTICS (from sample)")
        print("=" * 80)
        
        print(f"\nRows sampled: {data_analysis['total_rows_sampled']:,}")
        
        print("\n📊 Column Cardinalities:")
        for col, card in sorted(data_analysis['cardinalities'].items()):
            print(f"  {col:20s}: {card:,} distinct values")
        
        print("\n📊 NULL Percentages:")
        for col, pct in sorted(data_analysis['null_percentages'].items()):
            if pct > 0:
                print(f"  {col:20s}: {pct:5.1f}% NULL")
        
        print("\n📊 Type Distribution (sample):")
        total = sum(data_analysis['type_distribution'].values())
        for type_val, count in data_analysis['type_distribution'].most_common():
            pct = (count / total * 100) if total > 0 else 0
            print(f"  {type_val:15s}: {count:,} ({pct:5.1f}%)")
        
        # Selectivity estimates
        selectivity = estimate_selectivity(query_analysis, data_analysis)
        
        print("\n📊 Estimated Selectivity:")
        for pred, sel in selectivity.items():
            rows_est = int(data_analysis['total_rows_sampled'] * sel)
            print(f"  {pred:30s}: {sel:8.6f} (~{rows_est:,} rows)")
        
        # Generate recommendations
        print("\n" + "=" * 80)
        print("OPTIMIZATION RECOMMENDATIONS")
        print("=" * 80)
        
        recommendations = recommend_optimizations(query_analysis, data_analysis, selectivity)
        for rec in recommendations:
            print(rec)
    
    else:
        print(f"\n⚠️  Data file not found: {args.data_sample}")
        print("Skipping data analysis")
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
