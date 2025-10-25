"""
Data Distribution & Query Selectivity Analyzer

Analyzes actual data distribution to estimate:
1. Row counts per type partition
2. Time range distribution (daily/monthly patterns)
3. Actual selectivity of WHERE predicates
4. Estimated pre-aggregation sizes

This helps us accurately project storage sizes and query performance.
"""

import csv
import sys
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, List
import json


def analyze_full_dataset(data_dir: Path, sample_ratio: float = 0.1):
    """Analyze the full dataset (or a sample of it)"""
    
    print(f"\nAnalyzing data from {data_dir}...")
    
    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {data_dir}")
        return None
    
    print(f"Found {len(csv_files)} CSV files")
    
    # Sample files if needed
    if sample_ratio < 1.0:
        import random
        sample_count = max(1, int(len(csv_files) * sample_ratio))
        csv_files = random.sample(csv_files, sample_count)
        print(f"Sampling {len(csv_files)} files ({sample_ratio*100:.0f}%)")
    
    # Statistics to collect
    total_rows = 0
    type_counts = Counter()
    country_counts = Counter()
    day_counts = Counter()
    hour_counts = Counter()
    
    # Combinations for pre-agg estimation
    day_type_counts = Counter()
    day_advertiser_type_counts = Counter()
    day_publisher_type_counts = Counter()
    advertiser_type_counts = Counter()
    country_type_counts = Counter()
    
    # NULL tracking
    null_counts = defaultdict(int)
    
    print("\nProcessing files...")
    for i, csv_file in enumerate(csv_files, 1):
        print(f"  [{i}/{len(csv_files)}] {csv_file.name}...", end='', flush=True)
        
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            
            file_rows = 0
            for row in reader:
                total_rows += 1
                file_rows += 1
                
                # Extract fields
                ts = row.get('ts', '')
                event_type = row.get('type', '')
                country = row.get('country', '')
                advertiser_id = row.get('advertiser_id', '')
                publisher_id = row.get('publisher_id', '')
                bid_price = row.get('bid_price', '')
                total_price = row.get('total_price', '')
                
                # Convert timestamp to day/hour
                if ts:
                    try:
                        dt = datetime.fromtimestamp(int(ts) / 1000)
                        day = dt.strftime('%Y-%m-%d')
                        hour = dt.strftime('%Y-%m-%d %H:00')
                        
                        day_counts[day] += 1
                        hour_counts[hour] += 1
                        
                        # Combinations
                        day_type_counts[(day, event_type)] += 1
                        if advertiser_id:
                            day_advertiser_type_counts[(day, advertiser_id, event_type)] += 1
                        if publisher_id:
                            day_publisher_type_counts[(day, publisher_id, event_type)] += 1
                    except:
                        pass
                
                # Simple counts
                if event_type:
                    type_counts[event_type] += 1
                if country:
                    country_counts[country] += 1
                if advertiser_id and event_type:
                    advertiser_type_counts[(advertiser_id, event_type)] += 1
                if country and event_type:
                    country_type_counts[(country, event_type)] += 1
                
                # Track NULLs
                for col, val in row.items():
                    if val == '' or val is None:
                        null_counts[col] += 1
        
        print(f" {file_rows:,} rows")
    
    # Extrapolate if sampling
    if sample_ratio < 1.0:
        scale_factor = 1.0 / sample_ratio
        total_rows = int(total_rows * scale_factor)
        print(f"\n📊 Extrapolated total rows: ~{total_rows:,}")
    else:
        print(f"\n📊 Total rows: {total_rows:,}")
    
    return {
        'total_rows': total_rows,
        'type_counts': type_counts,
        'country_counts': country_counts,
        'day_counts': day_counts,
        'hour_counts': hour_counts,
        'day_type_counts': day_type_counts,
        'day_advertiser_type_counts': day_advertiser_type_counts,
        'day_publisher_type_counts': day_publisher_type_counts,
        'advertiser_type_counts': advertiser_type_counts,
        'country_type_counts': country_type_counts,
        'null_counts': null_counts,
        'sampled': sample_ratio < 1.0,
        'sample_ratio': sample_ratio
    }


def estimate_query_performance(stats: Dict, queries_dir: Path):
    """Estimate query performance based on data distribution"""
    
    print(f"\n{'='*80}")
    print("QUERY PERFORMANCE ESTIMATES")
    print("=" * 80)
    
    query_files = sorted(queries_dir.glob("*.json"))
    if not query_files:
        print("No query files found")
        return
    
    for qf in query_files:
        with open(qf, 'r') as f:
            query = json.load(f)
        
        print(f"\n📊 Query: {qf.stem}")
        print(f"   {json.dumps(query, indent=4)}")
        
        # Estimate rows scanned
        total_rows = stats['total_rows']
        estimated_rows = total_rows
        
        where_conditions = query.get('where', [])
        for cond in where_conditions:
            col = cond['col']
            op = cond['op']
            val = cond['val']
            
            if col == 'type' and op == 'eq':
                count = stats['type_counts'].get(val, 0)
                if stats['sampled']:
                    count = int(count / stats['sample_ratio'])
                selectivity = count / total_rows if total_rows > 0 else 0
                estimated_rows = int(estimated_rows * selectivity)
                print(f"   └─ Filter: type={val} → ~{count:,} rows ({selectivity*100:.1f}%)")
            
            elif col == 'country' and op == 'eq':
                count = stats['country_counts'].get(val, 0)
                if stats['sampled']:
                    count = int(count / stats['sample_ratio'])
                selectivity = count / total_rows if total_rows > 0 else 0
                estimated_rows = int(estimated_rows * selectivity)
                print(f"   └─ Filter: country={val} → ~{count:,} rows ({selectivity*100:.1f}%)")
            
            elif col == 'day' and op == 'between':
                # Estimate based on date range
                days_in_range = 0
                start_day, end_day = val
                for day in stats['day_counts']:
                    if start_day <= day <= end_day:
                        days_in_range += stats['day_counts'][day]
                if stats['sampled']:
                    days_in_range = int(days_in_range / stats['sample_ratio'])
                selectivity = days_in_range / total_rows if total_rows > 0 else 0
                estimated_rows = int(estimated_rows * selectivity)
                print(f"   └─ Filter: day BETWEEN {start_day} AND {end_day} → ~{days_in_range:,} rows ({selectivity*100:.1f}%)")
        
        print(f"   📍 Estimated rows to scan: {estimated_rows:,}")
        
        # Estimate result size
        group_by = query.get('group_by', [])
        if group_by:
            print(f"   📍 GROUP BY: {group_by}")
            
            # Rough cardinality estimate
            if tuple(sorted(group_by)) == ('day',):
                result_rows = len(stats['day_counts'])
            elif tuple(sorted(group_by)) == ('country',):
                result_rows = len(stats['country_counts'])
            elif tuple(sorted(group_by)) == ('advertiser_id', 'type'):
                result_rows = len(stats['advertiser_type_counts'])
            else:
                result_rows = estimated_rows // 100  # Rough guess
            
            print(f"   📍 Estimated result rows: ~{result_rows:,}")


def print_distribution_summary(stats: Dict):
    """Print summary of data distribution"""
    
    print(f"\n{'='*80}")
    print("DATA DISTRIBUTION SUMMARY")
    print("=" * 80)
    
    total = stats['total_rows']
    
    print(f"\n📊 Type Distribution:")
    for event_type, count in stats['type_counts'].most_common():
        pct = (count / total * 100) if total > 0 else 0
        if stats['sampled']:
            count = int(count / stats['sample_ratio'])
        print(f"   {event_type:<15s}: ~{count:>12,} rows ({pct:>5.1f}%)")
    
    print(f"\n📊 Country Distribution (top 10):")
    for country, count in stats['country_counts'].most_common(10):
        pct = (count / total * 100) if total > 0 else 0
        if stats['sampled']:
            count = int(count / stats['sample_ratio'])
        print(f"   {country:<15s}: ~{count:>12,} rows ({pct:>5.1f}%)")
    
    print(f"\n📊 Time Range:")
    if stats['day_counts']:
        days = sorted(stats['day_counts'].keys())
        print(f"   First day: {days[0]}")
        print(f"   Last day:  {days[-1]}")
        print(f"   Total days: {len(days)}")
        
        avg_per_day = total / len(days) if len(days) > 0 else 0
        print(f"   Avg rows/day: ~{avg_per_day:,.0f}")
    
    print(f"\n📊 Pre-Aggregation Size Estimates:")
    print(f"   (day, type):                  ~{len(stats['day_type_counts']):,} rows")
    print(f"   (day, advertiser_id, type):   ~{len(stats['day_advertiser_type_counts']):,} rows")
    print(f"   (day, publisher_id, type):    ~{len(stats['day_publisher_type_counts']):,} rows")
    print(f"   (advertiser_id, type):        ~{len(stats['advertiser_type_counts']):,} rows")
    print(f"   (country, type):              ~{len(stats['country_type_counts']):,} rows")
    
    print(f"\n📊 NULL Percentages:")
    for col, count in sorted(stats['null_counts'].items()):
        pct = (count / total * 100) if total > 0 else 0
        if pct > 0:
            print(f"   {col:<20s}: {pct:>5.1f}% NULL")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze data distribution')
    parser.add_argument('--data-dir', default='data',
                       help='Directory containing CSV files')
    parser.add_argument('--queries-dir', default='queries',
                       help='Directory containing query JSON files')
    parser.add_argument('--sample-ratio', type=float, default=0.2,
                       help='Ratio of files to sample (0.0-1.0, default 0.2 = 20%%)')
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"Error: Data directory not found: {args.data_dir}")
        return 1
    
    queries_dir = Path(args.queries_dir)
    
    print("=" * 80)
    print("DATA DISTRIBUTION & SELECTIVITY ANALYZER")
    print("=" * 80)
    
    # Analyze data
    stats = analyze_full_dataset(data_dir, sample_ratio=args.sample_ratio)
    
    if stats:
        # Print summary
        print_distribution_summary(stats)
        
        # Estimate query performance
        if queries_dir.exists():
            estimate_query_performance(stats, queries_dir)
    
    print(f"\n{'='*80}")
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
