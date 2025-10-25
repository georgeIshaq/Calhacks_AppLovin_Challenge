#!/usr/bin/env python3
"""
Run Phase: Execute queries against optimized rollup tables

This script:
1. Loads pre-built rollup tables (instant with pre-loading)
2. Parses query JSON
3. Routes queries to optimal rollups
4. Executes queries with filtering and aggregation
5. Outputs results to CSV files

Expected runtime: <1s for all queries
Memory usage: ~2GB (pre-loaded rollups)
"""

import sys
import time
import csv
import json
from pathlib import Path
import argparse
import logging

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.core import RollupLoader, QueryRouter, QueryExecutor
from src.core.fallback_executor import FallbackExecutor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_queries(query_dir: Path):
    """Load all query JSON files from directory."""
    queries = []
    
    # Try loading from baseline/inputs.py format
    try:
        from baseline.inputs import queries as baseline_queries
        return baseline_queries
    except:
        pass
    
    # Try loading individual JSON files
    for query_file in sorted(query_dir.glob('q*.json')):
        with open(query_file) as f:
            query = json.load(f)
            queries.append(query)
    
    return queries


def main():
    parser = argparse.ArgumentParser(
        description="Run phase: Execute queries against rollup tables"
    )
    parser.add_argument(
        '--rollup-dir',
        type=Path,
        default=Path('rollups'),
        help='Directory containing rollup files'
    )
    parser.add_argument(
        '--query-dir',
        type=Path,
        default=Path('queries'),
        help='Directory containing query JSON files'
    )
    parser.add_argument(
        '--out-dir',
        type=Path,
        default=Path('results'),
        help='Directory to write result CSV files'
    )
    
    args = parser.parse_args()
    
    print("="*70)
    print("RUN PHASE: Executing Queries")
    print("="*70)
    print()
    print(f"Rollup directory: {args.rollup_dir}")
    print(f"Query directory:  {args.query_dir}")
    print(f"Output directory: {args.out_dir}")
    print()
    
    # Validate rollup directory
    if not args.rollup_dir.exists():
        logger.error(f"Rollup directory not found: {args.rollup_dir}")
        logger.error("Run prepare.py first to build rollups!")
        sys.exit(1)
    
    rollup_files = list(args.rollup_dir.glob('*.arrow'))
    if not rollup_files:
        logger.error(f"No rollup files found in {args.rollup_dir}")
        logger.error("Run prepare.py first to build rollups!")
        sys.exit(1)
    
    logger.info(f"Found {len(rollup_files)} rollup files")
    
    # Create output directory
    args.out_dir.mkdir(parents=True, exist_ok=True)
    
    # Load queries
    logger.info("Loading queries...")
    queries = load_queries(args.query_dir)
    
    if not queries:
        logger.error(f"No queries found in {args.query_dir}")
        sys.exit(1)
    
    logger.info(f"Loaded {len(queries)} queries")
    
    # Initialize query system
    logger.info("")
    logger.info("="*70)
    logger.info("Initializing Query System")
    logger.info("="*70)
    
    init_start = time.time()
    
    try:
        # Load all rollups (should be pre-loaded from prepare phase)
        loader = RollupLoader(args.rollup_dir, preload_threshold_mb=1000)
        router = QueryRouter()
        executor = QueryExecutor(loader)
        
        # Initialize fallback executor for queries without suitable rollups
        fallback = FallbackExecutor(Path('data'))
    except Exception as e:
        logger.error(f"Failed to initialize query system: {e}", exc_info=True)
        sys.exit(1)
    
    init_time = time.time() - init_start
    logger.info(f"✅ Query system ready in {init_time:.3f}s")
    
    # Execute queries
    logger.info("")
    logger.info("="*70)
    logger.info("Executing Queries")
    logger.info("="*70)
    
    results = []
    total_query_time = 0
    
    for i, query in enumerate(queries, 1):
        logger.info(f"\nQuery {i}:")
        
        query_start = time.perf_counter()
        
        try:
            # Route query
            route_start = time.perf_counter()
            rollup_name, pattern = router.route_query(query)
            route_time = (time.perf_counter() - route_start) * 1000
            
            if rollup_name is None:
                # No suitable rollup - use fallback to raw data
                logger.info(f"  No rollup found - using FALLBACK to raw data")
                
                # Execute with fallback
                exec_start = time.perf_counter()
                cols, rows = fallback.execute_from_raw(pattern)
                exec_time = (time.perf_counter() - exec_start) * 1000
                
                query_time = (time.perf_counter() - query_start) * 1000
                total_query_time += query_time
                
                logger.info(f"  Fallback scan: {exec_time:.3f}ms")
                logger.info(f"  Total: {query_time:.3f}ms")
                logger.info(f"  Result: {len(rows)} rows")
                
                rollup_name = 'FALLBACK_RAW'
            else:
                # Use rollup
                logger.info(f"  Routed to: {rollup_name} ({route_time:.3f}ms)")
                
                # Execute query
                exec_start = time.perf_counter()
                cols, rows = executor.execute(rollup_name, pattern)
                exec_time = (time.perf_counter() - exec_start) * 1000
                
                query_time = (time.perf_counter() - query_start) * 1000
                total_query_time += query_time
                
                logger.info(f"  Execution: {exec_time:.3f}ms")
                logger.info(f"  Total: {query_time:.3f}ms")
                logger.info(f"  Result: {len(rows)} rows")
            
            # Write results to CSV
            out_path = args.out_dir / f"q{i}.csv"
            with open(out_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)
            
            logger.info(f"  ✅ Wrote results to: {out_path}")
            
            results.append({
                'query': i,
                'rollup': rollup_name,
                'rows': len(rows),
                'route_ms': route_time,
                'exec_ms': exec_time,
                'total_ms': query_time,
                'status': 'success'
            })
            
        except Exception as e:
            logger.error(f"  ❌ Query {i} FAILED: {e}")
            logger.error(f"     Query: {query}", exc_info=False)
            
            results.append({
                'query': i,
                'rollup': 'N/A',
                'rows': 0,
                'route_ms': 0,
                'exec_ms': 0,
                'total_ms': 0,
                'status': f'failed: {str(e)[:50]}'
            })
    
    # Summary
    print()
    print("="*70)
    print("QUERY EXECUTION SUMMARY")
    print("="*70)
    print()
    
    for r in results:
        status_icon = "✅" if r['status'] == 'success' else "❌"
        print(f"{status_icon} Q{r['query']}: {r['total_ms']:.3f}ms "
              f"({r['rows']} rows) → {r['rollup']}")
    
    print()
    print(f"Total query time: {total_query_time:.3f}ms ({total_query_time/1000:.3f}s)")
    print(f"Average per query: {total_query_time/len(queries):.3f}ms")
    print()
    
    # Check against budget
    budget_ms = 1000  # 1 second
    if total_query_time < budget_ms:
        speedup = budget_ms / total_query_time
        print(f"✅ UNDER BUDGET! ({speedup:.1f}× faster than 1s target)")
    else:
        print(f"⚠️ OVER BUDGET by {total_query_time - budget_ms:.0f}ms")
    
    # Count successes
    successes = sum(1 for r in results if r['status'] == 'success')
    print(f"\nSuccess rate: {successes}/{len(queries)} queries")
    
    print()
    print(f"Results written to: {args.out_dir}")
    print("="*70)
    
    # Exit with error if any queries failed
    if successes < len(queries):
        sys.exit(1)


if __name__ == "__main__":
    main()
