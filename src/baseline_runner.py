"""
Baseline DuckDB Runner for AppLovin Challenge
Runs queries against DuckDB and measures performance
"""

import os
import sys
import time
import argparse
import json
from pathlib import Path
from typing import List, Dict, Any
import duckdb
import pandas as pd

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent))
from query_parser import QueryParser


class BaselineRunner:
    """Run baseline DuckDB implementation"""
    
    def __init__(self, data_dir: str = 'data', db_file: str = None):
        """Initialize runner
        
        Args:
            data_dir: Directory containing CSV files
            db_file: Optional persistent database file path
        """
        self.data_dir = Path(data_dir)
        self.db_file = db_file
        self.conn = None
        
    def prepare(self):
        """Prepare phase: Load data into DuckDB"""
        print("=" * 80)
        print("PREPARE PHASE: Loading data into DuckDB")
        print("=" * 80)
        
        start_time = time.time()
        
        # Create connection
        if self.db_file:
            self.conn = duckdb.connect(self.db_file)
            print(f"Connected to persistent database: {self.db_file}")
        else:
            self.conn = duckdb.connect()
            print("Created in-memory database")
        
        # Find CSV files
        csv_files = list(self.data_dir.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {self.data_dir}")
        
        print(f"\nFound {len(csv_files)} CSV file(s)")
        
        # Load data
        for csv_file in csv_files:
            print(f"Loading {csv_file.name}...")
            file_start = time.time()
            
            # Create or replace table from CSV
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE events AS 
                SELECT * FROM read_csv_auto('{csv_file}')
            """)
            
            file_time = time.time() - file_start
            print(f"  Loaded in {file_time:.2f} seconds")
        
        # Get row count
        result = self.conn.execute("SELECT COUNT(*) FROM events").fetchone()
        row_count = result[0]
        
        load_time = time.time() - start_time
        print(f"\n✓ Data loaded: {row_count:,} rows in {load_time:.2f} seconds")
        print(f"  ({row_count / load_time:,.0f} rows/sec)")
        print("=" * 80)
        
        return load_time
    
    def run_query(self, query_dict: Dict[str, Any], query_name: str = "Query") -> tuple:
        """Run a single query and return results and timing
        
        Args:
            query_dict: Query as dictionary
            query_name: Name/description of query
            
        Returns:
            (result_df, execution_time)
        """
        parser = QueryParser(query_dict)
        sql = parser.to_sql()
        
        print(f"\n{query_name}:")
        print(f"  SQL: {sql}")
        
        start_time = time.time()
        result_df = self.conn.execute(sql).df()
        execution_time = time.time() - start_time
        
        print(f"  ✓ Completed in {execution_time:.4f} seconds")
        print(f"  Result: {len(result_df)} rows")
        
        return result_df, execution_time
    
    def run_queries_from_dir(self, queries_dir: str = 'queries', 
                            output_dir: str = 'results') -> List[Dict[str, Any]]:
        """Run all queries from a directory
        
        Args:
            queries_dir: Directory containing JSON query files
            output_dir: Directory to save results
            
        Returns:
            List of result dictionaries with timing info
        """
        print("\n" + "=" * 80)
        print("RUN PHASE: Executing queries")
        print("=" * 80)
        
        queries_path = Path(queries_dir)
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Find query files
        query_files = sorted(queries_path.glob("*.json"))
        if not query_files:
            print(f"No query files found in {queries_dir}")
            return []
        
        print(f"\nFound {len(query_files)} query file(s)\n")
        
        results = []
        total_time = 0
        
        for i, query_file in enumerate(query_files, 1):
            try:
                # Load query
                with open(query_file, 'r') as f:
                    query_dict = json.load(f)
                
                # Run query
                result_df, exec_time = self.run_query(
                    query_dict, 
                    query_name=f"Query {i}: {query_file.stem}"
                )
                
                # Save result
                output_file = output_path / f"{query_file.stem}_result.csv"
                result_df.to_csv(output_file, index=False)
                print(f"  Saved to: {output_file}")
                
                results.append({
                    'query_file': query_file.name,
                    'execution_time': exec_time,
                    'rows': len(result_df),
                    'success': True
                })
                
                total_time += exec_time
                
            except Exception as e:
                print(f"  ✗ Error: {e}")
                results.append({
                    'query_file': query_file.name,
                    'execution_time': None,
                    'rows': None,
                    'success': False,
                    'error': str(e)
                })
        
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total queries: {len(results)}")
        print(f"Successful: {sum(1 for r in results if r['success'])}")
        print(f"Failed: {sum(1 for r in results if not r['success'])}")
        print(f"Total execution time: {total_time:.4f} seconds")
        if len(results) > 0:
            print(f"Average query time: {total_time / len(results):.4f} seconds")
        print("=" * 80)
        
        return results
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Run baseline DuckDB queries')
    parser.add_argument('--data-dir', default='data',
                       help='Directory containing CSV files (default: data)')
    parser.add_argument('--queries-dir', default='queries',
                       help='Directory containing query JSON files (default: queries)')
    parser.add_argument('--output-dir', default='results/baseline',
                       help='Directory to save results (default: results/baseline)')
    parser.add_argument('--db-file', default=None,
                       help='Persistent database file (default: in-memory)')
    parser.add_argument('--prepare-only', action='store_true',
                       help='Only run prepare phase')
    
    args = parser.parse_args()
    
    # Check if data directory exists
    if not Path(args.data_dir).exists():
        print(f"Error: Data directory '{args.data_dir}' does not exist!")
        print("\nPlease download and extract data files first:")
        print("  1. Download data.zip or data-lite.zip from Google Drive")
        print("  2. Extract: unzip data.zip -d data/")
        return 1
    
    # Create runner
    runner = BaselineRunner(
        data_dir=args.data_dir,
        db_file=args.db_file
    )
    
    try:
        # Prepare phase
        prepare_time = runner.prepare()
        
        if not args.prepare_only:
            # Run phase
            results = runner.run_queries_from_dir(
                queries_dir=args.queries_dir,
                output_dir=args.output_dir
            )
            
            # Save timing results
            output_path = Path(args.output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            with open(output_path / 'timing_results.json', 'w') as f:
                json.dump({
                    'prepare_time': prepare_time,
                    'query_results': results
                }, f, indent=2)
        
    finally:
        runner.close()
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
