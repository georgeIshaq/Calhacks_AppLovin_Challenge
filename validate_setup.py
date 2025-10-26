#!/usr/bin/env python3
"""
Quick validation script for judges to verify setup is correct.

This script:
1. Checks if rollups exist
2. Checks if fallback.duckdb exists
3. Runs a simple test query
4. Validates results

Usage:
    python3 validate_setup.py
"""

import sys
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)


def check_rollups(rollup_dir: Path):
    """Check if rollup files exist."""
    if not rollup_dir.exists():
        logger.error(f"❌ Rollup directory not found: {rollup_dir}")
        logger.error("   Run: python3 prepare.py --data-dir ./data")
        return False
    
    rollup_files = list(rollup_dir.glob('*.arrow'))
    if not rollup_files:
        logger.error(f"❌ No rollup files found in {rollup_dir}")
        logger.error("   Run: python3 prepare.py --data-dir ./data")
        return False
    
    logger.info(f"✅ Found {len(rollup_files)} rollup files")
    expected_rollups = [
        'day_type.arrow',
        'hour_type.arrow',
        'minute_type.arrow',
        'week_type.arrow',
        'country_type.arrow',
        'advertiser_type.arrow',
        'publisher_type.arrow',
        'day_country_type.arrow',
        'day_advertiser_type.arrow',
        'hour_country_type.arrow',
        'day_publisher_country_type.arrow'
    ]
    
    missing = []
    for rollup in expected_rollups:
        if not (rollup_dir / rollup).exists():
            missing.append(rollup)
    
    if missing:
        logger.warning(f"⚠️  Missing expected rollups: {', '.join(missing)}")
        return False
    
    logger.info("✅ All 11 expected rollups found")
    return True


def check_fallback(fallback_path: Path):
    """Check if DuckDB fallback exists."""
    if not fallback_path.exists():
        logger.error(f"❌ DuckDB fallback not found: {fallback_path}")
        logger.error("   Run: python3 prepare.py --data-dir ./data")
        return False
    
    size_mb = fallback_path.stat().st_size / (1024 * 1024)
    logger.info(f"✅ DuckDB fallback found ({size_mb:.1f} MB)")
    return True


def check_queries():
    """Check if query files exist."""
    query_sources = [
        Path('baseline/inputs.py'),
        Path('queries/inputs.py'),
        Path('queries/example_queries.json')
    ]
    
    found = []
    for source in query_sources:
        if source.exists():
            found.append(str(source))
    
    if not found:
        logger.warning("⚠️  No query files found")
        logger.warning("   Expected at least one of: baseline/inputs.py, queries/inputs.py, or queries/*.json")
        return False
    
    logger.info(f"✅ Query files found: {', '.join(found)}")
    return True


def test_query_execution():
    """Run a simple test query to validate the system."""
    logger.info("\n" + "="*70)
    logger.info("Testing Query Execution")
    logger.info("="*70)
    
    try:
        # Import after checks to avoid import errors
        sys.path.insert(0, str(Path(__file__).parent))
        from src.core import RollupLoader, QueryRouter
        
        # Simple test query
        test_query = {
            "select": ["day", {"COUNT": "*"}],
            "from": "events",
            "where": [{"col": "type", "op": "eq", "val": "impression"}],
            "group_by": ["day"],
        }
        
        logger.info("Loading rollups...")
        loader = RollupLoader(Path('rollups'), preload_threshold_mb=1000)
        
        logger.info("Initializing query router...")
        router = QueryRouter()
        
        logger.info("Routing test query...")
        result = router.route_query(test_query)
        
        if not result or not result[0]:
            logger.error("❌ Query routing failed")
            return False
        
        rollup_name, pattern = result
        logger.info(f"✅ Query routed to: {rollup_name}")
        
        logger.info("Loading rollup...")
        df = loader.load_rollup(rollup_name)
        
        if df is None or len(df) == 0:
            logger.error("❌ Rollup is empty or failed to load")
            return False
        
        logger.info(f"✅ Rollup loaded successfully: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"   Columns: {', '.join(df.columns)}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Test query failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    logger.info("="*70)
    logger.info("CalHacks AppLovin Challenge - Setup Validation")
    logger.info("="*70)
    logger.info("")
    
    checks = []
    
    # Check 1: Rollups
    logger.info("1. Checking rollup files...")
    checks.append(check_rollups(Path('rollups')))
    logger.info("")
    
    # Check 2: Fallback
    logger.info("2. Checking DuckDB fallback...")
    checks.append(check_fallback(Path('fallback.duckdb')))
    logger.info("")
    
    # Check 3: Query files
    logger.info("3. Checking query files...")
    checks.append(check_queries())
    logger.info("")
    
    # If basic checks pass, test query execution
    if all(checks):
        checks.append(test_query_execution())
        logger.info("")
    
    # Summary
    logger.info("="*70)
    logger.info("Validation Summary")
    logger.info("="*70)
    
    if all(checks):
        logger.info("✅ All checks passed!")
        logger.info("")
        logger.info("Your setup is ready to run queries:")
        logger.info("  python3 run.py")
        logger.info("")
        logger.info("Or with custom queries:")
        logger.info("  python3 run.py --query-file ./your_queries.json")
        logger.info("  python3 run.py --query-dir ./your_query_dir")
        logger.info("")
        return 0
    else:
        logger.error("")
        logger.error("❌ Some checks failed!")
        logger.error("")
        logger.error("Please run the prepare phase first:")
        logger.error("  python3 prepare.py --data-dir ./data")
        logger.error("")
        return 1


if __name__ == "__main__":
    sys.exit(main())
