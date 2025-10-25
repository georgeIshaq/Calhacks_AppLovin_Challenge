#!/usr/bin/env python3
"""
Storage Writer - Arrow IPC serialization with LZ4 compression

This module writes rollup DataFrames to disk as Arrow IPC files.
Uses LZ4 compression for fast load times (<5ms per rollup).

Key features:
- Arrow IPC format (zero-copy deserialization)
- LZ4 compression (7-10× compression, fast decompression)
- Partitioned storage for large rollups (minute_type)
- Directory structure for easy lookup
"""

import polars as pl
from pathlib import Path
from typing import Dict, List
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StorageWriter:
    """
    Writes rollup DataFrames to Arrow IPC files.
    
    Directory structure:
        rollups/
            day_type.arrow
            hour_type.arrow
            week_type.arrow
            country_type.arrow
            advertiser_type.arrow
            publisher_type.arrow
            minute_type/
                day_2024_001.arrow
                day_2024_002.arrow
                ...
                day_2024_366.arrow
            day_country_type.arrow
            day_advertiser_type.arrow
            day_publisher_type.arrow
            week_country_type.arrow
            hour_country_type.arrow
    """
    
    def __init__(self, output_dir: Path):
        """
        Initialize storage writer.
        
        Args:
            output_dir: Directory to write rollup files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Storage writer initialized: {self.output_dir}")
    
    def write_rollup(
        self, 
        name: str, 
        df: pl.DataFrame,
        compression: str = 'lz4'
    ) -> Path:
        """
        Write a single rollup to Arrow IPC file.
        
        Args:
            name: Rollup name (e.g., "day_type")
            df: DataFrame to write
            compression: Compression algorithm ('lz4', 'zstd', or None)
        
        Returns:
            Path to written file
        """
        logger.info(f"Writing rollup: {name} ({len(df):,} rows)")
        start_time = time.time()
        
        # Determine output path
        output_path = self.output_dir / f"{name}.arrow"
        
        # Write with compression
        df.write_ipc(output_path, compression=compression)
        
        # Get file stats
        file_size_kb = output_path.stat().st_size / 1024
        write_time = time.time() - start_time
        
        logger.info(f"✅ {name}: {file_size_kb:.1f} KB written in {write_time*1000:.1f}ms")
        
        return output_path
    
    def write_partitioned_rollup(
        self,
        base_name: str,
        partitions: Dict[str, pl.DataFrame],
        compression: str = 'lz4'
    ) -> List[Path]:
        """
        Write partitioned rollup (e.g., minute_type by day).
        
        Args:
            base_name: Base rollup name (e.g., "minute_type")
            partitions: Dict of partition_name -> DataFrame
            compression: Compression algorithm
        
        Returns:
            List of paths to written files
        """
        logger.info(f"Writing partitioned rollup: {base_name} ({len(partitions)} partitions)")
        start_time = time.time()
        
        # Create subdirectory for partitions
        partition_dir = self.output_dir / base_name
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        written_paths = []
        total_size_kb = 0
        
        # Write each partition
        for partition_name, df in partitions.items():
            # Extract day identifier from partition name
            # e.g., "minute_type_day_2024_001" -> "day_2024_001"
            day_part = partition_name.replace(f"{base_name}_", "")
            output_path = partition_dir / f"{day_part}.arrow"
            
            # Write partition
            df.write_ipc(output_path, compression=compression)
            
            file_size_kb = output_path.stat().st_size / 1024
            total_size_kb += file_size_kb
            
            written_paths.append(output_path)
        
        write_time = time.time() - start_time
        avg_size_kb = total_size_kb / len(partitions)
        
        logger.info(f"✅ {base_name}: {len(partitions)} partitions, "
                   f"{total_size_kb/1024:.1f} MB total, "
                   f"{avg_size_kb:.1f} KB avg, "
                   f"written in {write_time:.1f}s")
        
        return written_paths
    
    def write_all_rollups(
        self,
        rollups: Dict[str, pl.DataFrame],
        compression: str = 'lz4'
    ) -> Dict[str, Path]:
        """
        Write all rollups to disk.
        
        Handles both regular and partitioned rollups automatically.
        
        Args:
            rollups: Dict of rollup_name -> DataFrame
            compression: Compression algorithm
        
        Returns:
            Dict of rollup_name -> file path (or directory for partitioned)
        """
        logger.info("="*60)
        logger.info("WRITING ALL ROLLUPS TO DISK")
        logger.info("="*60)
        
        start_total = time.time()
        written_paths = {}
        
        # Separate partitioned rollups from regular ones
        partitioned_rollups = {}
        regular_rollups = {}
        
        for name, df in rollups.items():
            if name.startswith('minute_type_day_'):
                # This is a minute_type partition
                base_name = 'minute_type'
                if base_name not in partitioned_rollups:
                    partitioned_rollups[base_name] = {}
                partitioned_rollups[base_name][name] = df
            else:
                regular_rollups[name] = df
        
        # Write regular rollups
        for name, df in regular_rollups.items():
            path = self.write_rollup(name, df, compression)
            written_paths[name] = path
        
        # Write partitioned rollups
        for base_name, partitions in partitioned_rollups.items():
            paths = self.write_partitioned_rollup(base_name, partitions, compression)
            written_paths[base_name] = self.output_dir / base_name  # Store directory path
        
        total_time = time.time() - start_total
        
        # Calculate total disk usage
        total_size_mb = 0
        for path in self.output_dir.rglob("*.arrow"):
            total_size_mb += path.stat().st_size / (1024 * 1024)
        
        logger.info("="*60)
        logger.info("WRITE COMPLETE!")
        logger.info("="*60)
        logger.info(f"Rollups written: {len(written_paths)}")
        logger.info(f"Total disk space: {total_size_mb:.1f} MB")
        logger.info(f"Write time: {total_time:.1f}s")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info("="*60)
        
        return written_paths
    
    def load_rollup(self, name: str) -> pl.DataFrame:
        """
        Load a rollup from disk (for testing).
        
        Args:
            name: Rollup name (e.g., "day_type")
        
        Returns:
            Loaded DataFrame
        """
        rollup_path = self.output_dir / f"{name}.arrow"
        
        if not rollup_path.exists():
            raise FileNotFoundError(f"Rollup not found: {rollup_path}")
        
        logger.info(f"Loading rollup: {name}")
        start_time = time.time()
        
        df = pl.read_ipc(rollup_path)
        
        load_time = time.time() - start_time
        logger.info(f"✅ Loaded {name}: {len(df):,} rows in {load_time*1000:.2f}ms")
        
        return df
    
    def load_partition(self, base_name: str, partition_key: str) -> pl.DataFrame:
        """
        Load a single partition from a partitioned rollup.
        
        Args:
            base_name: Base rollup name (e.g., "minute_type")
            partition_key: Partition identifier (e.g., "day_2024_001")
        
        Returns:
            Loaded DataFrame
        """
        partition_path = self.output_dir / base_name / f"{partition_key}.arrow"
        
        if not partition_path.exists():
            raise FileNotFoundError(f"Partition not found: {partition_path}")
        
        logger.info(f"Loading partition: {base_name}/{partition_key}")
        start_time = time.time()
        
        df = pl.read_ipc(partition_path)
        
        load_time = time.time() - start_time
        logger.info(f"✅ Loaded {base_name}/{partition_key}: {len(df):,} rows in {load_time*1000:.2f}ms")
        
        return df
    
    def get_partition_keys(self, base_name: str) -> List[str]:
        """
        Get all partition keys for a partitioned rollup.
        
        Args:
            base_name: Base rollup name (e.g., "minute_type")
        
        Returns:
            List of partition keys (e.g., ["day_2024_001", "day_2024_002", ...])
        """
        partition_dir = self.output_dir / base_name
        
        if not partition_dir.exists():
            return []
        
        partition_files = sorted(partition_dir.glob("*.arrow"))
        partition_keys = [f.stem for f in partition_files]
        
        return partition_keys
    
    def get_storage_stats(self) -> Dict:
        """
        Get statistics about stored rollups.
        
        Returns:
            Dictionary with storage statistics
        """
        stats = {
            'rollup_count': 0,
            'partition_count': 0,
            'total_size_mb': 0,
            'rollups': {}
        }
        
        # Count regular rollups
        for rollup_file in self.output_dir.glob("*.arrow"):
            rollup_name = rollup_file.stem
            file_size_mb = rollup_file.stat().st_size / (1024 * 1024)
            
            stats['rollup_count'] += 1
            stats['total_size_mb'] += file_size_mb
            stats['rollups'][rollup_name] = {
                'size_mb': file_size_mb,
                'type': 'regular'
            }
        
        # Count partitioned rollups
        for partition_dir in self.output_dir.iterdir():
            if partition_dir.is_dir():
                partition_files = list(partition_dir.glob("*.arrow"))
                partition_count = len(partition_files)
                partition_size_mb = sum(
                    f.stat().st_size / (1024 * 1024) 
                    for f in partition_files
                )
                
                stats['rollup_count'] += 1
                stats['partition_count'] += partition_count
                stats['total_size_mb'] += partition_size_mb
                stats['rollups'][partition_dir.name] = {
                    'size_mb': partition_size_mb,
                    'type': 'partitioned',
                    'partitions': partition_count
                }
        
        return stats


def main():
    """Test the storage writer."""
    from data_loader import DataLoader
    from rollup_builder import RollupBuilder
    
    print("\n" + "="*60)
    print("Storage Writer Test")
    print("="*60)
    
    # Initialize components
    data_dir = Path(__file__).parent.parent.parent / "data"
    output_dir = Path(__file__).parent.parent.parent / "rollups_test"
    
    loader = DataLoader(data_dir)
    builder = RollupBuilder(loader)
    writer = StorageWriter(output_dir)
    
    # Test 1: Build and write a small rollup
    print("\nTest 1: Building and writing country_type rollup...")
    country_type = builder.build_rollup('country_type', ['country', 'type'])
    print(f"Built: {len(country_type):,} rows, {country_type.estimated_size('mb'):.2f} MB")
    
    path = writer.write_rollup('country_type', country_type)
    print(f"✅ Written to: {path}")
    
    # Test 2: Load the rollup back
    print("\nTest 2: Loading rollup back from disk...")
    loaded = writer.load_rollup('country_type')
    print(f"✅ Loaded: {len(loaded):,} rows")
    
    # Verify data matches
    if len(loaded) == len(country_type):
        print("✅ Row count matches!")
    else:
        print("❌ Row count mismatch!")
    
    # Test 3: Storage stats
    print("\nTest 3: Getting storage statistics...")
    stats = writer.get_storage_stats()
    print(f"✅ Storage stats:")
    print(f"   Rollups: {stats['rollup_count']}")
    print(f"   Total size: {stats['total_size_mb']:.2f} MB")
    for name, info in stats['rollups'].items():
        print(f"   - {name}: {info['size_mb']:.3f} MB ({info['type']})")
    
    print("\n" + "="*60)
    print("Storage Writer Test Complete! ✅")
    print("="*60)
    print("\nReady to write full rollup set!")
    print("Run: writer.write_all_rollups(rollups_dict)")


if __name__ == "__main__":
    main()
