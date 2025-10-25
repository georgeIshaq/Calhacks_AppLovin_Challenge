"""
Compression & Encoding Microbenchmark

Tests different compression strategies to measure:
1. Compression ratio
2. Compression speed
3. Decompression speed
4. Memory usage

This helps decide which encodings to use for each column type.
"""

import time
import sys
import csv
from pathlib import Path
from typing import List, Tuple
import struct
import zlib

try:
    import lz4.frame
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False
    print("⚠️  lz4 not installed. Install with: pip install lz4")

import numpy as np


def load_column_sample(csv_file: Path, column: str, max_rows: int = 100000) -> List:
    """Load a single column from CSV"""
    print(f"Loading column '{column}' from {csv_file.name}...")
    
    values = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            val = row.get(column, '')
            values.append(val if val != '' else None)
    
    return values


def benchmark_dictionary_encoding(values: List) -> Tuple[float, int, int]:
    """Test dictionary encoding"""
    start = time.time()
    
    # Build dictionary
    unique_values = list(set(v for v in values if v is not None))
    value_to_id = {v: i for i, v in enumerate(unique_values)}
    
    # Encode as integers
    encoded = np.array([value_to_id.get(v, -1) if v is not None else -1 
                       for v in values], dtype=np.int32)
    
    # Dictionary size
    dict_bytes = sum(len(str(v).encode('utf-8')) for v in unique_values)
    
    # Encoded array size
    array_bytes = encoded.nbytes
    
    encode_time = time.time() - start
    total_bytes = dict_bytes + array_bytes
    
    return encode_time, total_bytes, len(unique_values)


def benchmark_rle_encoding(values: List) -> Tuple[float, int, int]:
    """Test run-length encoding"""
    start = time.time()
    
    runs = []
    if len(values) == 0:
        return 0, 0, 0
    
    current_val = values[0]
    count = 1
    
    for v in values[1:]:
        if v == current_val:
            count += 1
        else:
            runs.append((current_val, count))
            current_val = v
            count = 1
    runs.append((current_val, count))
    
    encode_time = time.time() - start
    
    # Estimate size (value + count per run)
    run_bytes = len(runs) * (8 + 4)  # 8 bytes for value string hash, 4 for count
    
    return encode_time, run_bytes, len(runs)


def benchmark_lz4_compression(data: bytes) -> Tuple[float, float, int]:
    """Test LZ4 compression"""
    if not HAS_LZ4:
        return 0, 0, len(data)
    
    # Compress
    start = time.time()
    compressed = lz4.frame.compress(data)
    compress_time = time.time() - start
    
    # Decompress
    start = time.time()
    _ = lz4.frame.decompress(compressed)
    decompress_time = time.time() - start
    
    return compress_time, decompress_time, len(compressed)


def benchmark_zlib_compression(data: bytes, level: int = 6) -> Tuple[float, float, int]:
    """Test zlib/gzip compression"""
    
    # Compress
    start = time.time()
    compressed = zlib.compress(data, level=level)
    compress_time = time.time() - start
    
    # Decompress
    start = time.time()
    _ = zlib.decompress(compressed)
    decompress_time = time.time() - start
    
    return compress_time, decompress_time, len(compressed)


def benchmark_column(csv_file: Path, column: str, max_rows: int = 100000):
    """Benchmark all encoding strategies for a column"""
    
    print(f"\n{'='*80}")
    print(f"BENCHMARKING COLUMN: {column}")
    print(f"{'='*80}")
    
    # Load data
    values = load_column_sample(csv_file, column, max_rows)
    print(f"Loaded {len(values):,} values")
    
    # Calculate null percentage
    null_count = sum(1 for v in values if v is None or v == '')
    null_pct = (null_count / len(values) * 100) if len(values) > 0 else 0
    print(f"NULL percentage: {null_pct:.1f}%")
    
    # Calculate cardinality
    unique_count = len(set(v for v in values if v is not None and v != ''))
    print(f"Unique values: {unique_count:,}")
    
    # Raw size (as strings)
    raw_bytes = sum(len(str(v).encode('utf-8')) for v in values if v is not None)
    print(f"Raw string size: {raw_bytes:,} bytes ({raw_bytes/1024/1024:.2f} MB)")
    
    print(f"\n{'Strategy':<30} {'Time (ms)':<15} {'Size (bytes)':<15} {'Ratio':<10} {'Throughput'}")
    print("-" * 90)
    
    # Dictionary encoding
    encode_time, dict_bytes, dict_size = benchmark_dictionary_encoding(values)
    ratio = raw_bytes / dict_bytes if dict_bytes > 0 else 0
    throughput = (raw_bytes / 1024 / 1024) / encode_time if encode_time > 0 else 0
    print(f"{'Dictionary Encoding':<30} {encode_time*1000:<15.2f} {dict_bytes:<15,} {ratio:<10.2f}x {throughput:.1f} MB/s")
    print(f"  └─ Dictionary size: {dict_size} entries")
    
    # RLE encoding
    encode_time, rle_bytes, run_count = benchmark_rle_encoding(values)
    ratio = raw_bytes / rle_bytes if rle_bytes > 0 else 0
    throughput = (raw_bytes / 1024 / 1024) / encode_time if encode_time > 0 else 0
    print(f"{'RLE Encoding':<30} {encode_time*1000:<15.2f} {rle_bytes:<15,} {ratio:<10.2f}x {throughput:.1f} MB/s")
    print(f"  └─ Run count: {run_count}")
    
    # LZ4 compression (on raw bytes)
    raw_data = ''.join(str(v) if v is not None else '' for v in values).encode('utf-8')
    if HAS_LZ4:
        comp_time, decomp_time, lz4_bytes = benchmark_lz4_compression(raw_data)
        ratio = len(raw_data) / lz4_bytes if lz4_bytes > 0 else 0
        comp_throughput = (len(raw_data) / 1024 / 1024) / comp_time if comp_time > 0 else 0
        decomp_throughput = (len(raw_data) / 1024 / 1024) / decomp_time if decomp_time > 0 else 0
        print(f"{'LZ4 Compression':<30} {comp_time*1000:<15.2f} {lz4_bytes:<15,} {ratio:<10.2f}x {comp_throughput:.1f} MB/s (comp)")
        print(f"{'LZ4 Decompression':<30} {decomp_time*1000:<15.2f} {lz4_bytes:<15,} {ratio:<10.2f}x {decomp_throughput:.1f} MB/s (decomp)")
    
    # Zlib compression
    comp_time, decomp_time, zlib_bytes = benchmark_zlib_compression(raw_data, level=6)
    ratio = len(raw_data) / zlib_bytes if zlib_bytes > 0 else 0
    comp_throughput = (len(raw_data) / 1024 / 1024) / comp_time if comp_time > 0 else 0
    decomp_throughput = (len(raw_data) / 1024 / 1024) / decomp_time if decomp_time > 0 else 0
    print(f"{'Zlib Compression (level 6)':<30} {comp_time*1000:<15.2f} {zlib_bytes:<15,} {ratio:<10.2f}x {comp_throughput:.1f} MB/s (comp)")
    print(f"{'Zlib Decompression':<30} {decomp_time*1000:<15.2f} {zlib_bytes:<15,} {ratio:<10.2f}x {decomp_throughput:.1f} MB/s (decomp)")
    
    # Recommendation
    print(f"\n💡 RECOMMENDATION:")
    if unique_count < 100:
        print(f"   Use DICTIONARY encoding (only {unique_count} unique values)")
        if unique_count < 10:
            print(f"   Also consider BITMAP index for filtering")
    elif unique_count < 1000:
        print(f"   Use DICTIONARY + RLE encoding")
    else:
        if HAS_LZ4:
            print(f"   Use LZ4 compression (fast decompression: {decomp_throughput:.0f} MB/s)")
        else:
            print(f"   Use Zlib compression or install LZ4 for better performance")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Benchmark compression strategies')
    parser.add_argument('--data-file', default='data/events_part_00000.csv',
                       help='CSV file to sample')
    parser.add_argument('--columns', nargs='+', 
                       default=['type', 'country', 'advertiser_id', 'publisher_id', 'bid_price'],
                       help='Columns to benchmark')
    parser.add_argument('--max-rows', type=int, default=100000,
                       help='Maximum rows to sample')
    
    args = parser.parse_args()
    
    data_file = Path(args.data_file)
    if not data_file.exists():
        print(f"Error: Data file not found: {args.data_file}")
        return 1
    
    print("=" * 80)
    print("COMPRESSION & ENCODING BENCHMARK")
    print("=" * 80)
    print(f"\nData file: {data_file}")
    print(f"Sample size: {args.max_rows:,} rows")
    print(f"Columns: {', '.join(args.columns)}")
    
    for column in args.columns:
        try:
            benchmark_column(data_file, column, args.max_rows)
        except Exception as e:
            print(f"\n❌ Error benchmarking column '{column}': {e}")
    
    print(f"\n{'='*80}")
    print("BENCHMARK COMPLETE")
    print("=" * 80)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
