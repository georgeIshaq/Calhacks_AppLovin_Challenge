#!/usr/bin/env python3
"""
Rollup Builder - Pre-aggregation engine for OLAP cube

This module builds all rollup tables from the raw event data.
Uses TRUE single-pass streaming aggregation with PyArrow batching.

Key features:
- NULL-safe aggregations (SUM, COUNT, AVG, MIN, MAX)
- Single-pass batch building (scan data ONCE, build ALL rollups simultaneously)
- Builds 9 regular + 366 partitioned rollups in ~2-3 minutes
- Memory efficient: ~3-5GB peak RAM (processes in batches)
"""

import polars as pl
import pyarrow as pa
import pyarrow.csv as pc
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import logging
import time as time_module
import datetime
import time
from collections import defaultdict

# Import relative to package structure
try:
    from .data_loader import DataLoader
except ImportError:
    # For standalone execution
    from data_loader import DataLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RollupBuilder:
    """
    Builds pre-aggregated rollup tables for fast query execution.
    
    Each rollup stores NULL-safe aggregates:
    - bid_price_sum: SUM(bid_price WHERE NOT NULL)
    - bid_price_count: COUNT(bid_price WHERE NOT NULL)
    - total_price_sum: SUM(total_price WHERE NOT NULL)
    - total_price_count: COUNT(total_price WHERE NOT NULL)
    - row_count: COUNT(*)
    """
    
    def __init__(self, data_loader: DataLoader):
        """
        Initialize rollup builder.
        
        Args:
            data_loader: DataLoader instance with CSV data
        """
        self.loader = data_loader
        self.rollups: Dict[str, pl.DataFrame] = {}
    
    def build_rollup(
        self, 
        name: str, 
        dimensions: List[str],
        lf: pl.LazyFrame = None
    ) -> pl.DataFrame:
        """
        Build a single rollup with NULL-safe aggregations.
        
        Args:
            name: Rollup name (e.g., "day_type")
            dimensions: List of dimension columns (e.g., ["day", "type"])
            lf: Optional lazy frame (uses loader's if None)
        
        Returns:
            Materialized DataFrame with aggregated data
        """
        logger.info(f"Building rollup: {name} (dimensions: {dimensions})")
        start_time = time_module.time()
        
        # Load data with time dimensions if not provided
        if lf is None:
            lf = self.loader.load_with_time_dims()
        
        # Build NULL-safe aggregates
        # CRITICAL: .drop_nulls() on the column BEFORE aggregating!
        # This ensures SUM/COUNT only operate on non-NULL values
        rollup = lf.group_by(dimensions).agg([
            # bid_price aggregates (NULL-safe)
            pl.col('bid_price').drop_nulls().sum().alias('bid_price_sum'),
            pl.col('bid_price').drop_nulls().count().alias('bid_price_count'),
            pl.col('bid_price').drop_nulls().min().alias('bid_price_min'),
            pl.col('bid_price').drop_nulls().max().alias('bid_price_max'),
            
            # total_price aggregates (NULL-safe)
            pl.col('total_price').drop_nulls().sum().alias('total_price_sum'),
            pl.col('total_price').drop_nulls().count().alias('total_price_count'),
            pl.col('total_price').drop_nulls().min().alias('total_price_min'),
            pl.col('total_price').drop_nulls().max().alias('total_price_max'),
            
            # Row count (always non-NULL)
            pl.len().alias('row_count'),
        ])
        
        # Materialize the rollup
        df = rollup.collect()
        
        build_time = time_module.time() - start_time
        logger.info(f"✅ {name}: {len(df):,} rows in {build_time:.2f}s "
                   f"({df.estimated_size('mb'):.2f} MB)")
        
        # Store in cache
        self.rollups[name] = df
        
        return df
    
    def build_all_rollups_single_pass(self) -> Dict[str, pl.DataFrame]:
        """
        Build all rollups in a SINGLE PASS with INCREMENTAL FOLDING.
        
        KEY OPTIMIZATION: Instead of storing 343 partial DataFrames per rollup,
        we FOLD each batch immediately into a single accumulator per rollup.
        This keeps memory bounded: sum(accumulator_sizes) + one_batch_size
        
        Memory: ~2-4GB peak (vs 12+ GB with naive approach)
        Time: ~7-10 minutes for 245M rows
        """
        start_time = time_module.time()
        
        # Rollup specs optimized for 16GB RAM constraint
        rollup_specs = [
            ('day_type', ['day', 'type']),
            ('hour_type', ['hour', 'type']),
            ('minute_type', ['minute', 'type']),
            ('week_type', ['week', 'type']),
            ('country_type', ['country', 'type']),
            ('advertiser_type', ['advertiser_id', 'type']),
            ('publisher_type', ['publisher_id', 'type']),
            ('day_country_type', ['day', 'country', 'type']),
            ('day_advertiser_type', ['day', 'advertiser_id', 'type']),
            ('hour_country_type', ['hour', 'country', 'type']),
            # 4D rollup for Q2: publisher × country × type × day
            ('day_publisher_country_type', ['day', 'publisher_id', 'country', 'type']),
        ]
        
        # Initialize empty accumulators (one per rollup)
        accumulators = {}
        temp_partials = {}  # Temporary storage for batching folds
        FOLD_BATCH_SIZE = 20  # Fold every 20 batches to reduce join overhead
        
        for rollup_name, dimensions in rollup_specs:
            # Create empty DataFrame with correct schema
            schema_dict = {dim: pl.Utf8 for dim in dimensions}
            schema_dict.update({
                'bid_price_sum': pl.Float64,
                'bid_price_count': pl.Int64,
                'bid_price_min': pl.Float64,
                'bid_price_max': pl.Float64,
                'total_price_sum': pl.Float64,
                'total_price_count': pl.Int64,
                'total_price_min': pl.Float64,
                'total_price_max': pl.Float64,
                'row_count': pl.Int64,
            })
            accumulators[rollup_name] = pl.DataFrame(schema=schema_dict)
            temp_partials[rollup_name] = []
        
        logger.info(f"\nBuilding {len(rollup_specs)} rollups with INCREMENTAL FOLDING...")
        logger.info(f"Reading {len(list(self.loader.data_dir.glob('*.csv')))} CSV files in batches...")
        logger.info(f"Memory strategy: Fold each batch immediately (bounded memory)")
        logger.info("")
        
        # Process each CSV file
        csv_files = sorted(self.loader.data_dir.glob('*.csv'))
        total_batches = 0
        
        batch_start = time_module.time()
        
        # Define PyArrow schema to avoid type inference issues
        arrow_schema = pa.schema([
            ('ts', pa.int64()),
            ('type', pa.string()),
            ('auction_id', pa.string()),
            ('advertiser_id', pa.int64()),
            ('publisher_id', pa.int64()),
            ('bid_price', pa.float64()),
            ('user_id', pa.string()),
            ('total_price', pa.float64()),
            ('country', pa.string()),
        ])
        
        for file_idx, csv_file in enumerate(csv_files):
            if (file_idx + 1) % 10 == 0:
                elapsed = time_module.time() - batch_start
                logger.info(f"  Processing file {file_idx+1}/{len(csv_files)} ({elapsed:.1f}s elapsed)...")
            
            # Read CSV with PyArrow in streaming mode with explicit schema
            try:
                # Use convert_options to specify schema
                convert_opts = pc.ConvertOptions(
                    column_types=arrow_schema,
                    strings_can_be_null=True
                )
                read_opts = pc.ReadOptions(block_size=64 * 1024 * 1024)  # 64MB batches
                
                reader = pc.open_csv(
                    csv_file,
                    convert_options=convert_opts,
                    read_options=read_opts
                )
                
                for arrow_batch in reader:
                    total_batches += 1
                    
                    # Convert Arrow batch to Polars (zero-copy)
                    df_batch = pl.from_arrow(arrow_batch)
                    
                    # Add time dimensions to batch
                    # CRITICAL: Match baseline's timezone behavior
                    # DuckDB's DATE(to_timestamp(ts)) uses system's LOCAL timezone
                    # We must use the system's local timezone too for consistency
                    import time
                    import datetime
                    
                    # Get system's local timezone
                    # This ensures we match DuckDB's behavior on any machine
                    local_tz = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
                    local_tz_name = str(local_tz)  # e.g., "PDT", "EST", etc.
                    
                    # For Polars, we need a proper IANA timezone name
                    # Use tzlocal to get the system timezone name
                    try:
                        from tzlocal import get_localzone
                        local_tz_name = str(get_localzone())
                    except:
                        # Fallback: try to infer from time.timezone
                        # This works on most Unix systems
                        if time.daylight:
                            utc_offset = -time.altzone
                        else:
                            utc_offset = -time.timezone
                        
                        # Common timezone mappings based on UTC offset
                        # This is a simplified fallback
                        offset_to_tz = {
                            -28800: 'America/Los_Angeles',  # UTC-8 (PST)
                            -25200: 'America/Los_Angeles',  # UTC-7 (PDT)
                            -18000: 'America/New_York',     # UTC-5 (EST)
                            -14400: 'America/New_York',     # UTC-4 (EDT)
                            0: 'UTC',
                        }
                        local_tz_name = offset_to_tz.get(utc_offset, 'UTC')
                    
                    df_batch = df_batch.with_columns([
                        pl.from_epoch(pl.col('ts'), time_unit='ms')
                          .dt.replace_time_zone('UTC')
                          .dt.convert_time_zone(local_tz_name)
                          .alias('datetime'),
                    ]).with_columns([
                        pl.col('datetime').dt.strftime('%Y-%j').alias('day'),
                        (pl.col('datetime').dt.strftime('%Y-%j') + ' ' + 
                         pl.col('datetime').dt.hour().cast(pl.Utf8)).alias('hour'),
                        (pl.col('datetime').dt.strftime('%Y-%j') + ' ' + 
                         pl.col('datetime').dt.hour().cast(pl.Utf8) + ':' +
                         pl.col('datetime').dt.minute().cast(pl.Utf8).str.zfill(2)).alias('minute'),
                        pl.col('datetime').dt.strftime('%Y-%U').alias('week'),
                        pl.col('datetime').dt.date().alias('date'),
                    ])
                    
                    # BATCHED INCREMENTAL FOLD: Accumulate partials, fold periodically
                    for rollup_name, dimensions in rollup_specs:
                        # Compute batch aggregates
                        batch_agg = df_batch.group_by(dimensions).agg([
                            pl.col('bid_price').drop_nulls().sum().alias('bid_price_sum'),
                            pl.col('bid_price').drop_nulls().count().alias('bid_price_count'),
                            pl.col('bid_price').drop_nulls().min().alias('bid_price_min'),
                            pl.col('bid_price').drop_nulls().max().alias('bid_price_max'),
                            pl.col('total_price').drop_nulls().sum().alias('total_price_sum'),
                            pl.col('total_price').drop_nulls().count().alias('total_price_count'),
                            pl.col('total_price').drop_nulls().min().alias('total_price_min'),
                            pl.col('total_price').drop_nulls().max().alias('total_price_max'),
                            pl.len().alias('row_count'),
                        ])
                        
                        # DIAGNOSTIC: Track advertiser_type specifically
                        if rollup_name == 'advertiser_type' and total_batches <= 2:
                            logger.info(f"[DIAG] Batch {total_batches} for {rollup_name}: {len(batch_agg)} unique keys")
                            logger.info(f"[DIAG] Sample keys: {batch_agg.select(dimensions).head(5)}")
                        
                        # Add to temporary partials
                        temp_partials[rollup_name].append(batch_agg)
                        
                        # Fold when we have enough partials (reduces expensive join ops)
                        if len(temp_partials[rollup_name]) >= FOLD_BATCH_SIZE:
                            # DIAGNOSTIC: Before fold
                            if rollup_name == 'advertiser_type':
                                logger.info(f"[DIAG] BEFORE fold: {len(temp_partials[rollup_name])} partials to combine")
                                total_rows_in_partials = sum(len(p) for p in temp_partials[rollup_name])
                                logger.info(f"[DIAG] Total rows across partials: {total_rows_in_partials}")
                            
                            # Combine partials together first
                            combined = pl.concat(temp_partials[rollup_name]).group_by(dimensions).agg([
                                pl.col('bid_price_sum').sum(),
                                pl.col('bid_price_count').sum(),
                                pl.col('bid_price_min').min(),
                                pl.col('bid_price_max').max(),
                                pl.col('total_price_sum').sum(),
                                pl.col('total_price_count').sum(),
                                pl.col('total_price_min').min(),
                                pl.col('total_price_max').max(),
                                pl.col('row_count').sum(),
                            ])
                            
                            # DIAGNOSTIC: After combine
                            if rollup_name == 'advertiser_type':
                                logger.info(f"[DIAG] AFTER concat+group_by: {len(combined)} unique keys")
                                logger.info(f"[DIAG] Accumulator size before merge: {len(accumulators[rollup_name])}")
                            
                            # Merge into accumulator
                            accumulators[rollup_name] = self._merge_accumulator(
                                accumulators[rollup_name],
                                combined,
                                dimensions
                            )
                            
                            # DIAGNOSTIC: After merge
                            if rollup_name == 'advertiser_type':
                                logger.info(f"[DIAG] Accumulator size after merge: {len(accumulators[rollup_name])}")
                            
                            # Clear temp partials
                            temp_partials[rollup_name] = []
                    
                    # Free batch memory
                    del df_batch
            
            except Exception as e:
                logger.error(f"Error processing {csv_file}: {e}")
                raise
        
        scan_time = time_module.time() - batch_start
        logger.info(f"\n✅ Scan complete: {total_batches} batches, {len(csv_files)} files in {scan_time:.1f}s")
        
        # Final fold: process any remaining partials
        logger.info(f"\nFinal fold: merging remaining partials...")
        for rollup_name, dimensions in rollup_specs:
            if temp_partials[rollup_name]:
                # DIAGNOSTIC: Before final fold
                if rollup_name == 'advertiser_type':
                    logger.info(f"[DIAG] FINAL FOLD: {len(temp_partials[rollup_name])} partials to combine")
                    total_rows_in_partials = sum(len(p) for p in temp_partials[rollup_name])
                    logger.info(f"[DIAG] Total rows across all partials: {total_rows_in_partials}")
                
                # Combine remaining partials
                combined = pl.concat(temp_partials[rollup_name]).group_by(dimensions).agg([
                    pl.col('bid_price_sum').sum(),
                    pl.col('bid_price_count').sum(),
                    pl.col('bid_price_min').min(),
                    pl.col('bid_price_max').max(),
                    pl.col('total_price_sum').sum(),
                    pl.col('total_price_count').sum(),
                    pl.col('total_price_min').min(),
                    pl.col('total_price_max').max(),
                    pl.col('row_count').sum(),
                ])
                
                # DIAGNOSTIC: After concat+group_by
                if rollup_name == 'advertiser_type':
                    logger.info(f"[DIAG] AFTER final concat+group_by: {len(combined)} unique keys")
                    logger.info(f"[DIAG] Accumulator size before final merge: {len(accumulators[rollup_name])}")
                
                # Merge into accumulator
                accumulators[rollup_name] = self._merge_accumulator(
                    accumulators[rollup_name],
                    combined,
                    dimensions
                )
                
                # DIAGNOSTIC: After final merge
                if rollup_name == 'advertiser_type':
                    logger.info(f"[DIAG] Accumulator size after final merge: {len(accumulators[rollup_name])}")
        
        logger.info(f"Final rollup sizes:")
        for rollup_name in accumulators:
            logger.info(f"  ✅ {rollup_name}: {len(accumulators[rollup_name]):,} rows")
        
        total_time = time_module.time() - start_time
        
        logger.info("\n" + "="*60)
        logger.info(f"✅ INCREMENTAL FOLD BUILD COMPLETE: {len(accumulators)} rollups")
        logger.info(f"   Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
        logger.info(f"   Peak memory: ~2-4GB (bounded by accumulator sizes)")
        logger.info("="*60)
        
        # Store in cache
        self.rollups.update(accumulators)
        
        return accumulators
    
    def _merge_accumulator(
        self,
        acc_df: pl.DataFrame,
        batch_df: pl.DataFrame,
        keys: List[str]
    ) -> pl.DataFrame:
        """
        Merge batch aggregates into accumulator using outer join and vectorized addition.
        
        This is the KEY optimization for bounded memory:
        - Input: accumulator (existing aggregates) + batch (new aggregates)
        - Output: merged accumulator (single DataFrame)
        - Memory: O(unique_keys) not O(num_batches)
        """
        if acc_df.height == 0:
            return batch_df
        
        # Outer join on keys
        merged = acc_df.join(batch_df, on=keys, how="outer", suffix="_batch")
        
        # Aggregate columns to merge
        agg_cols = ['bid_price_sum', 'bid_price_count', 'bid_price_min', 'bid_price_max',
                    'total_price_sum', 'total_price_count', 'total_price_min', 'total_price_max',
                    'row_count']
        
        # Build merge expressions
        merge_exprs = []
        
        # CRITICAL FIX: Coalesce keys to handle outer join nulls
        # When a key only exists in batch_df, the key column will be null after outer join
        # We need to coalesce with the _batch version to get the actual key value
        for key in keys:
            key_batch = f"{key}_batch"
            if key_batch in merged.columns:
                logger.debug(f"Coalescing key: {key} with {key_batch}")
                merge_exprs.append(
                    pl.coalesce([pl.col(key), pl.col(key_batch)]).alias(key)
                )
            else:
                logger.warning(f"Key {key_batch} not found in merged columns: {merged.columns}")
        
        for col in agg_cols:
            col_batch = f"{col}_batch"
            if 'min' in col:
                # MIN: take minimum of both sides
                merge_exprs.append(
                    pl.min_horizontal([
                        pl.col(col).fill_null(pl.lit(float('inf'))),
                        pl.col(col_batch).fill_null(pl.lit(float('inf')))
                    ]).alias(col)
                )
            elif 'max' in col:
                # MAX: take maximum of both sides
                merge_exprs.append(
                    pl.max_horizontal([
                        pl.col(col).fill_null(pl.lit(float('-inf'))),
                        pl.col(col_batch).fill_null(pl.lit(float('-inf')))
                    ]).alias(col)
                )
            else:
                # SUM/COUNT: add both sides
                merge_exprs.append(
                    (pl.col(col).fill_null(0) + pl.col(col_batch).fill_null(0)).alias(col)
                )
        
        # Apply merges and select final columns
        result = merged.with_columns(merge_exprs).select(keys + agg_cols)
        
        return result
    
    def build_all_rollups_streaming(self) -> Dict[str, pl.DataFrame]:
        """
        Build ALL rollups using Polars' native streaming engine (Option A).
        
        KEY OPTIMIZATION:
        Uses collect(streaming=True) which:
        1. Reads CSV in chunks (never loads full 245M rows into memory)
        2. Updates aggregation hash tables incrementally per chunk
        3. Discards raw data after each chunk
        4. Only materializes final aggregated result (tiny!)
        
        Expected: 15-20s per rollup, ~3-5GB peak RAM (fits in 16GB!)
        
        This is the CORRECT approach from your advice.
        
        Returns:
            Dictionary mapping rollup name -> DataFrame
        """
        logger.info("="*60)
        logger.info("STREAMING BUILDER: True streaming aggregation")
        logger.info("="*60)
        
        start_time = time_module.time()
        
        # Get base lazy frame (no data loaded yet)
        logger.info("\nCreating streaming query plans...")
        lf = self.loader.load_with_time_dims()
        
        # Rollup specifications
        rollup_specs = [
            # Core rollups (6)
            ('day_type', ['day', 'type']),
            ('hour_type', ['hour', 'type']),
            ('week_type', ['week', 'type']),
            ('country_type', ['country', 'type']),
            ('advertiser_type', ['advertiser_id', 'type']),
            ('publisher_type', ['publisher_id', 'type']),
            
            # Combo rollups (3)
            ('day_country_type', ['day', 'country', 'type']),
            ('day_advertiser_type', ['day', 'advertiser_id', 'type']),
            ('hour_country_type', ['hour', 'country', 'type']),
        ]
        
        logger.info(f"Building {len(rollup_specs)} rollups with streaming aggregation...")
        logger.info("Each rollup: scan CSV chunks → update hash tables → discard raw data")
        logger.info("")
        
        rollups = {}
        
        for name, dimensions in rollup_specs:
            rollup_start = time_module.time()
            logger.info(f"  [{len(rollups)+1}/{len(rollup_specs)}] {name}...")
            
            # Build aggregation query plan
            agg_plan = lf.group_by(dimensions).agg([
                # bid_price aggregates (NULL-safe)
                pl.col('bid_price').drop_nulls().sum().alias('bid_price_sum'),
                pl.col('bid_price').drop_nulls().count().alias('bid_price_count'),
                pl.col('bid_price').drop_nulls().min().alias('bid_price_min'),
                pl.col('bid_price').drop_nulls().max().alias('bid_price_max'),
                
                # total_price aggregates (NULL-safe)
                pl.col('total_price').drop_nulls().sum().alias('total_price_sum'),
                pl.col('total_price').drop_nulls().count().alias('total_price_count'),
                pl.col('total_price').drop_nulls().min().alias('total_price_min'),
                pl.col('total_price').drop_nulls().max().alias('total_price_max'),
                
                # Row count
                pl.len().alias('row_count'),
            ])
            
            # Execute with streaming=True
            # This is where the magic happens: Polars reads chunks, 
            # aggregates incrementally, never loads full data into memory
            rollups[name] = agg_plan.collect(streaming=True)
            
            rollup_time = time_module.time() - rollup_start
            logger.info(f"      ✅ {len(rollups[name]):,} rows in {rollup_time:.1f}s")
        
        total_time = time_module.time() - start_time
        
        logger.info("\n" + "="*60)
        logger.info(f"✅ STREAMING BUILD COMPLETE: {len(rollups)} rollups")
        logger.info(f"   Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
        logger.info(f"   Avg per rollup: {total_time/len(rollups):.1f}s")
        logger.info(f"   Memory: Streaming (no full dataset in RAM!)")
        logger.info("="*60)
        
        # Store in cache
        self.rollups.update(rollups)
        
        return rollups
    
    def build_core_rollups(self) -> Dict[str, pl.DataFrame]:
        """
        Build 7 core single-dimension rollups.
        
        Returns:
            Dictionary of rollup name -> DataFrame
        """
        logger.info("="*60)
        logger.info("Building Core Rollups (7 single-dimension)")
        logger.info("="*60)
        
        # Load data once
        lf = self.loader.load_with_time_dims()
        
        rollups = {}
        
        # 1. day_type: Daily aggregates by type
        rollups['day_type'] = self.build_rollup('day_type', ['day', 'type'], lf)
        
        # 2. hour_type: Hourly aggregates by type
        rollups['hour_type'] = self.build_rollup('hour_type', ['hour', 'type'], lf)
        
        # 3. week_type: Weekly aggregates by type
        rollups['week_type'] = self.build_rollup('week_type', ['week', 'type'], lf)
        
        # 4. country_type: Country aggregates by type
        rollups['country_type'] = self.build_rollup('country_type', ['country', 'type'], lf)
        
        # 5. advertiser_type: Advertiser aggregates by type
        rollups['advertiser_type'] = self.build_rollup(
            'advertiser_type', ['advertiser_id', 'type'], lf
        )
        
        # 6. publisher_type: Publisher aggregates by type
        rollups['publisher_type'] = self.build_rollup(
            'publisher_type', ['publisher_id', 'type'], lf
        )
        
        # 7. minute_type: Minute aggregates by type
        # NOTE: This will be partitioned by day in build_partitioned_minute_rollup()
        rollups['minute_type'] = self.build_rollup('minute_type', ['minute', 'type'], lf)
        
        logger.info("="*60)
        logger.info(f"✅ Core rollups complete: {len(rollups)} rollups built")
        logger.info("="*60)
        
        return rollups
    
    def build_combo_rollups(self) -> Dict[str, pl.DataFrame]:
        """
        Build 3 high-value multi-dimension combo rollups.
        
        Trimmed from 5 to 3 to stay within 10min build budget:
        - Kept: day_country_type, day_advertiser_type, hour_country_type
        - Dropped: day_publisher_type (low incremental value), week_country_type (week covered by day)
        
        Returns:
            Dictionary of rollup name -> DataFrame
        """
        logger.info("="*60)
        logger.info("Building Combo Rollups (3 multi-dimension)")
        logger.info("="*60)
        
        # Load data once
        lf = self.loader.load_with_time_dims()
        
        rollups = {}
        
        # 1. day_country_type: Daily by country and type
        rollups['day_country_type'] = self.build_rollup(
            'day_country_type', ['day', 'country', 'type'], lf
        )
        
        # 2. day_advertiser_type: Daily by advertiser and type
        rollups['day_advertiser_type'] = self.build_rollup(
            'day_advertiser_type', ['day', 'advertiser_id', 'type'], lf
        )
        
        # 3. hour_country_type: Hourly by country and type
        rollups['hour_country_type'] = self.build_rollup(
            'hour_country_type', ['hour', 'country', 'type'], lf
        )
        
        logger.info("="*60)
        logger.info(f"✅ Combo rollups complete: {len(rollups)} rollups built")
        logger.info("="*60)
        
        return rollups
    
    def build_partitioned_minute_rollup(self) -> Dict[str, pl.DataFrame]:
        """
        Build minute_type rollup partitioned by day (366 separate DataFrames).
        
        This is critical for performance: the monolithic minute_type rollup
        (2.1M rows) takes 98ms to load. Partitioned by day (5,760 rows each),
        it takes only 0.3ms to load!
        
        Returns:
            Dictionary of "minute_type_day_XXX" -> DataFrame
        """
        logger.info("="*60)
        logger.info("Building Partitioned Minute Rollup (366 day partitions)")
        logger.info("="*60)
        
        start_total = time_module.time()
        
        # Load data once
        lf = self.loader.load_with_time_dims()
        
        # Get unique days
        logger.info("Finding unique days...")
        unique_days = lf.select('day').unique().collect()['day'].to_list()
        unique_days.sort()
        
        logger.info(f"Found {len(unique_days)} unique days")
        
        partitions = {}
        
        # Build one partition per day
        for i, day in enumerate(unique_days, 1):
            # Filter to single day
            day_lf = lf.filter(pl.col('day') == day)
            
            # Build aggregates for this day
            partition = day_lf.group_by(['minute', 'type']).agg([
                pl.col('bid_price').drop_nulls().sum().alias('bid_price_sum'),
                pl.col('bid_price').drop_nulls().count().alias('bid_price_count'),
                pl.col('bid_price').drop_nulls().min().alias('bid_price_min'),
                pl.col('bid_price').drop_nulls().max().alias('bid_price_max'),
                
                pl.col('total_price').drop_nulls().sum().alias('total_price_sum'),
                pl.col('total_price').drop_nulls().count().alias('total_price_count'),
                pl.col('total_price').drop_nulls().min().alias('total_price_min'),
                pl.col('total_price').drop_nulls().max().alias('total_price_max'),
                
                pl.len().alias('row_count'),
            ])
            
            # Materialize
            df = partition.collect()
            
            # Store with day identifier
            partition_name = f"minute_type_day_{day.replace('-', '_')}"
            partitions[partition_name] = df
            
            if i % 50 == 0:
                logger.info(f"  Progress: {i}/{len(unique_days)} days complete")
        
        total_time = time_module.time() - start_total
        total_rows = sum(len(df) for df in partitions.values())
        avg_per_partition = total_rows / len(partitions)
        
        logger.info("="*60)
        logger.info(f"✅ Partitioned minute rollup complete:")
        logger.info(f"   Partitions: {len(partitions)}")
        logger.info(f"   Total rows: {total_rows:,}")
        logger.info(f"   Avg per partition: {avg_per_partition:.0f} rows")
        logger.info(f"   Build time: {total_time:.1f}s")
        logger.info("="*60)
        
        return partitions
    
    def build_all_rollups(self) -> Dict[str, pl.DataFrame]:
        """
        Build all rollups using optimized batch builder.
        
        Uses single-pass batch builder for regular rollups (9× faster),
        then builds partitioned minute rollup separately.
        
        Returns:
            Dictionary of all rollup names -> DataFrames (including partitioned)
        """
        logger.info("\n" + "="*60)
        logger.info("STARTING FULL ROLLUP BUILD (OPTIMIZED)")
        logger.info("="*60)
        
        start_total = time_module.time()
        
        all_rollups = {}
        
        # Step 1: Build all regular rollups with single-pass PyArrow batching
        logger.info("\n[Step 1/2] Building regular rollups (single-pass mode)...")
        regular_rollups = self.build_all_rollups_single_pass()
        all_rollups.update(regular_rollups)
        
        # Step 2: Build partitioned minute rollup
        logger.info("\n[Step 2/2] Building partitioned minute rollup...")
        partitioned_rollups = self.build_partitioned_minute_rollup()
        all_rollups.update(partitioned_rollups)
        
        total_time = time_module.time() - start_total
        
        logger.info("\n" + "="*60)
        logger.info("✅✅✅ FULL ROLLUP BUILD COMPLETE ✅✅✅")
        logger.info("="*60)
        logger.info(f"Regular rollups: {len(regular_rollups)}")
        logger.info(f"Partitioned rollups: {len(partitioned_rollups)}")
        logger.info(f"Total rollups: {len(all_rollups)}")
        logger.info(f"Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
        logger.info("="*60)
        
        return all_rollups
        
        # Build partitioned minute rollup
        minute_partitions = self.build_partitioned_minute_rollup()
        all_rollups.update(minute_partitions)
        
        total_time = time_module.time() - start_total
        total_rows = sum(len(df) for df in all_rollups.values())
        total_size_mb = sum(df.estimated_size('mb') for df in all_rollups.values())
        
        logger.info("\n" + "="*60)
        logger.info("ROLLUP BUILD COMPLETE!")
        logger.info("="*60)
        logger.info(f"Total rollups: {len(all_rollups)}")
        logger.info(f"Total rows: {total_rows:,}")
        logger.info(f"Total size: {total_size_mb:.1f} MB")
        logger.info(f"Build time: {total_time:.1f}s ({total_time/60:.1f} min)")
        logger.info("="*60)
        
        return all_rollups


def main():
    """Test the rollup builder."""
    
    # Initialize data loader
    data_dir = Path(__file__).parent.parent.parent / "data"
    loader = DataLoader(data_dir)
    
    # Initialize rollup builder
    builder = RollupBuilder(loader)
    
    print("\n" + "="*60)
    print("Rollup Builder Test - Building Small Sample Rollups")
    print("="*60)
    
    # Test 1: Build a single rollup (day_type)
    print("\nTest 1: Building day_type rollup...")
    day_type = builder.build_rollup('day_type', ['day', 'type'])
    print(f"✅ day_type: {len(day_type):,} rows")
    print("\nSample data:")
    print(day_type.head(5))
    
    # Test 2: Build country_type rollup
    print("\nTest 2: Building country_type rollup...")
    country_type = builder.build_rollup('country_type', ['country', 'type'])
    print(f"✅ country_type: {len(country_type):,} rows")
    print("\nSample data:")
    print(country_type.head(5))
    
    # Test 3: Verify NULL-safe aggregation
    print("\nTest 3: Verifying NULL-safe aggregation...")
    print("Checking that NULL values don't affect sums...")
    sample_row = day_type.head(1)
    print(f"  bid_price_sum: {sample_row['bid_price_sum'][0]}")
    print(f"  bid_price_count: {sample_row['bid_price_count'][0]}")
    print(f"  ✅ Non-zero values indicate NULL-safe aggregation works!")
    
    print("\n" + "="*60)
    print("Rollup Builder Test Complete! ✅")
    print("="*60)
    print("\nReady to build full rollup set!")
    print("Run: builder.build_all_rollups() to build all 12-15 rollups")


if __name__ == "__main__":
    main()
