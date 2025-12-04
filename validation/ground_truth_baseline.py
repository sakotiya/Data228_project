#!/usr/bin/env python3
"""
Ground Truth Baseline - NYC Taxi Trip Analysis

This script computes comprehensive ground truth statistics from cleaned NYC taxi data.

Computed Metrics:
1. Temporal patterns (hourly/daily aggregates)
2. Fare distribution (percentiles, mean, std)
3. Spatial analytics (zone-based demand)
4. Trip duration statistics
5. Overall summary statistics

Output: JSON files for each metric category
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime
from collections import defaultdict
import warnings
import pyarrow.parquet as pq
import argparse
import sys

warnings.filterwarnings('ignore')


# ============================================================================
# Helper Functions
# ============================================================================

def safe_read_parquet(path: Path) -> pd.DataFrame:
    """Read parquet file with timestamp overflow handling."""
    try:
        return pd.read_parquet(path)
    except Exception as e:
        error_type = type(e).__name__
        try:
            msg = str(e)
        except Exception:
            msg = ""
        
        is_ts_error = (
            'ArrowInvalid' in error_type or
            'out of bounds' in msg.lower() or
            'timestamp' in msg.lower() or
            'casting' in msg.lower()
        )
        
        if not is_ts_error:
            raise
        
        print(f"    ⚠️  Timestamp issue for {path.name}, using safe mode...")
        table = pq.read_table(path)
        df = table.to_pandas(timestamp_as_object=True)
        
        # Convert datetime-like columns
        for col in df.columns:
            if df[col].dtype == 'object' and any(x in col.lower() for x in ['time', 'datetime', 'date']):
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except Exception:
                    pass
        return df


def compute_percentiles(series, percentiles=[0.25, 0.5, 0.75, 0.90, 0.95, 0.99]):
    """Compute percentiles for a series."""
    if len(series) == 0:
        return {f"p{int(p*100)}": None for p in percentiles}
    return {f"p{int(p*100)}": float(series.quantile(p)) for p in percentiles}


def save_json(data, filepath):
    """Save data to JSON file."""
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  ✓ Saved: {filepath}")


# ============================================================================
# Column Identification Functions
# ============================================================================

def identify_datetime_columns(df):
    """Identify datetime columns (pickup and dropoff)."""
    datetime_cols = {}
    
    # Common patterns for pickup datetime
    pickup_patterns = ['pickup_datetime', 'tpep_pickup_datetime', 'lpep_pickup_datetime', 'pickup_dt']
    for col in df.columns:
        if any(pattern in col.lower() for pattern in pickup_patterns):
            if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].dtype == 'object':
                datetime_cols['pickup'] = col
                break
    
    # Common patterns for dropoff datetime
    dropoff_patterns = ['dropoff_datetime', 'tpep_dropoff_datetime', 'lpep_dropoff_datetime', 'dropoff_dt']
    for col in df.columns:
        if any(pattern in col.lower() for pattern in dropoff_patterns):
            if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].dtype == 'object':
                datetime_cols['dropoff'] = col
                break
    
    return datetime_cols


def identify_columns(df):
    """Identify relevant columns for analysis."""
    cols = {
        'datetime': identify_datetime_columns(df),
        'fare': None,
        'distance': None,
        'passenger_count': None,
        'pu_location': None,
        'do_location': None
    }
    
    # Find fare column
    for col in df.columns:
        if 'fare' in col.lower() and 'amount' in col.lower():
            cols['fare'] = col
            break
    
    # Find distance column
    for col in df.columns:
        if 'distance' in col.lower() and 'trip' in col.lower():
            cols['distance'] = col
            break
    
    # Find passenger count
    for col in df.columns:
        if 'passenger' in col.lower():
            cols['passenger_count'] = col
            break
    
    # Find pickup location
    for col in df.columns:
        if 'pulocation' in col.lower() or ('pickup' in col.lower() and 'location' in col.lower()):
            cols['pu_location'] = col
            break
    
    # Find dropoff location
    for col in df.columns:
        if 'dolocation' in col.lower() or ('dropoff' in col.lower() and 'location' in col.lower()):
            cols['do_location'] = col
            break
    
    return cols


# ============================================================================
# Ground Truth Computation Functions
# ============================================================================

def compute_temporal_patterns(df, cols, file_type):
    """Compute hourly and daily temporal patterns."""
    results = {}
    
    pickup_col = cols['datetime'].get('pickup')
    fare_col = cols['fare']
    distance_col = cols['distance']
    
    if not pickup_col:
        return None
    
    # Ensure datetime type
    df[pickup_col] = pd.to_datetime(df[pickup_col], errors='coerce')
    df = df.dropna(subset=[pickup_col])
    
    if len(df) == 0:
        return None
    
    # Add time features
    df['hour'] = df[pickup_col].dt.hour
    df['day_of_week'] = df[pickup_col].dt.dayofweek
    df['day_name'] = df[pickup_col].dt.day_name()
    
    # Compute trip duration if both pickup and dropoff exist
    dropoff_col = cols['datetime'].get('dropoff')
    if dropoff_col:
        df[dropoff_col] = pd.to_datetime(df[dropoff_col], errors='coerce')
        df['trip_duration_min'] = (df[dropoff_col] - df[pickup_col]).dt.total_seconds() / 60.0
        df['trip_duration_min'] = df['trip_duration_min'].clip(lower=0, upper=300)  # Cap at 5 hours
    
    # HOURLY STATS
    hourly_agg = {'trip_count': ('hour', 'size')}
    if fare_col:
        hourly_agg.update({
            'avg_fare': (fare_col, 'mean'),
            'median_fare': (fare_col, 'median'),
            'std_fare': (fare_col, 'std')
        })
    if distance_col:
        hourly_agg['avg_distance'] = (distance_col, 'mean')
    if 'trip_duration_min' in df.columns:
        hourly_agg['avg_duration_min'] = ('trip_duration_min', 'mean')
    
    hourly_stats = df.groupby('hour').agg(**hourly_agg).reset_index()
    results['hourly'] = hourly_stats.to_dict(orient='records')
    
    # DAILY STATS
    daily_agg = {'trip_count': ('day_of_week', 'size')}
    if fare_col:
        daily_agg.update({
            'avg_fare': (fare_col, 'mean'),
            'total_revenue': (fare_col, 'sum')
        })
    
    daily_stats = df.groupby(['day_of_week', 'day_name']).agg(**daily_agg).reset_index()
    results['daily'] = daily_stats.to_dict(orient='records')
    
    return results


def compute_fare_distribution(df, cols):
    """Compute fare distribution statistics."""
    fare_col = cols['fare']
    
    if not fare_col or fare_col not in df.columns:
        return None
    
    fares = df[fare_col].dropna()
    
    if len(fares) == 0:
        return None
    
    stats = {
        'count': int(len(fares)),
        'mean': float(fares.mean()),
        'median': float(fares.median()),
        'std': float(fares.std()),
        'min': float(fares.min()),
        'max': float(fares.max()),
        **compute_percentiles(fares)
    }
    
    # Fare buckets (histogram)
    fare_buckets = pd.cut(fares, bins=[0, 10, 20, 30, 40, 50, 100, 500], 
                          labels=['0-10', '10-20', '20-30', '30-40', '40-50', '50-100', '100+'])
    stats['distribution'] = fare_buckets.value_counts().sort_index().to_dict()
    
    return stats


def compute_spatial_analytics(df, cols):
    """Compute zone-based demand and spatial patterns."""
    pu_col = cols['pu_location']
    do_col = cols['do_location']
    fare_col = cols['fare']
    
    results = {}
    
    # Pickup zone demand
    if pu_col and pu_col in df.columns:
        pu_agg = {'pickup_count': (pu_col, 'size')}
        if fare_col:
            pu_agg['avg_fare_from_zone'] = (fare_col, 'mean')
        
        zone_demand = df.groupby(pu_col).agg(**pu_agg).reset_index()
        zone_demand = zone_demand.sort_values('pickup_count', ascending=False).head(50)
        results['top_pickup_zones'] = zone_demand.to_dict(orient='records')
    
    # Dropoff zone demand
    if do_col and do_col in df.columns:
        do_demand = df.groupby(do_col).size().reset_index(name='dropoff_count')
        do_demand = do_demand.sort_values('dropoff_count', ascending=False).head(50)
        results['top_dropoff_zones'] = do_demand.to_dict(orient='records')
    
    # OD pairs (Origin-Destination)
    if pu_col and do_col and pu_col in df.columns and do_col in df.columns:
        od_pairs = df.groupby([pu_col, do_col]).size().reset_index(name='trip_count')
        od_pairs = od_pairs.sort_values('trip_count', ascending=False).head(100)
        results['top_od_pairs'] = od_pairs.to_dict(orient='records')
    
    return results if results else None


def compute_duration_stats(df, cols):
    """Compute trip duration statistics."""
    pickup_col = cols['datetime'].get('pickup')
    dropoff_col = cols['datetime'].get('dropoff')
    
    if not pickup_col or not dropoff_col:
        return None
    
    df[pickup_col] = pd.to_datetime(df[pickup_col], errors='coerce')
    df[dropoff_col] = pd.to_datetime(df[dropoff_col], errors='coerce')
    
    df['trip_duration_min'] = (df[dropoff_col] - df[pickup_col]).dt.total_seconds() / 60.0
    
    # Filter valid durations (0 to 300 minutes)
    durations = df['trip_duration_min'].dropna()
    durations = durations[(durations > 0) & (durations <= 300)]
    
    if len(durations) == 0:
        return None
    
    stats = {
        'count': int(len(durations)),
        'mean_duration_min': float(durations.mean()),
        'median_duration_min': float(durations.median()),
        'std_duration_min': float(durations.std()),
        'min_duration_min': float(durations.min()),
        'max_duration_min': float(durations.max()),
        **compute_percentiles(durations)
    }
    
    return stats


def compute_summary_stats(df, cols, file_name):
    """Compute overall summary statistics."""
    stats = {
        'file_name': file_name,
        'total_trips': len(df)
    }
    
    # Unique zones
    if cols['pu_location'] and cols['pu_location'] in df.columns:
        stats['unique_pickup_zones'] = int(df[cols['pu_location']].nunique())
    if cols['do_location'] and cols['do_location'] in df.columns:
        stats['unique_dropoff_zones'] = int(df[cols['do_location']].nunique())
    
    # Revenue and distance
    if cols['fare'] and cols['fare'] in df.columns:
        stats['total_revenue'] = float(df[cols['fare']].sum())
        stats['avg_fare'] = float(df[cols['fare']].mean())
    
    if cols['distance'] and cols['distance'] in df.columns:
        stats['total_distance'] = float(df[cols['distance']].sum())
        stats['avg_distance'] = float(df[cols['distance']].mean())
    
    # Passenger count
    if cols['passenger_count'] and cols['passenger_count'] in df.columns:
        stats['avg_passenger_count'] = float(df[cols['passenger_count']].mean())
    
    return stats


# ============================================================================
# Main Processing Function
# ============================================================================

def process_file(file_path, results_dir):
    """Process a single parquet file and compute all ground truth metrics."""
    file_name = file_path.name
    
    # Determine file type
    if 'yellow' in file_name:
        file_type = 'yellow'
    elif 'green' in file_name:
        file_type = 'green'
    elif 'fhvhv' in file_name:
        file_type = 'fhvhv'
    elif 'fhv' in file_name:
        file_type = 'fhv'
    else:
        file_type = 'unknown'
    
    print(f"\n📊 Processing: {file_name} (type: {file_type})")
    
    # Read file
    try:
        df = safe_read_parquet(file_path)
        print(f"  Rows: {len(df):,}, Cols: {len(df.columns)}")
    except Exception as e:
        print(f"  ❌ Error reading file: {e}")
        return None
    
    if len(df) == 0:
        print(f"  ⚠️  Empty dataframe, skipping...")
        return None
    
    # Identify columns
    cols = identify_columns(df)
    print(f"  Identified columns:")
    print(f"    Pickup: {cols['datetime'].get('pickup')}")
    print(f"    Fare: {cols['fare']}")
    print(f"    Distance: {cols['distance']}")
    print(f"    PU Location: {cols['pu_location']}")
    
    # Compute metrics
    results = {
        'file_name': file_name,
        'file_type': file_type,
        'processed_at': datetime.now().isoformat()
    }
    
    # 1. Temporal patterns
    temporal = compute_temporal_patterns(df.copy(), cols, file_type)
    if temporal:
        results['temporal_patterns'] = temporal
        print(f"  ✓ Temporal patterns computed")
    
    # 2. Fare distribution
    fare_dist = compute_fare_distribution(df.copy(), cols)
    if fare_dist:
        results['fare_distribution'] = fare_dist
        print(f"  ✓ Fare distribution computed")
    
    # 3. Spatial analytics
    spatial = compute_spatial_analytics(df.copy(), cols)
    if spatial:
        results['spatial_analytics'] = spatial
        print(f"  ✓ Spatial analytics computed")
    
    # 4. Duration stats
    duration = compute_duration_stats(df.copy(), cols)
    if duration:
        results['duration_stats'] = duration
        print(f"  ✓ Duration stats computed")
    
    # 5. Summary
    summary = compute_summary_stats(df.copy(), cols, file_name)
    results['summary'] = summary
    print(f"  ✓ Summary stats computed")
    
    # Save individual file result
    output_file = f"{results_dir}/summary/{file_path.stem}_ground_truth.json"
    save_json(results, output_file)
    
    return results


# ============================================================================
# Aggregation Functions
# ============================================================================

def aggregate_results_by_type(file_type_groups, results_dir):
    """Aggregate results by file type."""
    print(f"\n{'='*80}")
    print("AGGREGATING RESULTS BY FILE TYPE")
    print(f"{'='*80}")
    
    for file_type, results in file_type_groups.items():
        print(f"\n📈 Aggregating {file_type.upper()} files ({len(results)} files)...")
        
        # Aggregate hourly stats
        all_hourly = []
        for r in results:
            if 'temporal_patterns' in r and 'hourly' in r['temporal_patterns']:
                all_hourly.extend(r['temporal_patterns']['hourly'])
        
        if all_hourly:
            hourly_df = pd.DataFrame(all_hourly)
            
            # Build aggregation dict based on available columns
            agg_dict = {'trip_count': 'sum'}
            if 'avg_fare' in hourly_df.columns:
                agg_dict['avg_fare'] = 'mean'
            if 'avg_distance' in hourly_df.columns:
                agg_dict['avg_distance'] = 'mean'
            if 'avg_duration_min' in hourly_df.columns:
                agg_dict['avg_duration_min'] = 'mean'
            
            agg_hourly = hourly_df.groupby('hour').agg(agg_dict).reset_index()
            
            output_file = f"{results_dir}/hourly_stats/{file_type}_aggregated.json"
            save_json(agg_hourly.to_dict(orient='records'), output_file)
        
        # Aggregate summary stats
        summaries = [r['summary'] for r in results if 'summary' in r]
        if summaries:
            total_summary = {
                'file_type': file_type,
                'total_files': len(summaries),
                'total_trips': sum(s.get('total_trips', 0) for s in summaries)
            }
            
            # Add optional fields only if they exist
            revenue_values = [s.get('total_revenue', 0) for s in summaries if 'total_revenue' in s]
            if revenue_values:
                total_summary['total_revenue'] = sum(revenue_values)
            
            distance_values = [s.get('total_distance', 0) for s in summaries if 'total_distance' in s]
            if distance_values:
                total_summary['total_distance'] = sum(distance_values)
            
            avg_fare_values = [s.get('avg_fare') for s in summaries if 'avg_fare' in s]
            if avg_fare_values:
                total_summary['avg_fare'] = np.mean(avg_fare_values)
            
            avg_distance_values = [s.get('avg_distance') for s in summaries if 'avg_distance' in s]
            if avg_distance_values:
                total_summary['avg_distance'] = np.mean(avg_distance_values)
            
            output_file = f"{results_dir}/summary/{file_type}_overall_summary.json"
            save_json(total_summary, output_file)
            
            print(f"  Total trips: {total_summary['total_trips']:,}")
            if 'total_revenue' in total_summary:
                print(f"  Total revenue: ${total_summary['total_revenue']:,.2f}")
            if 'avg_fare' in total_summary:
                print(f"  Avg fare: ${total_summary['avg_fare']:.2f}")
    
    print(f"\n✅ Aggregation complete!")


def create_master_index(all_results, file_type_groups, results_dir):
    """Create master index of all results."""
    master_index = {
        'generated_at': datetime.now().isoformat(),
        'total_files_processed': len(all_results),
        'file_types': {ft: len(results) for ft, results in file_type_groups.items()},
        'output_structure': {
            'hourly_stats': 'Aggregated hourly statistics by file type',
            'daily_stats': 'Aggregated daily statistics by file type',
            'fare_distribution': 'Fare distribution metrics',
            'zone_demand': 'Spatial analytics and zone-based demand',
            'duration_stats': 'Trip duration statistics',
            'summary': 'Individual file summaries and overall aggregates'
        },
        'files': [{
            'file_name': r['file_name'],
            'file_type': r['file_type'],
            'total_trips': r['summary']['total_trips'],
            'has_temporal': 'temporal_patterns' in r,
            'has_fare': 'fare_distribution' in r,
            'has_spatial': 'spatial_analytics' in r,
            'has_duration': 'duration_stats' in r
        } for r in all_results]
    }
    
    master_index_file = f"{results_dir}/MASTER_INDEX.json"
    save_json(master_index, master_index_file)
    
    return master_index_file


# ============================================================================
# Main Function
# ============================================================================

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Compute ground truth baseline statistics from cleaned NYC taxi data'
    )
    parser.add_argument(
        '--input-dir',
        type=str,
        default='/Users/vidushi/Documents/bubu/cleaned_data',
        help='Directory containing cleaned parquet files (default: /Users/vidushi/Documents/bubu/cleaned_data)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='/Users/vidushi/Documents/bubu/ground_truth_results',
        help='Directory to save results (default: /Users/vidushi/Documents/bubu/ground_truth_results)'
    )
    
    args = parser.parse_args()
    
    # Setup directories
    cleaned_data_dir = Path(args.input_dir)
    results_dir = Path(args.output_dir)
    
    if not cleaned_data_dir.exists():
        print(f"❌ Error: Input directory does not exist: {cleaned_data_dir}")
        sys.exit(1)
    
    # Create results directory structure
    results_dir.mkdir(exist_ok=True)
    for subdir in ['hourly_stats', 'daily_stats', 'fare_distribution', 'zone_demand', 'duration_stats', 'summary']:
        (results_dir / subdir).mkdir(exist_ok=True)
    
    print("✓ Directories setup complete")
    print(f"  Cleaned data: {cleaned_data_dir}")
    print(f"  Results: {results_dir}")
    
    # Get all cleaned parquet files
    cleaned_files = sorted(cleaned_data_dir.glob("*.parquet"))
    
    print(f"\n{'='*80}")
    print(f"GROUND TRUTH BASELINE COMPUTATION")
    print(f"{'='*80}")
    print(f"Total files: {len(cleaned_files)}")
    print(f"Output directory: {results_dir}")
    print(f"{'='*80}")
    
    if len(cleaned_files) == 0:
        print(f"❌ No parquet files found in {cleaned_data_dir}")
        sys.exit(1)
    
    # Process all files
    all_results = []
    file_type_groups = defaultdict(list)
    
    for idx, file_path in enumerate(cleaned_files, 1):
        print(f"\n[{idx}/{len(cleaned_files)}] {file_path.name}")
        
        result = process_file(file_path, results_dir)
        
        if result:
            all_results.append(result)
            file_type = result['file_type']
            file_type_groups[file_type].append(result)
    
    print(f"\n{'='*80}")
    print(f"✅ PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"Successfully processed: {len(all_results)}/{len(cleaned_files)} files")
    print(f"\nFiles by type:")
    for file_type, results in file_type_groups.items():
        print(f"  {file_type}: {len(results)} files")
    
    # Aggregate results by type
    aggregate_results_by_type(file_type_groups, results_dir)
    
    # Create master index
    master_index_file = create_master_index(all_results, file_type_groups, results_dir)
    
    print(f"\n{'='*80}")
    print("✅ GROUND TRUTH BASELINE COMPUTATION COMPLETE")
    print(f"{'='*80}")
    print(f"\nResults saved to: {results_dir}")
    print(f"\nTo view master index:")
    print(f"  cat {master_index_file}")
    print(f"\nDirectory structure:")
    print(f"  {results_dir}/")
    print(f"    ├── hourly_stats/        # Hourly aggregated metrics")
    print(f"    ├── daily_stats/         # Daily aggregated metrics")
    print(f"    ├── fare_distribution/   # Fare percentiles and distributions")
    print(f"    ├── zone_demand/         # Spatial analytics")
    print(f"    ├── duration_stats/      # Trip duration statistics")
    print(f"    ├── summary/             # Individual and overall summaries")
    print(f"    └── MASTER_INDEX.json    # Master catalog of all results")


if __name__ == "__main__":
    main()

