#!/usr/bin/env python3
"""
StreamTide Ground Truth Baseline - AWS EMR Version (Matches Local Notebook)
Processes each file individually, handles different schemas, creates same output structure as notebook

Matches: /Users/shreya/SJSU-Github/DA228/scripts/GroundTruthMR/ground_truth_baseline.ipynb
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("StreamTide-GroundTruth-PerFile") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def identify_columns(df):
    """
    Identify relevant columns (same as notebook)
    Handles both old and new schema formats
    """
    columns = [c.lower() for c in df.columns]
    col_map = {
        'pickup_datetime': None,
        'dropoff_datetime': None,
        'fare': None,
        'distance': None,
        'passenger_count': None,
        'pu_location': None,
        'do_location': None
    }

    # Find pickup datetime (handles multiple formats)
    for col in df.columns:
        col_lower = col.lower()
        if any(p in col_lower for p in ['pickup_datetime', 'tpep_pickup', 'lpep_pickup', 'trip_pickup']):
            col_map['pickup_datetime'] = col
            break

    # Find dropoff datetime
    for col in df.columns:
        col_lower = col.lower()
        if any(p in col_lower for p in ['dropoff_datetime', 'tpep_dropoff', 'lpep_dropoff', 'trip_dropoff']):
            col_map['dropoff_datetime'] = col
            break

    # Find fare (multiple patterns)
    for col in df.columns:
        col_lower = col.lower()
        if ('fare' in col_lower and 'amount' in col_lower) or col_lower == 'fare_amt':
            col_map['fare'] = col
            break

    # Find distance
    for col in df.columns:
        col_lower = col.lower()
        if 'distance' in col_lower and 'trip' in col_lower:
            col_map['distance'] = col
            break

    # Find passenger count
    for col in df.columns:
        col_lower = col.lower()
        if 'passenger' in col_lower:
            col_map['passenger_count'] = col
            break

    # Find pickup location
    for col in df.columns:
        col_lower = col.lower()
        if 'pulocation' in col_lower or col_lower == 'start_lon':
            col_map['pu_location'] = col
            break

    # Find dropoff location
    for col in df.columns:
        col_lower = col.lower()
        if 'dolocation' in col_lower or col_lower == 'end_lon':
            col_map['do_location'] = col
            break

    return col_map

def compute_file_metrics(df, col_map, file_name):
    """
    Compute all metrics for a single file (same as notebook)
    Returns dict with all computed metrics
    """
    results = {
        'file_name': file_name,
        'total_trips': df.count(),
        'columns_found': {k: v for k, v in col_map.items() if v is not None}
    }

    # Temporal patterns (hourly)
    pickup_col = col_map['pickup_datetime']
    if pickup_col:
        try:
            # Ensure datetime type
            df_temporal = df.withColumn('_pickup', to_timestamp(col(pickup_col)))
            df_temporal = df_temporal.withColumn('hour', hour('_pickup'))

            # Build hourly aggregation
            hourly_agg = [count('*').alias('trip_count')]

            if col_map['fare']:
                hourly_agg.extend([
                    mean(col_map['fare']).alias('avg_fare'),
                    expr(f'percentile_approx({col_map["fare"]}, 0.5)').alias('median_fare'),
                    stddev(col_map['fare']).alias('std_fare')
                ])

            if col_map['distance']:
                hourly_agg.append(mean(col_map['distance']).alias('avg_distance'))

            hourly_stats = df_temporal.groupBy('hour').agg(*hourly_agg).orderBy('hour')
            results['hourly_stats'] = [row.asDict() for row in hourly_stats.collect()]
        except Exception as e:
            print(f"    ⚠️  Could not compute temporal patterns: {e}")

    # Fare distribution
    if col_map['fare']:
        try:
            fare_stats = df.select(
                count(col_map['fare']).alias('count'),
                mean(col_map['fare']).alias('mean'),
                stddev(col_map['fare']).alias('std'),
                min(col_map['fare']).alias('min'),
                max(col_map['fare']).alias('max'),
                expr(f'percentile_approx({col_map["fare"]}, array(0.25, 0.5, 0.75, 0.95, 0.99))').alias('percentiles')
            ).collect()[0]

            results['fare_distribution'] = fare_stats.asDict()
        except Exception as e:
            print(f"    ⚠️  Could not compute fare distribution: {e}")

    # Spatial analytics (top zones)
    if col_map['pu_location']:
        try:
            pu_agg = [count('*').alias('pickup_count')]
            if col_map['fare']:
                pu_agg.append(mean(col_map['fare']).alias('avg_fare_from_zone'))

            top_zones = df.groupBy(col_map['pu_location']).agg(*pu_agg) \
                          .orderBy(desc('pickup_count')) \
                          .limit(50)

            results['top_pickup_zones'] = [row.asDict() for row in top_zones.collect()]
        except Exception as e:
            print(f"    ⚠️  Could not compute spatial analytics: {e}")

    # Summary stats
    if col_map['fare']:
        try:
            results['total_revenue'] = df.select(sum(col_map['fare'])).collect()[0][0]
            results['avg_fare'] = df.select(mean(col_map['fare'])).collect()[0][0]
        except:
            pass

    if col_map['distance']:
        try:
            results['total_distance'] = df.select(sum(col_map['distance'])).collect()[0][0]
            results['avg_distance'] = df.select(mean(col_map['distance'])).collect()[0][0]
        except:
            pass

    return results

def process_single_file(spark, file_path, output_base):
    """Process a single parquet file (same as notebook per-file processing)"""

    # Extract file name
    file_name = file_path.split('/')[-1]

    # Determine file type
    if 'yellow' in file_name.lower():
        file_type = 'yellow'
    elif 'green' in file_name.lower():
        file_type = 'green'
    elif 'fhvhv' in file_name.lower():
        file_type = 'fhvhv'
    elif 'fhv' in file_name.lower():
        file_type = 'fhv'
    else:
        file_type = 'unknown'

    print(f"  📊 Processing: {file_name} (type: {file_type})")

    try:
        # Read single file (no schema merging issues)
        df = spark.read.parquet(file_path)

        row_count = df.count()
        print(f"      Rows: {row_count:,}, Cols: {len(df.columns)}")

        if row_count == 0:
            print(f"      ⚠️  Empty file, skipping...")
            return None

        # Identify columns for THIS file's schema
        col_map = identify_columns(df)
        print(f"      Columns: pickup={col_map['pickup_datetime']}, fare={col_map['fare']}")

        # Compute metrics
        results = compute_file_metrics(df, col_map, file_name)
        results['file_type'] = file_type
        results['processed_at'] = datetime.now().isoformat()

        # Save per-file result (same structure as notebook)
        file_base_name = file_name.replace('.parquet', '')
        output_path = f"{output_base}/summary/{file_type}/{file_base_name}_ground_truth.json"

        # Convert to JSON string
        results_json = json.dumps(results, indent=2, default=str)

        # Save to S3 using Spark
        spark.sparkContext.parallelize([results_json]).saveAsTextFile(output_path)

        print(f"      ✓ Saved to {output_path}")

        return results

    except Exception as e:
        print(f"      ❌ Error processing {file_name}: {e}")
        return None

def aggregate_by_type(spark, all_results, output_base):
    """Aggregate results by file type (same as notebook)"""

    print(f"\n{'='*80}")
    print("AGGREGATING RESULTS BY FILE TYPE")
    print(f"{'='*80}\n")

    # Group results by type
    by_type = {}
    for result in all_results:
        if result:
            ftype = result['file_type']
            if ftype not in by_type:
                by_type[ftype] = []
            by_type[ftype].append(result)

    # Aggregate each type
    for file_type, results in by_type.items():
        print(f"📈 Aggregating {file_type.upper()} ({len(results)} files)...")

        # Aggregate hourly stats
        all_hourly = []
        for r in results:
            if 'hourly_stats' in r:
                all_hourly.extend(r['hourly_stats'])

        if all_hourly:
            hourly_df = spark.createDataFrame(all_hourly)

            # Build aggregation
            agg_exprs = [sum('trip_count').alias('trip_count')]
            if 'avg_fare' in hourly_df.columns:
                agg_exprs.append(mean('avg_fare').alias('avg_fare'))
            if 'avg_distance' in hourly_df.columns:
                agg_exprs.append(mean('avg_distance').alias('avg_distance'))

            agg_hourly = hourly_df.groupBy('hour').agg(*agg_exprs).orderBy('hour')

            # Save aggregated hourly
            hourly_json = [row.asDict() for row in agg_hourly.collect()]
            hourly_str = json.dumps(hourly_json, indent=2, default=str)
            spark.sparkContext.parallelize([hourly_str]).saveAsTextFile(
                f"{output_base}/hourly_stats/{file_type}_aggregated"
            )
            print(f"  ✓ Hourly stats aggregated")

        # Aggregate summary (use Python's built-in sum, not PySpark sum)
        import builtins

        total_summary = {
            'file_type': file_type,
            'total_files': len(results),
            'total_trips': builtins.sum(r.get('total_trips', 0) for r in results)
        }

        # Add optional fields
        revenues = [r.get('total_revenue', 0) for r in results if 'total_revenue' in r]
        if revenues:
            total_summary['total_revenue'] = builtins.sum(revenues)

        distances = [r.get('total_distance', 0) for r in results if 'total_distance' in r]
        if distances:
            total_summary['total_distance'] = builtins.sum(distances)

        avg_fares = [r.get('avg_fare') for r in results if 'avg_fare' in r]
        if avg_fares:
            total_summary['avg_fare'] = builtins.sum(avg_fares) / len(avg_fares)

        # Save overall summary
        summary_str = json.dumps(total_summary, indent=2, default=str)
        spark.sparkContext.parallelize([summary_str]).saveAsTextFile(
            f"{output_base}/summary/{file_type}_overall_summary"
        )

        print(f"  Total trips: {total_summary['total_trips']:,}")
        if 'total_revenue' in total_summary:
            print(f"  Total revenue: ${total_summary['total_revenue']:,.2f}")
        if 'avg_fare' in total_summary:
            print(f"  Avg fare: ${total_summary['avg_fare']:.2f}")
        print(f"  ✓ Overall summary saved\n")

def create_master_index(spark, all_results, output_base):
    """Create MASTER_INDEX.json (same as notebook)"""

    master_index = {
        'generated_at': datetime.now().isoformat(),
        'total_files_processed': len([r for r in all_results if r]),
        'file_types': {},
        'files': []
    }

    # Count by type
    for result in all_results:
        if result:
            ftype = result['file_type']
            master_index['file_types'][ftype] = master_index['file_types'].get(ftype, 0) + 1

            master_index['files'].append({
                'file_name': result['file_name'],
                'file_type': result['file_type'],
                'total_trips': result['total_trips'],
                'has_temporal': 'hourly_stats' in result,
                'has_fare': 'fare_distribution' in result,
                'has_spatial': 'top_pickup_zones' in result
            })

    # Save MASTER_INDEX
    index_str = json.dumps(master_index, indent=2, default=str)
    spark.sparkContext.parallelize([index_str]).saveAsTextFile(
        f"{output_base}/MASTER_INDEX"
    )

    print(f"\n✓ MASTER_INDEX.json created")

def main():
    """Main execution - matches notebook workflow"""

    INPUT_BASE = "s3://data228-bigdata-nyc/staging"
    OUTPUT_BASE = "s3://data228-bigdata-nyc/ground-truth"

    print(f"\n{'='*80}")
    print("STREAMTIDE GROUND TRUTH BASELINE - PER-FILE PROCESSING")
    print("(Matches local notebook behavior)")
    print(f"{'='*80}")
    print(f"Input: {INPUT_BASE}")
    print(f"Output: {OUTPUT_BASE}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"{'='*80}\n")

    spark = create_spark_session()

    # Get list of all parquet files
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jvm.java.net.URI(INPUT_BASE), hadoop_conf
    )

    file_status = fs.listStatus(
        spark.sparkContext._jvm.org.apache.hadoop.fs.Path(INPUT_BASE)
    )

    # Filter parquet files
    parquet_files = []
    for status in file_status:
        path_str = str(status.getPath())
        if path_str.endswith('.parquet'):
            parquet_files.append(path_str)

    print(f"Found {len(parquet_files)} parquet files\n")

    # Process each file individually (SAME AS NOTEBOOK)
    all_results = []
    for idx, file_path in enumerate(sorted(parquet_files), 1):
        print(f"[{idx}/{len(parquet_files)}]")
        result = process_single_file(spark, file_path, OUTPUT_BASE)
        if result:
            all_results.append(result)

    # Aggregate by type (SAME AS NOTEBOOK)
    aggregate_by_type(spark, all_results, OUTPUT_BASE)

    # Create MASTER_INDEX (SAME AS NOTEBOOK)
    create_master_index(spark, all_results, OUTPUT_BASE)

    print(f"\n{'='*80}")
    print("✅ GROUND TRUTH COMPUTATION COMPLETE")
    print(f"{'='*80}")
    print(f"\nProcessed: {len(all_results)}/{len(parquet_files)} files")
    print(f"\nResults saved to: {OUTPUT_BASE}/")
    print(f"\nDirectory structure (same as notebook):")
    print(f"  {OUTPUT_BASE}/")
    print(f"    ├── summary/")
    print(f"    │   ├── yellow/")
    print(f"    │   │   ├── yellow_tripdata_2020-01_ground_truth.json")
    print(f"    │   │   └── ...")
    print(f"    │   ├── yellow_overall_summary.json")
    print(f"    │   └── ...")
    print(f"    ├── hourly_stats/")
    print(f"    │   ├── yellow_aggregated.json")
    print(f"    │   └── ...")
    print(f"    └── MASTER_INDEX.json")
    print(f"\n{'='*80}\n")

    spark.stop()

if __name__ == "__main__":
    main()
