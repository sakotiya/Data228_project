#!/usr/bin/env python3
"""
StreamTide Ground Truth Baseline - AWS EMR Version
Replicates the local ground_truth_baseline.ipynb logic using PySpark on S3 data

Input: s3://data228-bigdata-nyc/staging/{yellow,green,fhv,fhvhv}/*.parquet
Output: s3://data228-bigdata-nyc/ground-truth/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
from datetime import datetime

def create_spark_session():
    """Create Spark session optimized for S3 access"""
    return SparkSession.builder \
        .appName("StreamTide-GroundTruth-AWS") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .getOrCreate()

def identify_columns(df):
    """
    Identify relevant columns (same logic as local notebook)
    Returns dict with column names for: pickup_datetime, dropoff_datetime,
    fare, distance, passenger_count, pu_location, do_location
    """
    columns = df.columns
    col_map = {
        'pickup_datetime': None,
        'dropoff_datetime': None,
        'fare': None,
        'distance': None,
        'passenger_count': None,
        'pu_location': None,
        'do_location': None
    }

    # Find pickup datetime
    for col in columns:
        if any(p in col.lower() for p in ['pickup_datetime', 'tpep_pickup', 'lpep_pickup']):
            col_map['pickup_datetime'] = col
            break

    # Find dropoff datetime
    for col in columns:
        if any(p in col.lower() for p in ['dropoff_datetime', 'tpep_dropoff', 'lpep_dropoff']):
            col_map['dropoff_datetime'] = col
            break

    # Find fare
    for col in columns:
        if 'fare' in col.lower() and 'amount' in col.lower():
            col_map['fare'] = col
            break

    # Find distance
    for col in columns:
        if 'distance' in col.lower() and 'trip' in col.lower():
            col_map['distance'] = col
            break

    # Find passenger count
    for col in columns:
        if 'passenger' in col.lower():
            col_map['passenger_count'] = col
            break

    # Find pickup location
    for col in columns:
        if 'pulocation' in col.lower():
            col_map['pu_location'] = col
            break

    # Find dropoff location
    for col in columns:
        if 'dolocation' in col.lower():
            col_map['do_location'] = col
            break

    return col_map

def compute_temporal_patterns(df, col_map):
    """
    Compute hourly and daily temporal patterns
    (PySpark version of local notebook logic)
    """
    pickup_col = col_map['pickup_datetime']
    if not pickup_col:
        return None

    # Add time features
    df_temporal = df.withColumn('hour', hour(col(pickup_col))) \
                    .withColumn('day_of_week', dayofweek(col(pickup_col))) \
                    .withColumn('day_name', date_format(col(pickup_col), 'EEEE'))

    # Add trip duration if both timestamps exist
    if col_map['dropoff_datetime']:
        df_temporal = df_temporal.withColumn(
            'trip_duration_min',
            (unix_timestamp(col(col_map['dropoff_datetime'])) -
             unix_timestamp(col(pickup_col))) / 60.0
        ).withColumn(
            'trip_duration_min',
            when((col('trip_duration_min') > 0) & (col('trip_duration_min') <= 300),
                 col('trip_duration_min'))
            .otherwise(None)
        )

    # Build hourly aggregation (same metrics as local notebook)
    hourly_agg = [count('*').alias('trip_count')]

    if col_map['fare']:
        hourly_agg.extend([
            mean(col_map['fare']).alias('avg_fare'),
            expr(f'percentile_approx({col_map["fare"]}, 0.5)').alias('median_fare'),
            stddev(col_map['fare']).alias('std_fare')
        ])

    if col_map['distance']:
        hourly_agg.append(mean(col_map['distance']).alias('avg_distance'))

    if col_map['dropoff_datetime']:
        hourly_agg.append(mean('trip_duration_min').alias('avg_duration_min'))

    # Compute hourly stats
    hourly_stats = df_temporal.groupBy('hour').agg(*hourly_agg).orderBy('hour')

    # Build daily aggregation
    daily_agg = [count('*').alias('trip_count')]
    if col_map['fare']:
        daily_agg.extend([
            mean(col_map['fare']).alias('avg_fare'),
            sum(col_map['fare']).alias('total_revenue')
        ])

    daily_stats = df_temporal.groupBy('day_of_week', 'day_name') \
                             .agg(*daily_agg) \
                             .orderBy('day_of_week')

    return {
        'hourly': hourly_stats,
        'daily': daily_stats
    }

def compute_fare_distribution(df, col_map):
    """
    Compute fare distribution (percentiles, buckets)
    (PySpark version of local notebook logic)
    """
    fare_col = col_map['fare']
    if not fare_col:
        return None

    # Overall statistics
    fare_stats = df.select(
        count(fare_col).alias('count'),
        mean(fare_col).alias('mean'),
        expr(f'percentile_approx({fare_col}, 0.5)').alias('median'),
        stddev(fare_col).alias('std'),
        min(fare_col).alias('min'),
        max(fare_col).alias('max'),
        expr(f'percentile_approx({fare_col}, 0.25)').alias('p25'),
        expr(f'percentile_approx({fare_col}, 0.75)').alias('p75'),
        expr(f'percentile_approx({fare_col}, 0.90)').alias('p90'),
        expr(f'percentile_approx({fare_col}, 0.95)').alias('p95'),
        expr(f'percentile_approx({fare_col}, 0.99)').alias('p99')
    )

    # Fare buckets (same buckets as local notebook)
    fare_buckets = df.withColumn(
        'fare_bucket',
        when(col(fare_col) <= 10, '0-10')
        .when(col(fare_col) <= 20, '10-20')
        .when(col(fare_col) <= 30, '20-30')
        .when(col(fare_col) <= 40, '30-40')
        .when(col(fare_col) <= 50, '40-50')
        .when(col(fare_col) <= 100, '50-100')
        .otherwise('100+')
    ).groupBy('fare_bucket').count().orderBy('fare_bucket')

    return {
        'stats': fare_stats,
        'buckets': fare_buckets
    }

def compute_spatial_analytics(df, col_map):
    """
    Compute zone demand and OD pairs (top 50 pickup, top 50 dropoff, top 100 OD pairs)
    (PySpark version of local notebook logic)
    """
    pu_col = col_map['pu_location']
    do_col = col_map['do_location']
    fare_col = col_map['fare']

    results = {}

    # Top 50 pickup zones
    if pu_col:
        pu_agg = [count('*').alias('pickup_count')]
        if fare_col:
            pu_agg.append(mean(fare_col).alias('avg_fare_from_zone'))

        results['top_pickup_zones'] = df.groupBy(pu_col).agg(*pu_agg) \
                                        .orderBy(desc('pickup_count')) \
                                        .limit(50)

    # Top 50 dropoff zones
    if do_col:
        results['top_dropoff_zones'] = df.groupBy(do_col) \
                                         .count() \
                                         .withColumnRenamed('count', 'dropoff_count') \
                                         .orderBy(desc('dropoff_count')) \
                                         .limit(50)

    # Top 100 OD pairs
    if pu_col and do_col:
        results['top_od_pairs'] = df.groupBy(pu_col, do_col) \
                                    .count() \
                                    .withColumnRenamed('count', 'trip_count') \
                                    .orderBy(desc('trip_count')) \
                                    .limit(100)

    return results if results else None

def compute_duration_stats(df, col_map):
    """
    Compute trip duration statistics (0-300 min range)
    (PySpark version of local notebook logic)
    """
    pickup_col = col_map['pickup_datetime']
    dropoff_col = col_map['dropoff_datetime']

    if not pickup_col or not dropoff_col:
        return None

    # Calculate duration and filter valid range
    df_duration = df.withColumn(
        'trip_duration_min',
        (unix_timestamp(col(dropoff_col)) - unix_timestamp(col(pickup_col))) / 60.0
    ).filter((col('trip_duration_min') > 0) & (col('trip_duration_min') <= 300))

    duration_stats = df_duration.select(
        count('trip_duration_min').alias('count'),
        mean('trip_duration_min').alias('mean_duration_min'),
        expr('percentile_approx(trip_duration_min, 0.5)').alias('median_duration_min'),
        stddev('trip_duration_min').alias('std_duration_min'),
        min('trip_duration_min').alias('min_duration_min'),
        max('trip_duration_min').alias('max_duration_min'),
        expr('percentile_approx(trip_duration_min, 0.25)').alias('p25'),
        expr('percentile_approx(trip_duration_min, 0.75)').alias('p75'),
        expr('percentile_approx(trip_duration_min, 0.95)').alias('p95')
    )

    return duration_stats

def compute_summary_stats(df, col_map):
    """
    Compute overall summary (same as local notebook)
    """
    stats_exprs = [count('*').alias('total_trips')]

    if col_map['pu_location']:
        stats_exprs.append(countDistinct(col_map['pu_location']).alias('unique_pickup_zones'))

    if col_map['do_location']:
        stats_exprs.append(countDistinct(col_map['do_location']).alias('unique_dropoff_zones'))

    if col_map['fare']:
        stats_exprs.extend([
            sum(col_map['fare']).alias('total_revenue'),
            mean(col_map['fare']).alias('avg_fare')
        ])

    if col_map['distance']:
        stats_exprs.extend([
            sum(col_map['distance']).alias('total_distance'),
            mean(col_map['distance']).alias('avg_distance')
        ])

    if col_map['passenger_count']:
        stats_exprs.append(mean(col_map['passenger_count']).alias('avg_passenger_count'))

    return df.select(*stats_exprs)

def process_taxi_type(spark, file_type, input_base, output_base):
    """
    Process all files for a given taxi type (yellow, green, fhv, fhvhv)
    Replicates the per-file-type aggregation from local notebook
    """
    print(f"\n{'='*80}")
    print(f"Processing {file_type.upper()} taxi data")
    print(f"{'='*80}\n")

    # Updated to match actual S3 structure: all files in staging root
    input_path = f"{input_base}/{file_type}_tripdata_*.parquet"

    try:
        # Read with schema merging to handle different data types across files
        df = spark.read \
            .option("mergeSchema", "true") \
            .option("parquet.vectorized.reader.enabled", "false") \
            .parquet(input_path)
    except Exception as e:
        print(f"⚠️  No data found for {file_type}: {e}")
        return None

    row_count = df.count()
    print(f"Total rows: {row_count:,}")
    print(f"Total columns: {len(df.columns)}")

    if row_count == 0:
        print(f"⚠️  Empty dataset for {file_type}, skipping...")
        return None

    # Identify columns
    col_map = identify_columns(df)
    print(f"\nIdentified columns:")
    print(f"  Pickup datetime: {col_map['pickup_datetime']}")
    print(f"  Dropoff datetime: {col_map['dropoff_datetime']}")
    print(f"  Fare: {col_map['fare']}")
    print(f"  Distance: {col_map['distance']}")
    print(f"  PU Location: {col_map['pu_location']}")
    print(f"  DO Location: {col_map['do_location']}")

    # Compute all metrics
    print(f"\nComputing metrics...")

    # 1. Temporal patterns
    temporal = compute_temporal_patterns(df, col_map)
    if temporal:
        temporal['hourly'].write.mode('overwrite').json(
            f"{output_base}/hourly_stats/{file_type}_aggregated"
        )
        temporal['daily'].write.mode('overwrite').json(
            f"{output_base}/daily_stats/{file_type}_aggregated"
        )
        print("  ✓ Temporal patterns (hourly & daily)")

    # 2. Fare distribution
    fare_dist = compute_fare_distribution(df, col_map)
    if fare_dist:
        fare_dist['stats'].write.mode('overwrite').json(
            f"{output_base}/fare_distribution/{file_type}_stats"
        )
        fare_dist['buckets'].write.mode('overwrite').json(
            f"{output_base}/fare_distribution/{file_type}_buckets"
        )
        print("  ✓ Fare distribution (stats & buckets)")

    # 3. Spatial analytics
    spatial = compute_spatial_analytics(df, col_map)
    if spatial:
        if 'top_pickup_zones' in spatial:
            spatial['top_pickup_zones'].write.mode('overwrite').json(
                f"{output_base}/zone_demand/{file_type}_pickup_zones"
            )
        if 'top_dropoff_zones' in spatial:
            spatial['top_dropoff_zones'].write.mode('overwrite').json(
                f"{output_base}/zone_demand/{file_type}_dropoff_zones"
            )
        if 'top_od_pairs' in spatial:
            spatial['top_od_pairs'].write.mode('overwrite').json(
                f"{output_base}/zone_demand/{file_type}_od_pairs"
            )
        print("  ✓ Spatial analytics (zones & OD pairs)")

    # 4. Duration stats
    duration = compute_duration_stats(df, col_map)
    if duration:
        duration.write.mode('overwrite').json(
            f"{output_base}/duration_stats/{file_type}_duration"
        )
        print("  ✓ Duration statistics")

    # 5. Overall summary
    summary = compute_summary_stats(df, col_map)
    summary.write.mode('overwrite').json(
        f"{output_base}/summary/{file_type}_overall_summary"
    )
    print("  ✓ Overall summary")

    # Display summary (like local notebook)
    summary_row = summary.collect()[0]
    print(f"\n📊 Summary for {file_type.upper()}:")
    print(f"  Total trips: {summary_row['total_trips']:,}")
    if 'total_revenue' in summary_row.asDict():
        print(f"  Total revenue: ${summary_row['total_revenue']:,.2f}")
    if 'avg_fare' in summary_row.asDict():
        print(f"  Avg fare: ${summary_row['avg_fare']:.2f}")
    if 'total_distance' in summary_row.asDict():
        print(f"  Total distance: {summary_row['total_distance']:,.2f} miles")

    print(f"\n✅ {file_type.upper()} processing complete!\n")

    return file_type

def main():
    """Main execution - processes all taxi types"""

    # Hardcoded S3 paths (matching your bucket structure)
    INPUT_BASE = "s3://data228-bigdata-nyc/staging"
    OUTPUT_BASE = "s3://data228-bigdata-nyc/ground-truth"

    print(f"\n{'='*80}")
    print("STREAMTIDE GROUND TRUTH BASELINE - AWS EMR")
    print(f"{'='*80}")
    print(f"Input: {INPUT_BASE}")
    print(f"Output: {OUTPUT_BASE}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"{'='*80}\n")

    # Create Spark session
    spark = create_spark_session()

    # Process each taxi type (same as local notebook)
    taxi_types = ['yellow', 'green', 'fhv', 'fhvhv']
    processed_types = []

    for taxi_type in taxi_types:
        result = process_taxi_type(spark, taxi_type, INPUT_BASE, OUTPUT_BASE)
        if result:
            processed_types.append(result)

    # Final summary
    print(f"\n{'='*80}")
    print("✅ GROUND TRUTH COMPUTATION COMPLETE")
    print(f"{'='*80}")
    print(f"\nProcessed taxi types: {', '.join(processed_types)}")
    print(f"\nResults saved to: {OUTPUT_BASE}/")
    print(f"\nDirectory structure (same as local notebook):")
    print(f"  {OUTPUT_BASE}/")
    print(f"    ├── hourly_stats/        # Hourly aggregated metrics by type")
    print(f"    ├── daily_stats/         # Daily aggregated metrics by type")
    print(f"    ├── fare_distribution/   # Fare stats and buckets by type")
    print(f"    ├── zone_demand/         # Top zones and OD pairs by type")
    print(f"    ├── duration_stats/      # Trip duration stats by type")
    print(f"    └── summary/             # Overall summaries by type")
    print(f"\n{'='*80}\n")

    spark.stop()

if __name__ == "__main__":
    main()
