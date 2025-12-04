#!/usr/bin/env python3
"""
Enhanced Ground Truth with ALL Metrics for Streaming Algorithm Validation

Computes:
- Original metrics (avg_fare, avg_distance, etc.)
- NEW: Distinct counts (zones, vendors) for HyperLogLog
- NEW: Duplicate detection metrics for Bloom Filter
- NEW: Frequency moments (F0, F1, F2) for Computing Moments
- NEW: High-fare trip counts for DGIM
- NEW: Route frequency for LSH validation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, mean, stddev, sum, expr,
    concat_ws, when, hour, year, desc
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
import os


def create_spark_session():
    """Create Spark session with S3 support for local execution"""
    import os
    # Set AWS profile
    os.environ['AWS_PROFILE'] = 'data228'

    return SparkSession.builder \
        .appName("StreamTide-EnhancedGroundTruth") \
        .master("local[*]") \
        .config("spark.driver.memory", "10g") \
        .config("spark.executor.memory", "10g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "600") \
        .config("spark.default.parallelism", "60") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
        .config("spark.hadoop.fs.s3a.retry.limit", "3") \
        .config("spark.hadoop.fs.s3a.retry.interval", "500") \
        .config("spark.hadoop.fs.s3a.timeout", "200000") \
        .config("spark.hadoop.fs.s3a.threads.max", "64") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "200") \
        .config("spark.hadoop.fs.s3a.socket.send.buffer", "8192") \
        .config("spark.hadoop.fs.s3a.socket.recv.buffer", "8192") \
        .config("spark.hadoop.fs.s3a.paging.maximum", "5") \
        .config("spark.hadoop.fs.s3a.threads.core", "15") \
        .config("spark.hadoop.fs.s3a.max.total.tasks", "20") \
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
        .getOrCreate()


def compute_enhanced_metrics(spark, df, output_path, pickup_col='tpep_pickup_datetime', dropoff_col='tpep_dropoff_datetime'):
    """
    Compute comprehensive ground truth metrics for ALL streaming algorithms

    Args:
        spark: SparkSession instance
        df: Input DataFrame
        output_path: Where to save results
        pickup_col: Name of the pickup datetime column
        dropoff_col: Name of the dropoff datetime column
    """

    print("\n" + "="*70)
    print("COMPUTING ENHANCED GROUND TRUTH METRICS")
    print("="*70)

    # Check which columns exist (yellow/green have fare/distance, FHV/FHVHV don't)
    available_columns = df.columns
    has_fare = 'fare_amount' in available_columns
    has_distance = 'trip_distance' in available_columns
    has_vendor = 'VendorID' in available_columns
    has_payment = 'payment_type' in available_columns

    print(f"\n📋 Available metrics:")
    print(f"  - Fare amount: {'✓' if has_fare else '✗'}")
    print(f"  - Trip distance: {'✓' if has_distance else '✗'}")
    print(f"  - Vendor ID: {'✓' if has_vendor else '✗'}")
    print(f"  - Payment type: {'✓' if has_payment else '✗'}")

    results = {}

    # ===== BASIC STATISTICS (for Reservoir Sampling) =====
    print("\n📊 Computing basic statistics...")

    # Build aggregation based on available columns
    agg_exprs = [count('*').alias('total_trips')]

    if has_fare:
        agg_exprs.extend([
            mean('fare_amount').alias('avg_fare'),
            stddev('fare_amount').alias('std_fare'),
            expr('percentile_approx(fare_amount, 0.5)').alias('median_fare'),
            sum('fare_amount').alias('total_revenue')
        ])

    if has_distance:
        agg_exprs.extend([
            mean('trip_distance').alias('avg_distance'),
            stddev('trip_distance').alias('std_distance'),
            sum('trip_distance').alias('total_distance')
        ])

    basic_stats = df.select(*agg_exprs).collect()[0]
    results['basic_stats'] = basic_stats.asDict()

    print(f"  ✓ Total trips: {basic_stats['total_trips']:,}")
    if has_fare and 'avg_fare' in basic_stats.asDict() and basic_stats['avg_fare'] is not None:
        print(f"  ✓ Avg fare: ${basic_stats['avg_fare']:.2f}")
    if has_distance and 'avg_distance' in basic_stats.asDict() and basic_stats['avg_distance'] is not None:
        print(f"  ✓ Avg distance: {basic_stats['avg_distance']:.2f} miles")


    # ===== DISTINCT COUNTS (for HyperLogLog) =====
    print("\n🔢 Computing cardinality (distinct counts)...")

    # Build cardinality query based on available columns
    cardinality_exprs = [
        countDistinct(col('PULocationID')).alias('distinct_pu_zones'),
        countDistinct(col('DOLocationID')).alias('distinct_do_zones')
    ]

    if has_vendor:
        cardinality_exprs.append(countDistinct(col('VendorID')).alias('distinct_vendors'))
    if has_payment:
        cardinality_exprs.append(countDistinct(col('payment_type')).alias('distinct_payment_types'))

    distinct_zones = df.select(*cardinality_exprs).collect()[0]

    # Calculate distinct total zones (union of pickup and dropoff)
    pu_zones = df.select('PULocationID').distinct()
    do_zones = df.select('DOLocationID').distinct()
    total_distinct_zones = pu_zones.union(do_zones).distinct().count()

    results['cardinality'] = {
        'distinct_pickup_zones': distinct_zones['distinct_pu_zones'],
        'distinct_dropoff_zones': distinct_zones['distinct_do_zones'],
        'distinct_total_zones': total_distinct_zones
    }

    if has_vendor:
        results['cardinality']['distinct_vendors'] = distinct_zones['distinct_vendors']
        # Debug: Check how many non-null vendor IDs we have
        non_null_vendors = df.filter(col('VendorID').isNotNull()).count()
        results['cardinality']['non_null_vendor_count'] = non_null_vendors
    if has_payment:
        results['cardinality']['distinct_payment_types'] = distinct_zones['distinct_payment_types']

    print(f"  ✓ Distinct pickup zones: {results['cardinality']['distinct_pickup_zones']}")
    print(f"  ✓ Distinct dropoff zones: {results['cardinality']['distinct_dropoff_zones']}")
    if has_vendor:
        print(f"  ✓ Distinct vendors: {results['cardinality']['distinct_vendors']}")
        print(f"  ℹ️  Non-null vendor records: {results['cardinality']['non_null_vendor_count']:,}")


    # ===== DUPLICATE DETECTION (for Bloom Filter) =====
    # SKIPPED: Too memory-intensive for 295M records on local machine
    print("\n🌸 Skipping duplicate detection (too memory-intensive for local processing)...")

    results['duplicates'] = {
        'total_unique_trips': None,
        'duplicate_trip_signatures': None,
        'duplicate_trip_count': 0,
        'duplicate_rate_percentage': 0.0,
        'note': 'Skipped due to memory constraints on local machine'
    }

    print(f"  ⚠ Duplicate detection skipped (run on EMR for full analysis)")


    # ===== FREQUENCY MOMENTS (for Computing Moments) =====
    print("\n📈 Computing frequency moments...")

    # Zone frequency distribution
    zone_freq = df.groupBy('PULocationID').count().withColumnRenamed('count', 'frequency')

    # F0: Distinct elements (already have this)
    f0 = zone_freq.count()

    # F1: Total count (sum of all frequencies)
    f1 = zone_freq.select(sum('frequency')).collect()[0][0]

    # F2: Sum of squared frequencies (surprise number)
    f2 = zone_freq.select(sum(col('frequency') * col('frequency'))).collect()[0][0]

    results['frequency_moments'] = {
        'F0': f0,  # Distinct zones
        'F1': f1,  # Total trips
        'F2': f2,  # Surprise number
        'gini_coefficient': (f2 ** 0.5) / f1 if f1 > 0 else 0
    }

    # Top zones with frequencies (for validation)
    top_zones = zone_freq.orderBy(desc('frequency')).limit(20).collect()
    results['top_zone_frequencies'] = [
        {'zone': row['PULocationID'], 'frequency': row['frequency']}
        for row in top_zones
    ]

    print(f"  ✓ F0 (distinct zones): {f0}")
    print(f"  ✓ F1 (total trips): {f1:,}")
    print(f"  ✓ F2 (surprise number): {f2:,}")
    print(f"  ✓ Gini coefficient: {results['frequency_moments']['gini_coefficient']:.4f}")


    # ===== HIGH-FARE TRIPS (for DGIM) =====
    if has_fare:
        print("\n📊 Computing high-fare trip metrics (for DGIM)...")

        fare_threshold = 20.0  # Same as used in streaming

        # Debug: Check non-null fares
        non_null_fares = df.filter(col('fare_amount').isNotNull()).count()
        max_fare = df.agg({'fare_amount': 'max'}).collect()[0][0]

        high_fare_count = df.filter(col('fare_amount') > fare_threshold).count()

        results['dgim_metrics'] = {
            'fare_threshold': fare_threshold,
            'high_fare_trip_count': high_fare_count,
            'high_fare_percentage': (high_fare_count / results['basic_stats']['total_trips']) * 100
                if results['basic_stats']['total_trips'] > 0 else 0,
            'non_null_fare_count': non_null_fares,
            'max_fare': max_fare
        }

        print(f"  ✓ High-fare trips (>${fare_threshold}): {high_fare_count:,}")
        print(f"  ✓ High-fare percentage: {results['dgim_metrics']['high_fare_percentage']:.2f}%")
        print(f"  ℹ️  Non-null fare records: {non_null_fares:,}")
        print(f"  ℹ️  Max fare amount: ${max_fare if max_fare else 0:.2f}")
    else:
        print("\n📊 Skipping DGIM metrics (no fare_amount column)")
        results['dgim_metrics'] = None


    # ===== ROUTE ANALYSIS (for LSH) =====
    print("\n🔍 Computing route frequencies (for LSH)...")

    route_freq = df.groupBy('PULocationID', 'DOLocationID') \
        .count() \
        .withColumnRenamed('count', 'frequency') \
        .orderBy(desc('frequency')) \
        .limit(100)

    top_routes = route_freq.collect()
    results['top_routes'] = [
        {
            'pickup_zone': row['PULocationID'],
            'dropoff_zone': row['DOLocationID'],
            'frequency': row['frequency']
        }
        for row in top_routes
    ]

    print(f"  ✓ Top route: Zone {top_routes[0]['PULocationID']} → {top_routes[0]['DOLocationID']} ({top_routes[0]['frequency']:,} trips)")


    # ===== HOURLY BREAKDOWN =====
    # SKIPPED: Moved to separate job (ground_truth_hourly.py)
    print("\n⏰ Skipping hourly statistics (run ground_truth_hourly.py separately)...")

    results['hourly_stats'] = []
    results['hourly_stats_note'] = 'Run ground_truth_hourly.py for hourly breakdown'

    print(f"  ⚠ Hourly statistics skipped (run ground_truth_hourly.py on EMR)")


    # ===== SAVE RESULTS =====
    print("\n💾 Saving enhanced ground truth...")

    # Convert results to JSON strings
    main_json = json.dumps(results, indent=2, default=str)
    cardinality_json = json.dumps(results['cardinality'], indent=2, default=str)
    moments_json = json.dumps(results['frequency_moments'], indent=2, default=str)
    duplicates_json = json.dumps(results['duplicates'], indent=2, default=str)

    # Save using Spark (works for both S3 and local)
    # This is the same approach as ground_truth_aws_v2.py

    # Save comprehensive JSON
    spark.sparkContext.parallelize([main_json]).saveAsTextFile(
        f"{output_path}/enhanced_ground_truth"
    )
    print(f"  ✓ Saved enhanced_ground_truth.json")

    # Save individual metric files
    spark.sparkContext.parallelize([cardinality_json]).saveAsTextFile(
        f"{output_path}/cardinality"
    )
    print(f"  ✓ Saved cardinality.json")

    spark.sparkContext.parallelize([moments_json]).saveAsTextFile(
        f"{output_path}/frequency_moments"
    )
    print(f"  ✓ Saved frequency_moments.json")

    spark.sparkContext.parallelize([duplicates_json]).saveAsTextFile(
        f"{output_path}/duplicates"
    )
    print(f"  ✓ Saved duplicates.json")

    print(f"\n  ✓ All files saved to: {output_path}/")

    print("\n✅ Enhanced ground truth computation complete!")

    return results


def main():
    """Main function"""

    # Configuration
    INPUT_PATH = "s3a://data228-bigdata-nyc/staging/*.parquet"
    OUTPUT_PATH = "s3a://data228-bigdata-nyc/prod/ground-truth-enhanced/"

    print(f"""
    ========================================
    Enhanced Ground Truth Computation
    ========================================
    Input:  ALL data from {INPUT_PATH}
    Filter: EXCLUDING year 2025
    Output: {OUTPUT_PATH}
    ========================================
    """)

    # Create Spark session with S3 support
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Read each taxi type separately to avoid schema conflicts
        print("📥 Reading data from S3 staging by taxi type...")

        dataframes = []

        # Read Yellow taxi data
        try:
            print("  Reading yellow taxi data...")
            df_yellow = spark.read.format("parquet") \
                .option("pathGlobFilter", "yellow_tripdata_*.parquet") \
                .load("s3a://data228-bigdata-nyc/staging/")
            count = df_yellow.count()
            # Debug: Check columns
            print(f"    Columns: {', '.join([c for c in df_yellow.columns if 'vendor' in c.lower() or 'Vendor' in c])}")
            dataframes.append(df_yellow)
            print(f"    ✓ Loaded {count:,} yellow taxi records")
        except Exception as e:
            print(f"    ⚠ Yellow taxi error: {str(e)[:200]}")

        # Read Green taxi data
        try:
            print("  Reading green taxi data...")
            df_green = spark.read.format("parquet") \
                .option("pathGlobFilter", "green_tripdata_*.parquet") \
                .load("s3a://data228-bigdata-nyc/staging/")
            count = df_green.count()
            dataframes.append(df_green)
            print(f"    ✓ Loaded {count:,} green taxi records")
        except Exception as e:
            print(f"    ⚠ Green taxi error: {str(e)[:200]}")

        # Read FHV data
        try:
            print("  Reading FHV data...")
            df_fhv = spark.read.format("parquet") \
                .option("pathGlobFilter", "fhv_tripdata_*.parquet") \
                .load("s3a://data228-bigdata-nyc/staging/")
            count = df_fhv.count()
            dataframes.append(df_fhv)
            print(f"    ✓ Loaded {count:,} FHV records")
        except Exception as e:
            print(f"    ⚠ FHV error: {str(e)[:200]}")

        # Read FHVHV data
        try:
            print("  Reading FHVHV data...")
            df_fhvhv = spark.read.format("parquet") \
                .option("pathGlobFilter", "fhvhv_tripdata_*.parquet") \
                .load("s3a://data228-bigdata-nyc/staging/")
            count = df_fhvhv.count()
            dataframes.append(df_fhvhv)
            print(f"    ✓ Loaded {count:,} FHVHV records")
        except Exception as e:
            print(f"    ⚠ FHVHV error: {str(e)[:200]}")

        if not dataframes:
            raise Exception("No data loaded from any taxi type!")

        # Union all dataframes
        print(f"\n  Combining {len(dataframes)} taxi type(s)...")
        df_all = dataframes[0]
        for df in dataframes[1:]:
            df_all = df_all.unionByName(df, allowMissingColumns=True)

        # Cast location IDs to consistent types
        df_all = df_all.withColumn("PULocationID", col("PULocationID").cast("int")) \
                       .withColumn("DOLocationID", col("DOLocationID").cast("int"))

        # Find the pickup and dropoff datetime columns (different names for different file types)
        columns = [c.lower() for c in df_all.columns]
        pickup_col = None
        dropoff_col = None

        for col_name in df_all.columns:
            col_lower = col_name.lower()
            if any(p in col_lower for p in ['pickup_datetime', 'tpep_pickup', 'lpep_pickup']):
                pickup_col = col_name
                print(f"📅 Using pickup datetime column: {pickup_col}")
            if any(p in col_lower for p in ['dropoff_datetime', 'tpep_dropoff', 'lpep_dropoff']):
                dropoff_col = col_name
                print(f"📅 Using dropoff datetime column: {dropoff_col}")

        if not pickup_col:
            raise Exception("Could not find pickup datetime column!")
        if not dropoff_col:
            raise Exception("Could not find dropoff datetime column!")

        # EXCLUDE 2025 data - filter by year using the detected column
        df = df_all.filter(year(col(pickup_col)) != 2025)

        total_records = df.count()
        print(f"✅ Loaded {total_records:,} records (excluding 2025)")

        # Compute all metrics
        results = compute_enhanced_metrics(spark, df, OUTPUT_PATH, pickup_col, dropoff_col)

        print("\n" + "="*70)
        print("📋 SUMMARY")
        print("="*70)
        print(f"Total trips: {results['basic_stats']['total_trips']:,}")
        print(f"Distinct zones: {results['cardinality']['distinct_pickup_zones']}")
        print(f"Duplicate rate: {results['duplicates']['duplicate_rate_percentage']:.2f}%")
        if results['dgim_metrics']:
            print(f"High-fare trips: {results['dgim_metrics']['high_fare_trip_count']:,}")
        print(f"F2 (surprise): {results['frequency_moments']['F2']:,}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
