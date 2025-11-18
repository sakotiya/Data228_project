#!/usr/bin/env python3
"""
Ground Truth: Hourly Statistics Only

Separate job to compute hourly breakdown for large datasets.
Outputs to same format/path as main ground truth script.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, mean, stddev, sum, expr, when, hour, year
)
import json
import os


def create_spark_session():
    """Create Spark session optimized for hourly aggregations"""
    import os
    os.environ['AWS_PROFILE'] = 'data228'

    return SparkSession.builder \
        .appName("StreamTide-HourlyStats") \
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


def compute_hourly_stats(spark, df, output_path, pickup_col):
    """
    Compute hourly breakdown statistics

    Args:
        spark: SparkSession
        df: Input DataFrame
        output_path: S3 path for output
        pickup_col: Pickup datetime column name
    """

    print("\n" + "="*70)
    print("COMPUTING HOURLY STATISTICS")
    print("="*70)

    # Check which columns exist
    available_columns = df.columns
    has_fare = 'fare_amount' in available_columns
    has_distance = 'trip_distance' in available_columns

    print(f"\n📋 Available metrics for hourly breakdown:")
    print(f"  - Fare amount: {'✓' if has_fare else '✗'}")
    print(f"  - Trip distance: {'✓' if has_distance else '✗'}")

    # ===== HOURLY BREAKDOWN =====
    print("\n⏰ Computing hourly statistics...")
    print("  (This may take a while with large datasets...)")

    df_with_hour = df.withColumn('hour', hour(pickup_col))

    # Build hourly aggregations based on available columns
    hourly_agg_exprs = [
        count('*').alias('trip_count'),
        countDistinct(col('PULocationID')).alias('distinct_zones_hour')
    ]

    if has_fare:
        fare_threshold = 20.0
        hourly_agg_exprs.extend([
            mean('fare_amount').alias('avg_fare'),
            expr('percentile_approx(fare_amount, 0.5)').alias('median_fare'),
            stddev('fare_amount').alias('std_fare'),
            sum(when(col('fare_amount') > fare_threshold, 1).otherwise(0)).alias('high_fare_count_hour')
        ])

    if has_distance:
        hourly_agg_exprs.append(mean('trip_distance').alias('avg_distance'))

    print("  Grouping by hour and computing aggregations...")
    hourly_stats = df_with_hour.groupBy('hour').agg(*hourly_agg_exprs).orderBy('hour')

    hourly_results = [row.asDict() for row in hourly_stats.collect()]

    print(f"\n  ✓ Computed stats for {len(hourly_results)} hours")

    # Show sample
    if hourly_results:
        print("\n  Sample (Hour 0):")
        sample = hourly_results[0]
        print(f"    - Trips: {sample.get('trip_count', 0):,}")
        if has_fare and sample.get('avg_fare'):
            print(f"    - Avg fare: ${sample.get('avg_fare', 0):.2f}")
        if has_distance and sample.get('avg_distance'):
            print(f"    - Avg distance: {sample.get('avg_distance', 0):.2f} miles")

    # ===== SAVE RESULTS =====
    print("\n💾 Saving hourly statistics...")

    hourly_json = json.dumps({'hourly_stats': hourly_results}, indent=2, default=str)

    # Save to same path structure as main ground truth
    spark.sparkContext.parallelize([hourly_json]).coalesce(1).saveAsTextFile(
        f"{output_path}/hourly_stats"
    )

    print(f"  ✓ Saved to: {output_path}/hourly_stats/")
    print("\n✅ Hourly statistics complete!")

    return hourly_results


def main():
    """Main function"""

    # Configuration - same as main ground truth script
    INPUT_PATH = "s3a://data228-bigdata-nyc/staging/*.parquet"
    OUTPUT_PATH = "s3a://data228-bigdata-nyc/ground-truth-enhanced"

    print(f"""
    ========================================
    Hourly Statistics Ground Truth
    ========================================
    Input:  ALL data from staging
    Filter: EXCLUDING year 2025
    Output: {OUTPUT_PATH}/hourly_stats/
    ========================================
    """)

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Read each taxi type separately (same as main script)
        print("📥 Reading data from S3 staging by taxi type...")

        dataframes = []

        # Read Yellow taxi data
        try:
            print("  Reading yellow taxi data...")
            df_yellow = spark.read.format("parquet") \
                .option("pathGlobFilter", "yellow_tripdata_*.parquet") \
                .load("s3a://data228-bigdata-nyc/staging/")

            # Standardize column names
            # Distance columns
            if 'Trip_Distance' in df.columns:
                df = df.withColumnRenamed('Trip_Distance', 'trip_distance')
            if 'trip_miles' in df.columns:
                df = df.withColumnRenamed('trip_miles', 'trip_distance')

            # Fare columns
            if 'Fare_Amt' in df.columns:
                df = df.withColumnRenamed('Fare_Amt', 'fare_amount')
            if 'base_passenger_fare' in df.columns:
                df = df.withColumnRenamed('base_passenger_fare', 'fare_amount')

            # Location ID columns (fix FHV casing)
            if 'PUlocationID' in df.columns:
                df = df.withColumnRenamed('PUlocationID', 'PULocationID')
            if 'DOlocationID' in df.columns:
                df = df.withColumnRenamed('DOlocationID', 'DOLocationID')

            # Vendor columns
            if 'vendor_name' in df.columns:
                df = df.withColumnRenamed('vendor_name', 'VendorID')
            if 'VendorID' in df.columns:   
                df = df.withColumn('VendorID', col('VendorID').cast('string'))

            # Payment type columns (cast to string to handle INT64 vs STRING mismatch)
            if 'Payment_Type' in df.columns:
                df = df.withColumnRenamed('Payment_Type', 'payment_type')
            if 'payment_type' in df.columns:
                df = df.withColumn('payment_type', col('payment_type').cast('string'))

            # Datetime columns - standardize to pickup_datetime and dropoff_datetime
            if 'Trip_Pickup_DateTime' in df.columns:
                df = df.withColumnRenamed('Trip_Pickup_DateTime', 'pickup_datetime')
            if 'lpep_pickup_datetime' in df.columns:
                df = df.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime')
            if 'tpep_pickup_datetime' in df.columns:
                df = df.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')

            if 'Trip_Dropoff_DateTime' in df.columns:
                df = df.withColumnRenamed('Trip_Dropoff_DateTime', 'dropoff_datetime')
            if 'lpep_dropoff_datetime' in df.columns:
                df = df.withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
            if 'tpep_dropoff_datetime' in df.columns:
                df = df.withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
            if 'dropOff_datetime' in df.columns:
                df = df.withColumnRenamed('dropOff_datetime', 'dropoff_datetime')

            # Cast datetime columns to timestamp (yellow taxi has them as strings)
            if 'pickup_datetime' in df.columns:
                df = df.withColumn('pickup_datetime', col('pickup_datetime').cast('timestamp'))
            if 'dropoff_datetime' in df.columns:
                df = df.withColumn('dropoff_datetime', col('dropoff_datetime').cast('timestamp'))


            count = df_yellow.count()
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

            # Standardize column names
            if 'trip_miles' in df_fhvhv.columns:
                df_fhvhv = df_fhvhv.withColumnRenamed('trip_miles', 'trip_distance')
            if 'base_passenger_fare' in df_fhvhv.columns:
                df_fhvhv = df_fhvhv.withColumnRenamed('base_passenger_fare', 'fare_amount')

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

        # Now we can use standardized pickup_datetime column
        df_all = df_all.filter(year(col('pickup_datetime')) < 2025)

        total_records = df_all.count()
        print(f"✅ Loaded {total_records:,} records (excluding 2025)")

        # Compute hourly statistics
        hourly_results = compute_hourly_stats(spark, df_all, OUTPUT_PATH, 'pickup_datetime')

        print("\n" + "="*70)
        print("📋 SUMMARY")
        print("="*70)
        print(f"Total trips analyzed: {total_records:,}")
        print(f"Hours computed: {len(hourly_results)}")
        print(f"Output: {OUTPUT_PATH}/hourly_stats/")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
