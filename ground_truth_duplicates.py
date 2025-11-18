#!/usr/bin/env python3
"""
Ground Truth: Duplicate Detection Only (for Bloom Filter Validation)

Separate job to compute duplicate metrics for large datasets.
Outputs to same format/path as main ground truth script.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, concat_ws, year, hour
)
import json
import os


def create_spark_session():
    """Create Spark session optimized for duplicate detection"""
    import os
    os.environ['AWS_PROFILE'] = 'data228'

    return SparkSession.builder \
        .appName("StreamTide-DuplicateDetection") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "800") \
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


def compute_duplicates(spark, df, output_path, pickup_col, dropoff_col, total_trips):
    """
    Compute duplicate detection metrics for Bloom Filter validation

    Args:
        spark: SparkSession
        df: Input DataFrame
        output_path: S3 path for output
        pickup_col: Pickup datetime column name
        dropoff_col: Dropoff datetime column name
        total_trips: Total trip count (for percentage calculation)
    """

    print("\n" + "="*70)
    print("COMPUTING DUPLICATE DETECTION METRICS")
    print("="*70)
    print(f"Total trips to process: {total_trips:,}")

    # Check which columns exist
    available_columns = df.columns
    has_vendor = 'VendorID' in available_columns

    print(f"\n📋 Using columns for trip signature:")
    print(f"  - Pickup datetime: {pickup_col}")
    print(f"  - Dropoff datetime: {dropoff_col}")
    print(f"  - PULocationID: ✓")
    print(f"  - DOLocationID: ✓")
    print(f"  - VendorID: {'✓' if has_vendor else '✗'}")

    # ===== DUPLICATE DETECTION (for Bloom Filter) =====
    print("\n🌸 Analyzing duplicates...")
    print("  (This may take a while with large datasets...)")

    # Create trip signature
    sig_cols = [col(pickup_col), col(dropoff_col), col('PULocationID'), col('DOLocationID')]
    if has_vendor:
        sig_cols.insert(0, col('VendorID'))

    df_with_sig = df.withColumn(
        'trip_signature',
        concat_ws('_', *sig_cols)
    )

    print("  Step 1/4: Grouping by trip signature...")
    duplicate_counts = df_with_sig.groupBy('trip_signature').count()

    print("  Step 2/4: Filtering duplicates...")
    duplicates = duplicate_counts.filter(col('count') > 1)

    print("  Step 3/4: Counting unique trips...")
    total_unique = duplicate_counts.count()

    print("  Step 4/4: Computing duplicate statistics...")
    duplicate_sigs = duplicates.count()
    duplicate_total = duplicates.select(sum(col('count') - 1)).collect()[0][0] or 0

    results = {
        'total_unique_trips': total_unique,
        'duplicate_trip_signatures': duplicate_sigs,
        'duplicate_trip_count': duplicate_total,
        'duplicate_rate_percentage': (duplicate_total / total_trips * 100) if total_trips > 0 else 0.0
    }

    print(f"\n  ✓ Total trips: {total_trips:,}")
    print(f"  ✓ Unique trips: {results['total_unique_trips']:,}")
    print(f"  ✓ Duplicate trip signatures: {results['duplicate_trip_signatures']:,}")
    print(f"  ✓ Duplicate trips: {results['duplicate_trip_count']:,}")
    print(f"  ✓ Duplicate rate: {results['duplicate_rate_percentage']:.2f}%")

    # ===== SAVE RESULTS =====
    print("\n💾 Saving duplicate detection results...")

    duplicates_json = json.dumps(results, indent=2, default=str)

    # Save to same path structure as main ground truth
    spark.sparkContext.parallelize([duplicates_json]).coalesce(1).saveAsTextFile(
        f"{output_path}/duplicates"
    )

    print(f"  ✓ Saved to: {output_path}/duplicates/")
    print("\n✅ Duplicate detection complete!")

    return results


def main():
    """Main function"""

    # Configuration - same as main ground truth script
    INPUT_PATH = "s3a://data228-bigdata-nyc/staging/*.parquet"
    OUTPUT_PATH = "s3a://data228-bigdata-nyc/ground-truth-enhanced"

    print(f"""
    ========================================
    Duplicate Detection Ground Truth
    ========================================
    Input:  ALL data from staging
    Filter: EXCLUDING year 2025
    Output: {OUTPUT_PATH}/duplicates/
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
            if 'Trip_Distance' in df_yellow.columns:
                df_yellow = df_yellow.withColumnRenamed('Trip_Distance', 'trip_distance')
            if 'Fare_Amt' in df_yellow.columns:
                df_yellow = df_yellow.withColumnRenamed('Fare_Amt', 'fare_amount')

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

        pickup_col = 'pickup_datetime'
        dropoff_col = 'dropoff_datetime'

        # Now we can use standardized pickup_datetime column
        df_all = df_all.filter(year(col('pickup_datetime')) < 2025)

        total_records = df_all.count()
        print(f"✅ Loaded {total_records:,} records (excluding 2025)")

        # Compute duplicate detection
        results = compute_duplicates(spark, df_all, OUTPUT_PATH, pickup_col, dropoff_col, total_records)

        print("\n" + "="*70)
        print("📋 SUMMARY")
        print("="*70)
        print(f"Total trips: {total_records:,}")
        print(f"Unique trips: {results['total_unique_trips']:,}")
        print(f"Duplicate rate: {results['duplicate_rate_percentage']:.2f}%")
        print(f"Output: {OUTPUT_PATH}/duplicates/")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
