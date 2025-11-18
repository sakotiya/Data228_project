#!/usr/bin/env python3
"""
Ground Truth: DGIM Metrics with 20 Window Snapshots
Computes actual counts for comparison with DGIM estimates
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, when, lit, row_number, floor
from pyspark.sql.window import Window
import json
import os


def create_spark_session():
    """Create Spark session"""
    os.environ['AWS_PROFILE'] = 'data228'

    return SparkSession.builder \
        .appName("StreamTide-DGIM-GroundTruth") \
        .master("local[*]") \
        .config("spark.driver.memory", "12g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.0,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.683") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()


def compute_dgim_ground_truth(spark, df, output_path, total_trips, window_size=10000):
    """Compute ground truth for all 5 DGIM metrics at 20 intervals"""

    print("\n" + "="*70)
    print("COMPUTING DGIM GROUND TRUTH (20 Window Snapshots)")
    print("="*70)

    # Airport zone IDs (JFK, LaGuardia, Newark)
    airport_zones = [132, 138, 1]

    # Add row numbers to simulate streaming order
    df_with_row = df.withColumn("row_num", row_number().over(Window.orderBy(lit(1))))

    # Add binary condition columns
    df_binary = df_with_row \
        .withColumn('is_high_fare',
            when(col('fare_amount') > 20, 1).otherwise(0)) \
        .withColumn('is_long_distance',
            when(col('trip_distance') > 5, 1).otherwise(0)) \
        .withColumn('is_high_tip',
            when((col('total_amount') > 0) &
                 ((col('tip_amount') / col('total_amount') * 100) > 20), 1).otherwise(0)) \
        .withColumn('is_premium',
            when(col('passenger_count') >= 3, 1).otherwise(0)) \
        .withColumn('is_airport',
            when(col('PULocationID').isin(airport_zones) |
                 col('DOLocationID').isin(airport_zones), 1).otherwise(0))

    # Cache for multiple passes
    df_binary.cache()

    # Calculate snapshot intervals (20 snapshots)
    snapshot_interval = total_trips // 20

    print(f"\n  Window size: {window_size}")
    print(f"  Total trips: {total_trips:,}")
    print(f"  Snapshot interval: {snapshot_interval:,}")

    # Compute ground truth at each of 20 intervals
    window_snapshots = []

    for i in range(1, 21):
        record_num = min(i * snapshot_interval, total_trips)
        window_start = max(1, record_num - window_size + 1)

        # Filter to window
        window_df = df_binary.filter(
            (col('row_num') >= window_start) & (col('row_num') <= record_num)
        )

        # Compute actual counts in window
        counts = window_df.agg(
            {'is_high_fare': 'sum', 'is_long_distance': 'sum',
             'is_high_tip': 'sum', 'is_premium': 'sum', 'is_airport': 'sum'}
        ).collect()[0]

        snapshot = {
            'record_num': record_num,
            'window_start': window_start,
            'window_size': record_num - window_start + 1,
            'high_fare': {
                'actual': int(counts['sum(is_high_fare)'] or 0)
            },
            'long_distance': {
                'actual': int(counts['sum(is_long_distance)'] or 0)
            },
            'high_tip': {
                'actual': int(counts['sum(is_high_tip)'] or 0)
            },
            'premium': {
                'actual': int(counts['sum(is_premium)'] or 0)
            },
            'airport': {
                'actual': int(counts['sum(is_airport)'] or 0)
            }
        }

        window_snapshots.append(snapshot)
        print(f"  Snapshot #{i}: records {window_start:,} - {record_num:,}")

    # Compute cumulative totals
    totals = df_binary.agg(
        {'is_high_fare': 'sum', 'is_long_distance': 'sum',
         'is_high_tip': 'sum', 'is_premium': 'sum', 'is_airport': 'sum'}
    ).collect()[0]

    # Compile results
    results = {
        'run_metadata': {
            'total_records': total_trips,
            'window_size': window_size,
            'num_snapshots': 20
        },
        'window_snapshots': window_snapshots,
        'cumulative_stats': {
            'high_fare_trips': int(totals['sum(is_high_fare)'] or 0),
            'long_distance_trips': int(totals['sum(is_long_distance)'] or 0),
            'high_tip_trips': int(totals['sum(is_high_tip)'] or 0),
            'premium_trips': int(totals['sum(is_premium)'] or 0),
            'airport_trips': int(totals['sum(is_airport)'] or 0)
        }
    }

    # Print summary
    print(f"\n📊 Ground Truth Summary:")
    print(f"   ├─ High Fare Trips: {results['cumulative_stats']['high_fare_trips']:,}")
    print(f"   ├─ Long Distance Trips: {results['cumulative_stats']['long_distance_trips']:,}")
    print(f"   ├─ High Tip Trips: {results['cumulative_stats']['high_tip_trips']:,}")
    print(f"   ├─ Premium Trips: {results['cumulative_stats']['premium_trips']:,}")
    print(f"   └─ Airport Trips: {results['cumulative_stats']['airport_trips']:,}")

    # Save to S3
    print("\n💾 Saving ground truth to S3...")

    # Save as JSON
    import boto3
    session = boto3.Session()
    s3 = session.client("s3")

    bucket = "data228-bigdata-nyc"
    key = f"{output_path.replace('s3a://data228-bigdata-nyc/', '')}/dgim_ground_truth.json"

    json_str = json.dumps(results, indent=2, default=str)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json_str.encode('utf-8'),
        ContentType='application/json'
    )

    print(f"  ✓ Saved to: s3://{bucket}/{key}")

    df_binary.unpersist()

    return results


def load_data(spark):
    """Load and prepare data from staging (excluding 2025/ and 2015_bloomdedup.parquet)"""
    print("📥 Reading data from S3 staging...")
    print("   (excluding 2025/ folder and 2015_bloomdedup.parquet)")

    dataframes = []
    for taxi_type in ["yellow", "green", "fhv", "fhvhv"]:
        try:
            # Load from staging root, excluding 2025 subfolder
            df = spark.read.format("parquet") \
                .option("pathGlobFilter", f"{taxi_type}_tripdata_*.parquet") \
                .option("recursiveFileLookup", "false") \
                .load("s3a://data228-bigdata-nyc/staging/")

            print(f"  {taxi_type} has fare: {'fare_amount' in df.columns or 'Fare_Amt' in df.columns or 'base_passenger_fare' in df.columns}")

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


            count = df.count()
            dataframes.append(df)
            print(f"  ✓ {taxi_type}: {count:,} records")
        except Exception as e:
            print(f"  ⚠ {taxi_type}: {str(e)[:100]}")

    if not dataframes:
        raise Exception("No data loaded!")

    # Union all
    df_all = dataframes[0]
    for df in dataframes[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)

    # Now we can use standardized pickup_datetime column
    df_all = df_all.filter(year(col('pickup_datetime')) < 2025)

    total = df_all.count()
    print(f"\n✅ Total: {total:,} records (excluding 2025)")

    return df_all, total


def main():
    OUTPUT_PATH = "s3a://data228-bigdata-nyc/ground-truth-enhanced"

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df, total = load_data(spark)
        results = compute_dgim(spark, df, OUTPUT_PATH, total)

        print("\n✅ DGIM metrics complete!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
