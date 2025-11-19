#!/usr/bin/env python3
"""
Ground Truth: Basic Statistics (Reservoir Sampling Validation)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, stddev, sum, expr, year
import json
import os


def create_spark_session():
    """Create Spark session"""
    os.environ['AWS_PROFILE'] = 'data228'

    return SparkSession.builder \
        .appName("StreamTide-BasicStats") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()


def compute_basic_stats(spark, df, output_path):
    """Compute basic statistics"""

    print("\n" + "="*70)
    print("COMPUTING BASIC STATISTICS (Reservoir Sampling)")
    print("="*70)

    available_columns = df.columns
    has_fare = 'fare_amount' in available_columns
    has_distance = 'trip_distance' in available_columns

    print(f"\n📋 Available metrics:")
    print(f"  - Fare amount: {'✓' if has_fare else '✗'}")
    print(f"  - Trip distance: {'✓' if has_distance else '✗'}")

    # Build aggregation
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
    results = basic_stats.asDict()

    print(f"\n  ✓ Total trips: {results['total_trips']:,}")
    if has_fare and 'avg_fare' in results and results['avg_fare'] is not None:
        print(f"  ✓ Avg fare: ${results['avg_fare']:.2f}")
        print(f"  ✓ Median fare: ${results.get('median_fare', 0):.2f}")
        print(f"  ✓ Total revenue: ${results.get('total_revenue', 0):,.2f}")
    if has_distance and 'avg_distance' in results and results['avg_distance'] is not None:
        print(f"  ✓ Avg distance: {results['avg_distance']:.2f} miles")
        print(f"  ✓ Total distance: {results.get('total_distance', 0):,.2f} miles")

    # Save
    print("\n💾 Saving basic statistics...")
    stats_json = json.dumps({'basic_stats': results}, indent=2, default=str)
    spark.sparkContext.parallelize([stats_json]).coalesce(1).saveAsTextFile(
        f"{output_path}/basic_stats"
    )
    print(f"  ✓ Saved to: {output_path}/basic_stats/")

    return results


def load_data(spark):
    """Load and prepare data"""
    print("📥 Reading data from S3 staging...")

    dataframes = []
    for taxi_type in ["yellow", "green", "fhv", "fhvhv"]:
        try:
            df = spark.read.format("parquet") \
                .option("pathGlobFilter", f"{taxi_type}_tripdata_*.parquet") \
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

            # Payment type columns
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

    # Filter 2025 using standardized pickup_datetime column
    df_all = df_all.filter(year(col('pickup_datetime')) != 2025)

    total = df_all.count()
    print(f"\n✅ Total: {total:,} records (excluding 2025)")

    return df_all


def main():
    OUTPUT_PATH = "s3a://data228-bigdata-nyc/ground-truth-enhanced"

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = load_data(spark)
        results = compute_basic_stats(spark, df, OUTPUT_PATH)

        print("\n✅ Basic statistics complete!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
