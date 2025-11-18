#!/usr/bin/env python3
"""
Ground Truth: DGIM (High-Fare Trip Counts)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year
import json
import os


def create_spark_session():
    """Create Spark session"""
    os.environ['AWS_PROFILE'] = 'data228'

    return SparkSession.builder \
        .appName("StreamTide-DGIM") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()


def compute_dgim(spark, df, output_path, total_trips):
    """Compute high-fare trip metrics"""

    print("\n" + "="*70)
    print("COMPUTING DGIM METRICS (High-Fare Trips)")
    print("="*70)

    has_fare = 'fare_amount' in df.columns

    if not has_fare:
        print("  ⚠ No fare_amount column found!")
        results = {'note': 'No fare data available'}
    else:
        fare_threshold = 20.0

        # Debug
        non_null_fares = df.filter(col('fare_amount').isNotNull()).count()
        max_fare = df.agg({'fare_amount': 'max'}).collect()[0][0]

        high_fare_count = df.filter(col('fare_amount') > fare_threshold).count()

        results = {
            'fare_threshold': fare_threshold,
            'high_fare_trip_count': high_fare_count,
            'high_fare_percentage': (high_fare_count / total_trips * 100) if total_trips > 0 else 0,
            'non_null_fare_count': non_null_fares,
            'max_fare': max_fare,
            'total_trips': total_trips
        }

        print(f"\n  ✓ Fare threshold: ${fare_threshold}")
        print(f"  ✓ High-fare trips (>${fare_threshold}): {high_fare_count:,}")
        print(f"  ✓ High-fare percentage: {results['high_fare_percentage']:.2f}%")
        print(f"  ℹ️  Non-null fare records: {non_null_fares:,}")
        print(f"  ℹ️  Max fare: ${max_fare if max_fare else 0:.2f}")

    # Save
    print("\n💾 Saving DGIM metrics...")
    dgim_json = json.dumps({'dgim_metrics': results}, indent=2, default=str)
    spark.sparkContext.parallelize([dgim_json]).coalesce(1).saveAsTextFile(
        f"{output_path}/dgim_metrics"
    )
    print(f"  ✓ Saved to: {output_path}/dgim_metrics/")

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
