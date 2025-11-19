#!/usr/bin/env python3
"""
Ground Truth: Frequency Moments (F0, F1, F2)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, year,countDistinct, max, count
import json
import os


def create_spark_session():
    """Create Spark session"""
    os.environ['AWS_PROFILE'] = 'data228'

    return SparkSession.builder \
        .appName("StreamTide-FrequencyMoments") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()


def compute_frequency_moments(spark, df, output_path):
    """Compute frequency moments"""

    print("\n" + "="*70)
    print("COMPUTING FREQUENCY MOMENTS")
    print("="*70)

    # Zone frequency distribution
    zone_freq = df.groupBy('PULocationID').count().withColumnRenamed('count', 'frequency')

    # F0: Distinct elements
    f0 = zone_freq.count()

    # F1: Total count (sum of all frequencies)
    f1 = zone_freq.select(sum('frequency')).collect()[0][0]

    # F2: Sum of squared frequencies (surprise number)
    f2 = zone_freq.select(sum(col('frequency') * col('frequency'))).collect()[0][0]

    results = {
        'F0': f0,  # Distinct zones
        'F1': f1,  # Total trips
        'F2': f2,  # Surprise number
        'gini_coefficient': (f2 ** 0.5) / f1 if f1 > 0 else 0
    }

    # Top zones
    top_zones = zone_freq.orderBy(desc('frequency')).limit(20).collect()
    results['top_zone_frequencies'] = [
        {'zone': row['PULocationID'], 'frequency': row['frequency']}
        for row in top_zones
    ]

    print(f"\n  ✓ F0 (distinct zones): {f0}")
    print(f"  ✓ F1 (total trips): {f1:,}")
    print(f"  ✓ F2 (surprise number): {f2:,}")
    print(f"  ✓ Gini coefficient: {results['gini_coefficient']:.4f}")
    print(f"  ✓ Top zone: {top_zones[0]['PULocationID']} ({top_zones[0]['frequency']:,} trips)")

    # Save
    print("\n💾 Saving frequency moments...")
    moments_json = json.dumps({'frequency_moments': results}, indent=2, default=str)
    spark.sparkContext.parallelize([moments_json]).coalesce(1).saveAsTextFile(
        f"{output_path}/frequency_moments"
    )
    print(f"  ✓ Saved to: {output_path}/frequency_moments/")

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

            dfcount = df.count()
            dataframes.append(df)
            print(f"  ✓ {taxi_type}: {dfcount:,} records")
        except Exception as e:
            print(f"  ⚠ {taxi_type}: {str(e)[:100]}")

    if not dataframes:
        raise Exception("No data loaded!")

    # Union all
    df_all = dataframes[0]
    for df in dataframes[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)

    # Cast location IDs
    df_all = df_all.withColumn("PULocationID", col("PULocationID").cast("int"))
    
    # Now we can use standardized pickup_datetime column
    df_all = df_all.filter(year(col('pickup_datetime')) < 2025)


    total = df_all.count()
    print(f"\n✅ Total: {total:,} records (excluding 2025)")

    return df_all


def main():
    OUTPUT_PATH = "s3a://data228-bigdata-nyc/ground-truth-enhanced"

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = load_data(spark)
        results = compute_frequency_moments(spark, df, OUTPUT_PATH)

        print("\n✅ Frequency moments complete!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
