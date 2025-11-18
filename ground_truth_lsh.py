#!/usr/bin/env python3
"""
Ground Truth: LSH (Route Frequency Analysis)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, year
import json
import os


def create_spark_session():
    """Create Spark session"""
    os.environ['AWS_PROFILE'] = 'data228'

    return SparkSession.builder \
        .appName("StreamTide-LSH") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()


def compute_lsh(spark, df, output_path):
    """Compute route frequencies"""

    print("\n" + "="*70)
    print("COMPUTING LSH METRICS (Route Frequencies)")
    print("="*70)

    # Filter out NULL and invalid location IDs
    total_before = df.count()
    df_valid = df.filter(
        (col('PULocationID').isNotNull()) &
        (col('DOLocationID').isNotNull()) &
        (col('PULocationID') > 0) &
        (col('DOLocationID') > 0)
    )
    total_after = df_valid.count()

    print(f"\n📊 Data quality:")
    print(f"  - Total trips: {total_before:,}")
    print(f"  - Valid location IDs: {total_after:,} ({total_after/total_before*100:.1f}%)")
    print(f"  - Filtered out: {total_before - total_after:,}")

    # Compute route frequencies
    route_freq = df_valid.groupBy('PULocationID', 'DOLocationID') \
        .count() \
        .withColumnRenamed('count', 'frequency') \
        .orderBy(desc('frequency')) \
        .limit(100)

    top_routes = route_freq.collect()

    results = {
        'top_routes': [
            {
                'pickup_zone': row['PULocationID'],
                'dropoff_zone': row['DOLocationID'],
                'frequency': row['frequency']
            }
            for row in top_routes
        ]
    }

    print(f"\n  ✓ Analyzed route pairs")
    print(f"  ✓ Top route: Zone {top_routes[0]['PULocationID']} → {top_routes[0]['DOLocationID']} ({top_routes[0]['frequency']:,} trips)")
    if len(top_routes) > 1:
        print(f"  ✓ 2nd route: Zone {top_routes[1]['PULocationID']} → {top_routes[1]['DOLocationID']} ({top_routes[1]['frequency']:,} trips)")
    if len(top_routes) > 2:
        print(f"  ✓ 3rd route: Zone {top_routes[2]['PULocationID']} → {top_routes[2]['DOLocationID']} ({top_routes[2]['frequency']:,} trips)")

    # Save
    print("\n💾 Saving LSH metrics...")
    lsh_json = json.dumps({'lsh_routes': results}, indent=2, default=str)
    spark.sparkContext.parallelize([lsh_json]).coalesce(1).saveAsTextFile(
        f"{output_path}/lsh_routes"
    )
    print(f"  ✓ Saved to: {output_path}/lsh_routes/")

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

    # Cast location IDs
    df_all = df_all.withColumn("PULocationID", col("PULocationID").cast("int")) \
                   .withColumn("DOLocationID", col("DOLocationID").cast("int"))

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
        results = compute_lsh(spark, df, OUTPUT_PATH)

        print("\n✅ LSH route analysis complete!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
