#!/usr/bin/env python3
"""
Ground Truth: Cardinality / Distinct Counts (HyperLogLog Validation)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, year
import json
import os


def create_spark_session():
    """Create Spark session"""
    os.environ['AWS_PROFILE'] = 'data228'

    return SparkSession.builder \
        .appName("StreamTide-Cardinality") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.files.ignoreCorruptFiles", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()


def compute_cardinality(spark, df, output_path):
    """Compute distinct counts"""

    print("\n" + "="*70)
    print("COMPUTING CARDINALITY (HyperLogLog)")
    print("="*70)

    available_columns = df.columns
    has_vendor = 'VendorID' in available_columns
    has_payment = 'payment_type' in available_columns
    # has_payment = False  # Disable payment type due to inconsistent types

    print(f"\n📋 Checking columns:")
    print(f"  - VendorID: {'✓' if has_vendor else '✗'}")
    print(f"  - payment_type: {'✓' if has_payment else '✗'}")

    # Build cardinality query
    cardinality_exprs = [
        countDistinct(col('PULocationID')).alias('distinct_pu_zones'),
        countDistinct(col('DOLocationID')).alias('distinct_do_zones')
    ]

    if has_vendor:
        cardinality_exprs.append(countDistinct(col('VendorID')).alias('distinct_vendors'))
    if has_payment:
        df = df.withColumn("payment_type", col("payment_type").cast("string"))
        cardinality_exprs.append(countDistinct(col('payment_type')).alias('distinct_payment_types'))

    distinct_zones = df.select(*cardinality_exprs).collect()[0]

    # Total distinct zones (union of pickup and dropoff)
    pu_zones = df.select('PULocationID').distinct()
    do_zones = df.select('DOLocationID').distinct()
    total_distinct_zones = pu_zones.union(do_zones).distinct().count()

    results = {
        'distinct_pickup_zones': distinct_zones['distinct_pu_zones'],
        'distinct_dropoff_zones': distinct_zones['distinct_do_zones'],
        'distinct_total_zones': total_distinct_zones
    }

    if has_vendor:
        results['distinct_vendors'] = distinct_zones['distinct_vendors']
        # Debug
        non_null_vendors = df.filter(col('VendorID').isNotNull()).count()
        results['non_null_vendor_count'] = non_null_vendors
    if has_payment:
        results['distinct_payment_types'] = distinct_zones['distinct_payment_types']

    print(f"\n  ✓ Distinct pickup zones: {results['distinct_pickup_zones']}")
    print(f"  ✓ Distinct dropoff zones: {results['distinct_dropoff_zones']}")
    print(f"  ✓ Distinct total zones: {results['distinct_total_zones']}")
    if has_vendor:
        print(f"  ✓ Distinct vendors: {results['distinct_vendors']}")
        print(f"  ℹ️  Non-null vendor records: {results['non_null_vendor_count']:,}")

    # Save
    print("\n💾 Saving cardinality metrics...")
    card_json = json.dumps({'cardinality': results}, indent=2, default=str)
    spark.sparkContext.parallelize([card_json]).coalesce(1).saveAsTextFile(
        f"{output_path}/cardinality"
    )
    print(f"  ✓ Saved to: {output_path}/cardinality/")

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


            df.printSchema()

            count = df.count()
            dataframes.append(df)
            print(f"  ✓ {taxi_type}: {count:,} records: {df.columns}")
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
                   .withColumn("DOLocationID", col("DOLocationID").cast("int")) \
                   .withColumn("payment_type", col("payment_type").cast("string"))

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
        results = compute_cardinality(spark, df, OUTPUT_PATH)

        print("\n✅ Cardinality complete!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
