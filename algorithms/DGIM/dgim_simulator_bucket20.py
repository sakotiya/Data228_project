#!/usr/bin/env python3
"""
DGIM Stream Simulator - S3 Edition
Focuses on sliding window analysis of binary streams
Tracks busy periods, surge pricing, high-value trips in real-time

Reads data from S3, processes with DGIM algorithm, writes results to S3
"""

import time
import json
from datetime import datetime
import sys
import os
import argparse
import random
from typing import List

import boto3
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
#from dotenv import load_dotenv

# Import DGIM algorithm
sys.path.append(os.path.dirname(__file__))
from dgim import DGIM


def list_parquet_keys(bucket_name: str, prefix: str, profile: str, year: str = None, num_months: int = None) -> List[str]:
    """List all parquet keys under the given S3 prefix, optionally filtered by year and random months."""
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.resource("s3")
    bucket = s3.Bucket(bucket_name)

    parquet_keys = [
        obj.key
        for obj in bucket.objects.filter(Prefix=prefix)
        if obj.key.endswith(".parquet")
    ]

    # Filter by year if specified
    if year:
        parquet_keys = [k for k in parquet_keys if f"_{year}-" in k or f"_{year}." in k]
        print(f"  Filtered to {len(parquet_keys)} files for year {year}")

    # Randomly select months if specified
    if num_months and len(parquet_keys) > num_months:
        # Group files by month
        month_files = {}
        for key in parquet_keys:
            # Extract month from filename like "yellow_tripdata_2025-01.parquet"
            for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
                if f"-{month}." in key or f"-{month}_" in key:
                    if month not in month_files:
                        month_files[month] = []
                    month_files[month].append(key)
                    break

        # Randomly select months
        available_months = list(month_files.keys())
        if len(available_months) > num_months:
            selected_months = random.sample(available_months, num_months)
        else:
            selected_months = available_months

        # Get all files for selected months
        parquet_keys = []
        for month in sorted(selected_months):
            parquet_keys.extend(month_files[month])

        print(f"  Randomly selected months: {sorted(selected_months)}")
        print(f"  Total files for selected months: {len(parquet_keys)}")

    return parquet_keys


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names across different taxi types"""
    df = df.copy()

    # Fare columns
    if 'Fare_Amt' in df.columns:
        df['fare_amount'] = df['Fare_Amt']
    if 'base_passenger_fare' in df.columns:
        df['fare_amount'] = df['base_passenger_fare']

    # Distance columns
    if 'Trip_Distance' in df.columns:
        df['trip_distance'] = df['Trip_Distance']
    if 'trip_miles' in df.columns:
        df['trip_distance'] = df['trip_miles']

    # Tip columns
    if 'Tip_Amt' in df.columns:
        df['tip_amount'] = df['Tip_Amt']
    if 'tips' in df.columns:
        df['tip_amount'] = df['tips']

    # Total amount columns
    if 'Total_Amt' in df.columns:
        df['total_amount'] = df['Total_Amt']

    # Passenger count
    if 'Passenger_Count' in df.columns:
        df['passenger_count'] = df['Passenger_Count']

    # Location ID columns
    if 'PUlocationID' in df.columns:
        df['PULocationID'] = df['PUlocationID']
    if 'DOlocationID' in df.columns:
        df['DOLocationID'] = df['DOlocationID']

    return df


def load_from_s3(
    bucket_name: str,
    keys: List[str],
    profile: str,
) -> pd.DataFrame:
    """
    Load a set of parquet keys from S3 into a single pandas DataFrame.
    """
    storage_options = {"profile": profile} if profile else {}

    dfs = []
    for key in keys:
        s3_path = f"s3://{bucket_name}/{key}"
        print(f"Reading {s3_path}")
        df_part = pd.read_parquet(s3_path, storage_options=storage_options)

        # Standardize column names
        df_part = standardize_columns(df_part)

        # Debug: print columns for first file
        if len(dfs) == 0:
            print(f"  Columns after standardization: {list(df_part.columns)[:8]}...")
            print(f"  Has fare_amount: {'fare_amount' in df_part.columns}")
            print(f"  Has trip_distance: {'trip_distance' in df_part.columns}")

        dfs.append(df_part)

    if not dfs:
        raise RuntimeError("No data loaded from S3 (no parquet files found or all empty).")

    df = pd.concat(dfs, ignore_index=True)
    print(f"Loaded {len(df)} rows from {len(keys)} parquet files")

    # Print sample values
    if 'fare_amount' in df.columns:
        print(f"  Sample fares: {df['fare_amount'].head(5).tolist()}")
        print(f"  Max fare: ${df['fare_amount'].max():.2f}")
        print(f"  Fares > $20: {(df['fare_amount'] > 20).sum():,}")

    return df


def write_json_to_s3(
    data: dict,
    bucket_name: str,
    key: str,
    profile: str,
) -> str:
    """Write a JSON dictionary to S3."""
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3")

    s3_path = f"s3://{bucket_name}/{key}"
    print(f"Writing results to {s3_path}")

    json_str = json.dumps(data, indent=2, default=str)
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json_str.encode('utf-8'),
        ContentType='application/json'
    )

    return s3_path


def create_spark_session(app_name: str = "DGIM_Simulator") -> SparkSession:
    """Create a local Spark session optimized for M4 Mac with 16GB RAM."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[10]") \
        .config("spark.driver.memory", "12g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.default.parallelism", "20") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.0,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.683") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_and_preprocess_with_spark(
    spark: SparkSession,
    bucket_name: str,
    keys: List[str],
    airport_zones: set
) -> pd.DataFrame:
    """
    Load parquet files with Spark and compute binary conditions in parallel.
    Returns a pandas DataFrame with only the binary columns needed for DGIM.
    """
    # Group files by taxi type
    taxi_files = {'yellow': [], 'green': [], 'fhv': [], 'fhvhv': []}
    for key in keys:
        for taxi_type in taxi_files.keys():
            if f"{taxi_type}_tripdata" in key:
                taxi_files[taxi_type].append(f"s3a://{bucket_name}/{key}")
                break

    print(f"Loading files by taxi type with Spark...")
    for taxi_type, files in taxi_files.items():
        if files:
            print(f"  {taxi_type}: {len(files)} files")

    # Load and standardize each taxi type separately, then union
    dataframes = []

    for taxi_type, files in taxi_files.items():
        if not files:
            continue

        try:
            df = spark.read.parquet(*files)
            count = df.count()
            print(f"  ✓ {taxi_type}: {count:,} records")

            # Standardize column names for this taxi type

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

            # Tip columns
            if 'Tip_Amt' in df.columns:
                df = df.withColumnRenamed('Tip_Amt', 'tip_amount')
            if 'tips' in df.columns:
                df = df.withColumnRenamed('tips', 'tip_amount')

            # Total amount
            if 'Total_Amt' in df.columns:
                df = df.withColumnRenamed('Total_Amt', 'total_amount')

            # Passenger count
            if 'Passenger_Count' in df.columns:
                df = df.withColumnRenamed('Passenger_Count', 'passenger_count')

            # Location ID columns (fix FHV casing)
            if 'PUlocationID' in df.columns:
                df = df.withColumnRenamed('PUlocationID', 'PULocationID')
            if 'DOlocationID' in df.columns:
                df = df.withColumnRenamed('DOlocationID', 'DOLocationID')

            # Cast location IDs to int
            if 'PULocationID' in df.columns:
                df = df.withColumn('PULocationID', F.col('PULocationID').cast('int'))
            if 'DOLocationID' in df.columns:
                df = df.withColumn('DOLocationID', F.col('DOLocationID').cast('int'))

            dataframes.append(df)

        except Exception as e:
            print(f"  ⚠️  {taxi_type}: {str(e)[:100]}")

    if not dataframes:
        raise RuntimeError("No data loaded from any taxi type!")

    # Union all dataframes with allowMissingColumns
    df = dataframes[0]
    for other_df in dataframes[1:]:
        df = df.unionByName(other_df, allowMissingColumns=True)

    total_rows = df.count()
    print(f"\nTotal: {total_rows:,} rows after union")

    # Add missing columns with default values
    if 'fare_amount' not in df.columns:
        df = df.withColumn('fare_amount', F.lit(0.0))
    if 'trip_distance' not in df.columns:
        df = df.withColumn('trip_distance', F.lit(0.0))
    if 'tip_amount' not in df.columns:
        df = df.withColumn('tip_amount', F.lit(0.0))
    if 'total_amount' not in df.columns:
        df = df.withColumn('total_amount', F.lit(1.0))
    if 'passenger_count' not in df.columns:
        df = df.withColumn('passenger_count', F.lit(0))
    if 'PULocationID' not in df.columns:
        df = df.withColumn('PULocationID', F.lit(0))
    if 'DOLocationID' not in df.columns:
        df = df.withColumn('DOLocationID', F.lit(0))

    # Fill remaining nulls with defaults
    df = df.fillna({
        'fare_amount': 0.0,
        'trip_distance': 0.0,
        'tip_amount': 0.0,
        'total_amount': 1.0,
        'passenger_count': 0,
        'PULocationID': 0,
        'DOLocationID': 0
    })

    # Compute binary conditions in parallel using Spark
    print("Computing binary conditions with Spark...")

    # High fare (fare > $20)
    df = df.withColumn('is_high_fare',
        F.when(F.col('fare_amount') > 20, 1).otherwise(0).cast(IntegerType()))

    # Long distance (distance > 5 miles)
    df = df.withColumn('is_long_distance',
        F.when(F.col('trip_distance') > 5, 1).otherwise(0).cast(IntegerType()))

    # High tip (tip > 20% of total)
    df = df.withColumn('is_high_tip',
        F.when(
            (F.col('total_amount') > 0) &
            ((F.col('tip_amount') / F.col('total_amount') * 100) > 20),
            1
        ).otherwise(0).cast(IntegerType()))

    # Premium rides (3+ passengers)
    df = df.withColumn('is_premium',
        F.when(F.col('passenger_count') >= 3, 1).otherwise(0).cast(IntegerType()))

    # Airport trips
    airport_list = list(airport_zones)
    df = df.withColumn('is_airport',
        F.when(
            F.col('PULocationID').isin(airport_list) |
            F.col('DOLocationID').isin(airport_list),
            1
        ).otherwise(0).cast(IntegerType()))

    # Select only binary columns and collect to pandas
    binary_df = df.select(
        'is_high_fare',
        'is_long_distance',
        'is_high_tip',
        'is_premium',
        'is_airport'
    ).toPandas()

    print(f"Preprocessed {len(binary_df):,} records")

    return binary_df


class DGIMStreamProcessor:
    """Process streaming data with multiple DGIM windows"""

    def __init__(self, window_size=1000):
        # Multiple DGIM instances for different binary conditions

        # Track high-fare trips (fare > $20)
        self.dgim_high_fare = DGIM(window_size=window_size)

        # Track long-distance trips (distance > 5 miles)
        self.dgim_long_distance = DGIM(window_size=window_size)

        # Track high tip percentage (tip > 20%)
        self.dgim_high_tip = DGIM(window_size=window_size)

        # Track premium rides (passenger_count >= 3)
        self.dgim_premium = DGIM(window_size=window_size)

        # Track airport trips (specific zone IDs)
        self.dgim_airport = DGIM(window_size=window_size)

        # Airport zone IDs (JFK, LaGuardia, Newark in NYC)
        self.airport_zones = {132, 138, 1}

        # True counts for accuracy validation
        self.true_counts = {
            'high_fare': [],
            'long_distance': [],
            'high_tip': [],
            'premium': [],
            'airport': []
        }

        # Statistics
        self.stats = {
            'start_time': datetime.now(),
            'total_records': 0,
            'high_fare_trips': 0,
            'long_distance_trips': 0,
            'high_tip_trips': 0,
            'premium_trips': 0,
            'airport_trips': 0
        }

        self.window_size = window_size

        # Store snapshots for last 20 windows
        self.window_snapshots = []

    def process_binary_batch(self, binary_df: pd.DataFrame):
        """
        Process preprocessed binary data efficiently using numpy arrays.
        Much faster than row-by-row iteration.
        """
        # Convert to numpy arrays for speed
        high_fare = binary_df['is_high_fare'].values
        long_distance = binary_df['is_long_distance'].values
        high_tip = binary_df['is_high_tip'].values
        premium = binary_df['is_premium'].values
        airport = binary_df['is_airport'].values

        total_records = len(binary_df)
        print(f"\n📊 Processing {total_records:,} records with optimized DGIM...")

        # Calculate snapshot interval for 20 windows
        snapshot_interval = max(1, total_records // 20)

        # Process each record
        for i in range(total_records):
            # Add bits to DGIM windows
            self.dgim_high_fare.add_bit(int(high_fare[i]))
            self.dgim_long_distance.add_bit(int(long_distance[i]))
            self.dgim_high_tip.add_bit(int(high_tip[i]))
            self.dgim_premium.add_bit(int(premium[i]))
            self.dgim_airport.add_bit(int(airport[i]))

            # Track true counts (keep only last window_size)
            self.true_counts['high_fare'].append(int(high_fare[i]))
            self.true_counts['long_distance'].append(int(long_distance[i]))
            self.true_counts['high_tip'].append(int(high_tip[i]))
            self.true_counts['premium'].append(int(premium[i]))
            self.true_counts['airport'].append(int(airport[i]))

            # Trim to window size periodically (every 10000 records)
            if (i + 1) % 10000 == 0:
                for key in self.true_counts:
                    if len(self.true_counts[key]) > self.window_size:
                        self.true_counts[key] = self.true_counts[key][-self.window_size:]
                print(f"  Processed {i + 1:,} records...")

            # Capture snapshot at regular intervals (20 snapshots total)
            if (i + 1) % snapshot_interval == 0 and len(self.window_snapshots) < 20:
                snapshot = {
                    'record_num': i + 1,
                    'high_fare': {
                        'estimated': self.dgim_high_fare.count_ones(),
                        'actual': sum(self.true_counts['high_fare'][-self.window_size:]),
                        'buckets': len(self.dgim_high_fare.buckets)
                    },
                    'long_distance': {
                        'estimated': self.dgim_long_distance.count_ones(),
                        'actual': sum(self.true_counts['long_distance'][-self.window_size:]),
                        'buckets': len(self.dgim_long_distance.buckets)
                    },
                    'high_tip': {
                        'estimated': self.dgim_high_tip.count_ones(),
                        'actual': sum(self.true_counts['high_tip'][-self.window_size:]),
                        'buckets': len(self.dgim_high_tip.buckets)
                    },
                    'premium': {
                        'estimated': self.dgim_premium.count_ones(),
                        'actual': sum(self.true_counts['premium'][-self.window_size:]),
                        'buckets': len(self.dgim_premium.buckets)
                    },
                    'airport': {
                        'estimated': self.dgim_airport.count_ones(),
                        'actual': sum(self.true_counts['airport'][-self.window_size:]),
                        'buckets': len(self.dgim_airport.buckets)
                    }
                }
                self.window_snapshots.append(snapshot)

        # Final trim
        for key in self.true_counts:
            if len(self.true_counts[key]) > self.window_size:
                self.true_counts[key] = self.true_counts[key][-self.window_size:]

        # Update cumulative stats
        self.stats['total_records'] = total_records
        self.stats['high_fare_trips'] = int(np.sum(high_fare))
        self.stats['long_distance_trips'] = int(np.sum(long_distance))
        self.stats['high_tip_trips'] = int(np.sum(high_tip))
        self.stats['premium_trips'] = int(np.sum(premium))
        self.stats['airport_trips'] = int(np.sum(airport))

    def process_record(self, row):
        """Process a single record and update DGIM windows"""
        # Extract fields
        fare = float(row.get('fare_amount', 0))
        distance = float(row.get('trip_distance', 0))
        tip = float(row.get('tip_amount', 0))
        total = float(row.get('total_amount', 1))
        passenger_count = int(row.get('passenger_count', 0))
        pu_zone = int(row.get('PULocationID', 0))
        do_zone = int(row.get('DOLocationID', 0))

        # Calculate conditions
        is_high_fare = 1 if fare > 20 else 0
        is_long_distance = 1 if distance > 5 else 0
        tip_percentage = (tip / total * 100) if total > 0 else 0
        is_high_tip = 1 if tip_percentage > 20 else 0
        is_premium = 1 if passenger_count >= 3 else 0
        is_airport = 1 if (pu_zone in self.airport_zones or do_zone in self.airport_zones) else 0

        # Add to DGIM windows
        self.dgim_high_fare.add_bit(is_high_fare)
        self.dgim_long_distance.add_bit(is_long_distance)
        self.dgim_high_tip.add_bit(is_high_tip)
        self.dgim_premium.add_bit(is_premium)
        self.dgim_airport.add_bit(is_airport)

        # Track true counts
        self.true_counts['high_fare'].append(is_high_fare)
        self.true_counts['long_distance'].append(is_long_distance)
        self.true_counts['high_tip'].append(is_high_tip)
        self.true_counts['premium'].append(is_premium)
        self.true_counts['airport'].append(is_airport)

        # Keep only last window_size elements
        for key in self.true_counts:
            if len(self.true_counts[key]) > self.window_size:
                self.true_counts[key] = self.true_counts[key][-self.window_size:]

        # Update stats
        self.stats['total_records'] += 1
        self.stats['high_fare_trips'] += is_high_fare
        self.stats['long_distance_trips'] += is_long_distance
        self.stats['high_tip_trips'] += is_high_tip
        self.stats['premium_trips'] += is_premium
        self.stats['airport_trips'] += is_airport

    def print_window_analysis(self):
        """Print sliding window analysis from all DGIM instances"""
        print(f"\n{'='*70}")
        print(f"📊 DGIM SLIDING WINDOW ANALYSIS")
        print(f"{'='*70}")

        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()

        print(f"\n⏱️  Performance:")
        print(f"   ├─ Runtime: {elapsed:.1f}s")
        print(f"   ├─ Total Records: {self.stats['total_records']:,}")
        print(f"   └─ Throughput: {self.stats['total_records']/elapsed:.0f} records/sec")

        print(f"\n🪟 Window Configuration:")
        print(f"   ├─ Window Size: {self.window_size} trips")
        print(f"   └─ Memory per DGIM: ~{len(self.dgim_high_fare.buckets) * 8} bytes")

        # Analyze each condition
        conditions = {
            'High Fare (>$20)': ('high_fare', self.dgim_high_fare),
            'Long Distance (>5mi)': ('long_distance', self.dgim_long_distance),
            'High Tip (>20%)': ('high_tip', self.dgim_high_tip),
            'Premium (3+ passengers)': ('premium', self.dgim_premium),
            'Airport Trips': ('airport', self.dgim_airport)
        }

        # Print all 20 window snapshots
        if self.window_snapshots:
            print(f"\n🎯 Window Analysis History (20 Snapshots, Window Size: {self.window_size}):")
            print(f"\n{'='*100}")

            # Print header
            print(f"{'Window':<10} {'Metric':<25} {'Estimated':<12} {'Actual':<12} {'Error %':<10} {'Buckets':<8}")
            print(f"{'-'*100}")

            for idx, snapshot in enumerate(self.window_snapshots):
                record_num = snapshot['record_num']

                # Print each metric for this window
                metrics = [
                    ('High Fare (>$20)', 'high_fare'),
                    ('Long Distance (>5mi)', 'long_distance'),
                    ('High Tip (>20%)', 'high_tip'),
                    ('Premium (3+ pass)', 'premium'),
                    ('Airport Trips', 'airport')
                ]

                for i, (label, key) in enumerate(metrics):
                    data = snapshot[key]
                    estimated = data['estimated']
                    actual = data['actual']
                    error = abs(estimated - actual) / actual * 100 if actual > 0 else 0
                    buckets = data['buckets']

                    if i == 0:
                        print(f"#{idx+1:<8} {label:<25} {estimated:<12} {actual:<12} {error:<9.2f}% {buckets:<8}")
                    else:
                        print(f"{'':10} {label:<25} {estimated:<12} {actual:<12} {error:<9.2f}% {buckets:<8}")

                print(f"{'-'*100}")

        # Also print final window state
        print(f"\n🎯 Final Window Analysis (Last {self.window_size} Trips):")
        for label, (key, dgim) in conditions.items():
            estimated = dgim.count_ones()
            true_count = sum(self.true_counts[key][-self.window_size:]) if len(self.true_counts[key]) > 0 else 0
            error = abs(estimated - true_count) / true_count * 100 if true_count > 0 else 0
            percentage = (estimated / self.window_size * 100) if self.window_size > 0 else 0

            print(f"\n   {label}:")
            print(f"   ├─ Estimated Count: ~{estimated}")
            print(f"   ├─ Actual Count: {true_count}")
            print(f"   ├─ Error: {error:.2f}%")
            print(f"   ├─ Window %: {percentage:.1f}%")
            print(f"   └─ Buckets: {len(dgim.buckets)}")

        # Cumulative statistics (all-time)
        print(f"\n📈 Cumulative Statistics (All-Time):")
        print(f"   ├─ High Fare Trips: {self.stats['high_fare_trips']:,}")
        print(f"   ├─ Long Distance Trips: {self.stats['long_distance_trips']:,}")
        print(f"   ├─ High Tip Trips: {self.stats['high_tip_trips']:,}")
        print(f"   ├─ Premium Trips: {self.stats['premium_trips']:,}")
        print(f"   └─ Airport Trips: {self.stats['airport_trips']:,}")

    def save_results_to_s3(self, bucket: str, prefix: str, profile: str = ""):
        """Save DGIM results to S3"""
        print(f"\n📤 Saving DGIM results to S3...")

        # Calculate window estimates
        window_estimates = {
            'high_fare': {
                'estimated': self.dgim_high_fare.count_ones(),
                'actual': sum(self.true_counts['high_fare'][-self.window_size:]),
                'buckets': len(self.dgim_high_fare.buckets)
            },
            'long_distance': {
                'estimated': self.dgim_long_distance.count_ones(),
                'actual': sum(self.true_counts['long_distance'][-self.window_size:]),
                'buckets': len(self.dgim_long_distance.buckets)
            },
            'high_tip': {
                'estimated': self.dgim_high_tip.count_ones(),
                'actual': sum(self.true_counts['high_tip'][-self.window_size:]),
                'buckets': len(self.dgim_high_tip.buckets)
            },
            'premium': {
                'estimated': self.dgim_premium.count_ones(),
                'actual': sum(self.true_counts['premium'][-self.window_size:]),
                'buckets': len(self.dgim_premium.buckets)
            },
            'airport': {
                'estimated': self.dgim_airport.count_ones(),
                'actual': sum(self.true_counts['airport'][-self.window_size:]),
                'buckets': len(self.dgim_airport.buckets)
            }
        }

        # Calculate accuracy metrics
        accuracy = {}
        for key, data in window_estimates.items():
            estimated = data['estimated']
            actual = data['actual']
            error = abs(estimated - actual) / actual * 100 if actual > 0 else 0
            accuracy[key] = {
                'error_percentage': round(error, 3),
                'absolute_error': abs(estimated - actual)
            }

        # Process window snapshots for S3 (add error percentages)
        processed_snapshots = []
        for snapshot in self.window_snapshots:
            processed = {'record_num': snapshot['record_num']}
            for key in ['high_fare', 'long_distance', 'high_tip', 'premium', 'airport']:
                data = snapshot[key]
                estimated = data['estimated']
                actual = data['actual']
                error = abs(estimated - actual) / actual * 100 if actual > 0 else 0
                processed[key] = {
                    'estimated': estimated,
                    'actual': actual,
                    'error_percentage': round(error, 2),
                    'buckets': data['buckets']
                }
            processed_snapshots.append(processed)

        # Compile summary
        summary = {
            'run_metadata': {
                'start_time': str(self.stats['start_time']),
                'end_time': str(datetime.now()),
                'duration_seconds': (datetime.now() - self.stats['start_time']).total_seconds(),
                'total_records': self.stats['total_records']
            },
            'window_configuration': {
                'window_size': self.window_size,
                'num_dgim_instances': 5,
                'num_snapshots': len(self.window_snapshots)
            },
            'window_snapshots': processed_snapshots,
            'final_window_estimates': window_estimates,
            'accuracy_metrics': accuracy,
            'cumulative_stats': {
                'high_fare_trips': self.stats['high_fare_trips'],
                'long_distance_trips': self.stats['long_distance_trips'],
                'high_tip_trips': self.stats['high_tip_trips'],
                'premium_trips': self.stats['premium_trips'],
                'airport_trips': self.stats['airport_trips']
            }
        }

        # Get individual DGIM stats
        dgim_stats = {
            'high_fare': self.dgim_high_fare.get_statistics(),
            'long_distance': self.dgim_long_distance.get_statistics(),
            'high_tip': self.dgim_high_tip.get_statistics(),
            'premium': self.dgim_premium.get_statistics(),
            'airport': self.dgim_airport.get_statistics()
        }

        # Save individual DGIM stats to S3
        for key, stats in dgim_stats.items():
            s3_key = f"{prefix}/{key}_dgim.json"
            write_json_to_s3(stats, bucket, s3_key, profile)

        # Save complete summary to S3
        summary_key = f"{prefix}/dgim_summary.json"
        write_json_to_s3(summary, bucket, summary_key, profile)

        print(f"\n✅ All DGIM results saved to s3://{bucket}/{prefix}/")
        return f"s3://{bucket}/{prefix}/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run DGIM sliding window analysis on S3 data."
    )

    # S3 options
    parser.add_argument(
        "--bucket",
        default="data228-bigdata-nyc",
        help="S3 bucket name (default: data228-bigdata-nyc)",
    )
    parser.add_argument(
        "--input-prefix",
        default="staging/2025/",
        help="S3 prefix for input parquet files (default: staging/2025/)",
    )
    parser.add_argument(
        "--output-prefix",
        default="dgim_analysis/",
        help="S3 prefix for output (default: dgim_analysis/)",
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=None,
        help="Number of parquet files to sample from S3 (default: all files)",
    )
    parser.add_argument(
        "--year",
        default=None,
        help="Filter data by year (e.g., 2025)",
    )
    parser.add_argument(
        "--num-months",
        type=int,
        default=None,
        help="Randomly select this many months from the year",
    )
    parser.add_argument(
        "--aws-profile",
        default="",
        help="AWS named profile to use. Leave empty for default credentials.",
    )

    # Algorithm options
    parser.add_argument(
        "--window-size",
        type=int,
        default=10000,
        help="DGIM sliding window size (default: 10000)",
    )

    return parser.parse_args()


def main():
    """Main DGIM streaming simulation"""

    # Load environment variables (commented out - using AWS profile instead)
    # env_path = os.path.join(os.path.dirname(__file__), ".env")
    # if os.path.exists(env_path):
    #     load_dotenv(env_path)

    args = parse_args()

    print(f"""
╔════════════════════════════════════════════════════════════════════╗
║           StreamTide DGIM Sliding Window Analyzer - S3            ║
╚════════════════════════════════════════════════════════════════════╝
    Algorithm: DGIM (Datar-Gionis-Indyk-Motwani)
    Window Size: {args.window_size} trips
    Purpose: Real-time sliding window analysis
════════════════════════════════════════════════════════════════════
    """)

    print(f"📦 S3 Configuration:")
    print(f"    Bucket: {args.bucket}")
    print(f"    Input prefix: {args.input_prefix}")
    print(f"    Output prefix: {args.output_prefix}")
    print(f"    AWS profile: {args.aws_profile or '(default)'}")
    if args.year:
        print(f"    Year filter: {args.year}")
    if args.num_months:
        print(f"    Random months: {args.num_months}")
    if args.num_files:
        print(f"    Max files: {args.num_files}")
    print()

    spark = None
    try:
        # Create Spark session
        print(f"🚀 Initializing Spark session...")
        spark = create_spark_session("DGIM_Simulator")
        print(f"   Spark UI: http://localhost:4040")

        # List parquet files from S3
        print(f"\n📋 Listing parquet files from S3...")
        parquet_keys = list_parquet_keys(
            args.bucket,
            args.input_prefix,
            args.aws_profile,
            year=args.year,
            num_months=args.num_months
        )

        if not parquet_keys:
            raise RuntimeError(f"No parquet files found under s3://{args.bucket}/{args.input_prefix}")

        print(f"Found {len(parquet_keys)} parquet files")

        # Optionally limit number of files
        if args.num_files and len(parquet_keys) > args.num_files:
            sample_keys = parquet_keys[:args.num_files]
        else:
            sample_keys = parquet_keys

        print(f"\nUsing {len(sample_keys)} files:")
        for k in sample_keys[:5]:
            print(f"  - {k}")
        if len(sample_keys) > 5:
            print(f"  ... and {len(sample_keys) - 5} more")
        print()

        # Create DGIM processor (need airport_zones for preprocessing)
        processor = DGIMStreamProcessor(window_size=args.window_size)

        # Load and preprocess with Spark
        binary_df = load_and_preprocess_with_spark(
            spark,
            args.bucket,
            sample_keys,
            processor.airport_zones
        )

        # Process with DGIM using optimized batch method
        processor.process_binary_batch(binary_df)

        # Print final analysis
        processor.print_window_analysis()

        # Save results to S3
        output_location = processor.save_results_to_s3(
            bucket=args.bucket,
            prefix=args.output_prefix,
            profile=args.aws_profile
        )

        print(f"\n✨ DGIM S3 analysis complete!")
        print(f"📂 Results: {output_location}")

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        # Clean up Spark session
        if spark:
            print("\n🛑 Stopping Spark session...")
            spark.stop()


if __name__ == "__main__":
    main()
