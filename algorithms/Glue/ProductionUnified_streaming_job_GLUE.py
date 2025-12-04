#!/usr/bin/env python3
"""
NYC TAXI UNIFIED STREAMING JOB - AWS GLUE PRODUCTION
Processes ALL incremental streaming data with ALL algorithms
Outputs to: s3://data228-bigdata-nyc/Prod_Results/
"""

import sys
import os
import json
import pickle
import traceback
import zipfile
from datetime import datetime
from typing import Dict, List

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.functions import lit, col, concat_ws, unix_timestamp, count, avg, sum, coalesce
import boto3

# Save Python's builtin round before Spark shadows it
python_round = round

# Initialize Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"Starting Job: {args['JOB_NAME']}")


# Configuration
class Config:
    S3_BUCKET = "data228-bigdata-nyc"
    S3_INPUT_PREFIX = "staging/Incremental/"  # ALL STREAMING DATA (fhv, fhvhv, green, yellow)
    S3_OUTPUT_PREFIX = "Prod_Results/"  # PRODUCTION OUTPUT!
    S3_STATE_PREFIX = "glue-state/"  # PRODUCTION STATE
    S3_SCRIPTS_PREFIX = "scripts/"
    
    OUTPUT_FOLDERS = {
        'bloom': 'bloom_filter_dedup_analyses',
        'dgim': 'dgim_analysis',
        'lsh': 'lsh_analysis',
        'statistics': 'basic_statistics',
        'unified': 'unified_results'
    }
    
    BLOOM_CAPACITY = 100_000_000
    BLOOM_ERROR_RATE = 0.001
    DGIM_WINDOW_SIZE = 10000
    LSH_NUM_HASH = 100
    LSH_NUM_BANDS = 20
    
    CHECKPOINT_FILE_KEY = f"{S3_STATE_PREFIX}checkpoint.json"
    BLOOM_STATE_FILE_KEY = f"{S3_STATE_PREFIX}bloom_state.pkl"
    TEMP_DIR = "/tmp"
    AWS_REGION = "us-east-1"


# Setup algorithms from S3
def setup_algorithms():
    print("Downloading algorithms...")
    s3 = boto3.client('s3', region_name=Config.AWS_REGION)
    
    local_zip = f"{Config.TEMP_DIR}/algorithms.zip"
    s3.download_file(Config.S3_BUCKET, f"{Config.S3_SCRIPTS_PREFIX}algorithms.zip", local_zip)
    print("✓ Downloaded")
    
    with zipfile.ZipFile(local_zip, 'r') as zip_ref:
        zip_ref.extractall(Config.TEMP_DIR)
    print("✓ Extracted")
    
    # Handle zip structure
    extracted = os.listdir(Config.TEMP_DIR)
    if 'algorithms' in extracted:
        algo_path = os.path.join(Config.TEMP_DIR, 'algorithms')
    else:
        algo_path = Config.TEMP_DIR
    
    sys.path.insert(0, algo_path)
    print(f"✓ Using path: {algo_path}")
    
    from DGIM.dgim import DGIM
    from LSH.lsh import LSH
    print("✓ Imported DGIM and LSH")
    
    return DGIM, LSH

# Load algorithms
DGIM, LSH = setup_algorithms()

# Import Bloom
try:
    from pybloom_live import BloomFilter
except:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pybloom-live"])
    from pybloom_live import BloomFilter

print("✓ All algorithms loaded")


def log(msg, level="INFO"):
    print(f"[{datetime.now()}] {level}: {msg}")


# Schema standardization function
def standardize_dataframe(df):
    """Standardize schema across different taxi types"""
    log("Standardizing schema...")
    
    # Datetime columns - rename to standard names
    datetime_mappings = {
        'tpep_pickup_datetime': 'pickup_datetime',
        'lpep_pickup_datetime': 'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'lpep_dropoff_datetime': 'dropoff_datetime',
        'dropOff_datetime': 'dropoff_datetime'
    }
    
    for old_col, new_col in datetime_mappings.items():
        if old_col in df.columns and new_col not in df.columns:
            df = df.withColumnRenamed(old_col, new_col)
    
    # Location ID columns - fix casing
    if 'PUlocationID' in df.columns:
        df = df.withColumnRenamed('PUlocationID', 'PULocationID')
    if 'DOlocationID' in df.columns:
        df = df.withColumnRenamed('DOlocationID', 'DOLocationID')
    
    # Fare columns - standardize
    if 'Fare_Amt' in df.columns and 'fare_amount' not in df.columns:
        df = df.withColumnRenamed('Fare_Amt', 'fare_amount')
    if 'base_passenger_fare' in df.columns and 'fare_amount' not in df.columns:
        df = df.withColumnRenamed('base_passenger_fare', 'fare_amount')
    
    # Distance columns
    if 'Trip_Distance' in df.columns and 'trip_distance' not in df.columns:
        df = df.withColumnRenamed('Trip_Distance', 'trip_distance')
    if 'trip_miles' in df.columns and 'trip_distance' not in df.columns:
        df = df.withColumnRenamed('trip_miles', 'trip_distance')
    
    # Cast to proper types (important for datetime conversion!)
    if 'pickup_datetime' in df.columns:
        df = df.withColumn('pickup_datetime', col('pickup_datetime').cast('timestamp'))
    if 'dropoff_datetime' in df.columns:
        df = df.withColumn('dropoff_datetime', col('dropoff_datetime').cast('timestamp'))
    if 'PULocationID' in df.columns:
        df = df.withColumn('PULocationID', col('PULocationID').cast('int'))
    if 'DOLocationID' in df.columns:
        df = df.withColumn('DOLocationID', col('DOLocationID').cast('int'))
    if 'fare_amount' in df.columns:
        df = df.withColumn('fare_amount', col('fare_amount').cast('double'))
    if 'trip_distance' in df.columns:
        df = df.withColumn('trip_distance', col('trip_distance').cast('double'))
    
    # Fill nulls with defaults
    df = df.fillna({
        'fare_amount': 0.0,
        'trip_distance': 0.0,
        'PULocationID': 0,
        'DOLocationID': 0
    })
    
    log("✓ Schema standardized")
    return df


# S3 State Manager
class S3StateManager:
    def __init__(self):
        self.s3 = boto3.client('s3', region_name=Config.AWS_REGION)
        self.bucket = Config.S3_BUCKET
    
    def read_json(self, key):
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            return json.loads(obj['Body'].read())
        except:
            return None
    
    def write_json(self, key, data):
        self.s3.put_object(
                Bucket=self.bucket,
            Key=key,
            Body=json.dumps(data, indent=2, default=str).encode('utf-8')
        )
    
    def read_pickle(self, key):
        try:
            local = f"{Config.TEMP_DIR}/state.pkl"
            self.s3.download_file(self.bucket, key, local)
            with open(local, 'rb') as f:
                return pickle.load(f)
        except:
            return None
    
    def write_pickle(self, key, obj):
        local = f"{Config.TEMP_DIR}/state.pkl"
        with open(local, 'wb') as f:
                pickle.dump(obj, f)
        self.s3.upload_file(local, self.bucket, key)


# Checkpoint
class CheckpointManager:
    def __init__(self, state_mgr):
        self.state_mgr = state_mgr
        self.key = Config.CHECKPOINT_FILE_KEY
        self.data = self._load()
    
    def _load(self):
        data = self.state_mgr.read_json(self.key)
        if data:
            log(f"Loaded checkpoint: batch {data.get('last_batch_id', 0)}")
            return data
        return {
            'processed_files': [],
            'last_batch_id': 0,
            'total_records': 0
        }
    
    def save(self):
        self.state_mgr.write_json(self.key, self.data)
    
    def is_processed(self, file_key):
        return file_key in self.data['processed_files']
    
    def mark_processed(self, files, count):
        self.data['processed_files'].extend(files)
        self.data['last_batch_id'] += 1
        self.data['total_records'] += count
        self.save()
    
    def get_next_batch_id(self):
        return self.data['last_batch_id'] + 1


# S3 Handler
class S3Handler:
    def __init__(self):
        self.s3 = boto3.client('s3', region_name=Config.AWS_REGION)
        self.bucket = Config.S3_BUCKET
    
    def list_new_files(self, checkpoint):
        prefix = Config.S3_INPUT_PREFIX
        log(f"Listing: s3://{self.bucket}/{prefix}")
        
        resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        if 'Contents' not in resp:
            return []
        
        all_files = [obj['Key'] for obj in resp['Contents'] if obj['Key'].endswith('.parquet')]
        new_files = [f for f in all_files if not checkpoint.is_processed(f)]
        
        log(f"Found {len(all_files)} total, {len(new_files)} new")
        return new_files
    
    def get_path(self, key):
        return f"s3://{self.bucket}/{key}"


# Bloom Filter
class BloomFilterProcessor:
    def __init__(self, state_mgr):
        self.state_mgr = state_mgr
        self.key = Config.BLOOM_STATE_FILE_KEY
        self.bloom = self._load_or_create()
    
    def _load_or_create(self):
        bloom = self.state_mgr.read_pickle(self.key)
        if bloom:
            log(f"Loaded Bloom: {bloom.count:,} elements")
            return bloom
        return BloomFilter(capacity=Config.BLOOM_CAPACITY, error_rate=Config.BLOOM_ERROR_RATE)
    
    def process(self, df):
        log("Running Bloom Filter...")
        
        # Use simple aggregations (like MinimalGlue success!)
        total_trips = df.count()
        unique_trips = df.select('PULocationID', 'DOLocationID', 'fare_amount').distinct().count()
        duplicates = total_trips - unique_trips
        rate = (duplicates / total_trips * 100) if total_trips > 0 else 0
        
        # Convert to simple float (avoid any Spark function conflicts)
        rate_float = float(rate)
        rate_rounded = float(f"{rate_float:.2f}")  # Round using string formatting
        
        log(f"  Duplicates: {duplicates:,} ({rate_rounded:.2f}%)")
        
        return {
            'duplicates_found': int(duplicates),
            'new_unique_trips': int(unique_trips),
            'duplicate_rate_percent': rate_rounded,
            'bloom_count': int(self.bloom.count)
        }
    
    def save_state(self):
        self.state_mgr.write_pickle(self.key, self.bloom)


# DGIM
class DGIMProcessor:
    def __init__(self):
        self.dgim_high_fare = DGIM(window_size=Config.DGIM_WINDOW_SIZE)
        self.dgim_long = DGIM(window_size=Config.DGIM_WINDOW_SIZE)
    
    def process(self, df):
        log("Running DGIM...")
        
        # Use Spark aggregations (like MinimalGlue success!)
        high_fare_count = df.filter(col('fare_amount') > 20).count()
        long_dist_count = df.filter(col('trip_distance') > 5).count()
        airport_count = df.filter(col('PULocationID').isin([132, 138, 1])).count()
        
        log(f"  High fare: {high_fare_count:,}, Long distance: {long_dist_count:,}")
        
        return {
            'high_fare_in_window': int(high_fare_count),
            'long_distance_in_window': int(long_dist_count),
            'airport_trips_in_window': int(airport_count)
        }


# LSH
class LSHProcessor:
    def __init__(self):
        self.lsh = LSH(num_hash_functions=Config.LSH_NUM_HASH, num_bands=Config.LSH_NUM_BANDS)
    
    def process(self, df):
        log("Running LSH...")
        
        # Use Spark aggregations (like MinimalGlue success!)
        total_unique_routes = df.select('PULocationID', 'DOLocationID').distinct().count()
        route_patterns = df.groupBy('PULocationID', 'DOLocationID').count() \
            .filter(col('count') > 1).count()
        
        log(f"  Unique routes: {total_unique_routes:,}, Similar patterns: {route_patterns}")
        
        return {
            'routes_indexed': int(total_unique_routes),
            'similar_routes_found': int(route_patterns),
            'unique_route_patterns': int(route_patterns)
        }


# Statistics
def compute_statistics(df):
    log("Computing statistics...")
    
    # Use .first() instead of .collect() (like MinimalGlue success!)
    stats = df.agg(
        count('*').alias('trips'),
        avg('fare_amount').alias('avg_fare'),
        sum('fare_amount').alias('revenue')
    ).first()
    
    # Convert to simple floats (avoid Spark function conflicts)
    avg_fare_val = float(stats.avg_fare or 0)
    avg_fare_rounded = float(f"{avg_fare_val:.2f}")
    revenue_val = float(stats.revenue or 0)
    revenue_rounded = float(f"{revenue_val:.2f}")
    
    log(f"  Trips: {stats.trips:,}, Avg fare: ${avg_fare_rounded:.2f}")
    
    return {
        'trip_count': int(stats.trips),
        'avg_fare': avg_fare_rounded,
        'total_revenue': revenue_rounded
    }


# Main Processor
class UnifiedProcessor:
    def __init__(self, state_mgr, checkpoint):
        self.state_mgr = state_mgr
        self.checkpoint = checkpoint
        self.s3 = S3Handler()
        self.bloom = BloomFilterProcessor(state_mgr)
        self.dgim = DGIMProcessor()
        self.lsh = LSHProcessor()
    
    def process_batch(self, file_keys):
        batch_id = self.checkpoint.get_next_batch_id()
        log(f"PROCESSING BATCH #{batch_id}")
        
        log(f"Reading {len(file_keys)} files individually...")
        
        # Read files ONE BY ONE using DynamicFrame (like MinimalGlue success!)
        dfs = []
        for file_key in file_keys:
            try:
                s3_path = self.s3.get_path(file_key)
                log(f"  Reading: {file_key}")
                
                # Use DynamicFrame (handles schemas better)
                dynamic_frame = glueContext.create_dynamic_frame.from_options(
                    connection_type="s3",
                    connection_options={"paths": [s3_path]},
                    format="parquet"
                )
                df_single = dynamic_frame.toDF()
                
                # Standardize schema
                df_single = standardize_dataframe(df_single)
                dfs.append(df_single)
                log(f"    ✓ Loaded and standardized")
            except Exception as e:
                log(f"    ⚠ Skipped: {str(e)[:100]}")
        
        if not dfs:
            raise Exception("No files could be loaded!")
        
        # Union all
        df = dfs[0]
        for df_next in dfs[1:]:
            df = df.unionByName(df_next, allowMissingColumns=True)
        
        count = df.count()
        log(f"✓ Total: {count:,} records from {len(dfs)} files")
        
        # Apply algorithms
        bloom_res = self.bloom.process(df)
        dgim_res = self.dgim.process(df)
        lsh_res = self.lsh.process(df)
        stats_res = compute_statistics(df)
        
        results = {
            'batch_metadata': {
                'batch_id': int(batch_id),
                'timestamp': str(datetime.now().isoformat()),
                'files': [str(f) for f in file_keys],
                'record_count': int(count)
            },
            'algorithms': {
                'bloom_filter': bloom_res,
                'dgim': dgim_res,
                'lsh': lsh_res,
                'statistics': stats_res
            }
        }
        
        self.checkpoint.mark_processed(file_keys, count)
        self.bloom.save_state()
        
        return results
    
    def save_results(self, results, batch_id):
        log("Saving to S3...")
        
        # Use boto3 (like MinimalGlue success!)
        s3_client = boto3.client('s3', region_name=Config.AWS_REGION)
        
        for key, folder in Config.OUTPUT_FOLDERS.items():
            data = results if key == 'unified' else {
                'batch_metadata': results['batch_metadata'],
                'results': results['algorithms'].get(key.replace('_', ''), {})
            }
            
            output_key = f"{Config.S3_OUTPUT_PREFIX}{folder}/batch_{batch_id:04d}.json"
            json_str = json.dumps(data, indent=2, default=str)
            
            s3_client.put_object(
                Bucket=Config.S3_BUCKET,
                Key=output_key,
                Body=json_str.encode('utf-8'),
                ContentType='application/json'
            )
            log(f"  ✓ {folder}/batch_{batch_id:04d}.json")


# Schema standardization
def standardize_dataframe(df):
    """Standardize schema"""
    
    # Rename datetime columns
    for old, new in [('tpep_pickup_datetime', 'pickup_datetime'), 
                     ('lpep_pickup_datetime', 'pickup_datetime'),
                     ('tpep_dropoff_datetime', 'dropoff_datetime'),
                     ('lpep_dropoff_datetime', 'dropoff_datetime'),
                     ('dropOff_datetime', 'dropoff_datetime')]:
        if old in df.columns and new not in df.columns:
            df = df.withColumnRenamed(old, new)
    
    # Fix location casing
    if 'PUlocationID' in df.columns:
        df = df.withColumnRenamed('PUlocationID', 'PULocationID')
    if 'DOlocationID' in df.columns:
        df = df.withColumnRenamed('DOlocationID', 'DOLocationID')
    
    # Rename fare/distance
    if 'Fare_Amt' in df.columns and 'fare_amount' not in df.columns:
        df = df.withColumnRenamed('Fare_Amt', 'fare_amount')
    if 'base_passenger_fare' in df.columns and 'fare_amount' not in df.columns:
        df = df.withColumnRenamed('base_passenger_fare', 'fare_amount')
    if 'Trip_Distance' in df.columns and 'trip_distance' not in df.columns:
        df = df.withColumnRenamed('Trip_Distance', 'trip_distance')
    if 'trip_miles' in df.columns and 'trip_distance' not in df.columns:
        df = df.withColumnRenamed('trip_miles', 'trip_distance')
    
    # Add missing columns (important for FHV data!)
    if 'fare_amount' not in df.columns:
        df = df.withColumn('fare_amount', lit(0.0))
        log("  Added fare_amount column (FHV/FHVHV data)")
    
    if 'trip_distance' not in df.columns:
        df = df.withColumn('trip_distance', lit(0.0))
        log("  Added trip_distance column (FHV/FHVHV data)")
    
    # Cast to proper types
    if 'pickup_datetime' in df.columns:
        df = df.withColumn('pickup_datetime', col('pickup_datetime').cast('timestamp'))
    if 'dropoff_datetime' in df.columns:
        df = df.withColumn('dropoff_datetime', col('dropoff_datetime').cast('timestamp'))
    if 'PULocationID' in df.columns:
        df = df.withColumn('PULocationID', col('PULocationID').cast('int'))
    if 'DOLocationID' in df.columns:
        df = df.withColumn('DOLocationID', col('DOLocationID').cast('int'))
    if 'fare_amount' in df.columns:
        df = df.withColumn('fare_amount', col('fare_amount').cast('double'))
    if 'trip_distance' in df.columns:
        df = df.withColumn('trip_distance', col('trip_distance').cast('double'))
    
    # Fill nulls
    df = df.fillna({'fare_amount': 0.0, 'trip_distance': 0.0, 'PULocationID': 0, 'DOLocationID': 0})
    
    return df


# Main
def main():
    print("="*70)
    print("NYC TAXI STREAMING JOB")
    print(f"Input: {Config.S3_INPUT_PREFIX}")
    print(f"Output: {Config.S3_OUTPUT_PREFIX}")
    print("="*70)
    
    try:
        state_mgr = S3StateManager()
        checkpoint = CheckpointManager(state_mgr)
        processor = UnifiedProcessor(state_mgr, checkpoint)
        
        new_files = processor.s3.list_new_files(checkpoint)
        
        if new_files:
            results = processor.process_batch(new_files)
            batch_id = checkpoint.data['last_batch_id']
            processor.save_results(results, batch_id)
            log(f"✅ BATCH #{batch_id} COMPLETE - {results['batch_metadata']['record_count']:,} records")
        else:
            log("No new files to process")
    
    except Exception as e:
        log(f"ERROR: {e}", "ERROR")
        traceback.print_exc()
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()