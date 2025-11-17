#!/usr/bin/env python3
"""
Spark Structured Streaming Simulator for NYC Taxi Data
Simulates real-time streaming from S3 data using Reservoir Sampling and Bloom Filter
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import json
from datetime import datetime
import sys
import os

# Import our streaming algorithms
sys.path.append(os.path.dirname(__file__))
from reservoir_sampling import ReservoirSampler
from bloom_filter import BloomFilter

def create_spark_session():
    """Create Spark session for local streaming with S3 support"""
    # Set AWS profile to use
    os.environ['AWS_PROFILE'] = 'data228'

    # Unset HADOOP_CONF_DIR to prevent loading external Hadoop configs
    os.environ.pop('HADOOP_CONF_DIR', None)
    os.environ.pop('HADOOP_HOME', None)

    spark = SparkSession.builder \
        .appName("StreamTide-Local-Simulator") \
        .master("local[*]") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.aws.profile", "data228") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
        .getOrCreate()

    # Set S3A configuration - explicitly set all timeout values as numbers
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    hadoop_conf.set("fs.s3a.aws.profile", "data228")

    # Set all timeout and connection settings explicitly (no "s" suffix)
    hadoop_conf.setLong("fs.s3a.connection.timeout", 60000)
    hadoop_conf.setLong("fs.s3a.connection.establish.timeout", 60000)
    hadoop_conf.setInt("fs.s3a.attempts.maximum", 3)
    hadoop_conf.setInt("fs.s3a.connection.maximum", 100)
    hadoop_conf.setLong("fs.s3a.readahead.range", 65536)
    hadoop_conf.setInt("fs.s3a.threads.max", 10)

    # Override any duration/timeout settings that might have "s" suffix
    hadoop_conf.setLong("fs.s3a.retry.interval", 500)
    hadoop_conf.setLong("fs.s3a.retry.limit", 3)

    return spark

class StreamProcessor:
    """Process streaming data with Reservoir Sampling and Bloom Filter"""

    def __init__(self, reservoir_size=10000, expected_records=1000000):
        self.reservoir = ReservoirSampler(reservoir_size=reservoir_size)
        self.bloom_filter = BloomFilter(
            expected_elements=expected_records,
            false_positive_rate=0.01
        )
        self.stats = {
            'start_time': datetime.now(),
            'total_records': 0,
            'total_batches': 0,
            'duplicates': 0
        }

    def process_batch(self, batch_df, batch_id):
        """
        Process each micro-batch from the stream

        Args:
            batch_df: Spark DataFrame for current batch
            batch_id: Batch identifier
        """
        print(f"\n{'='*60}")
        print(f"📦 Processing Batch #{batch_id}")
        print(f"{'='*60}")

        batch_start = time.time()

        # Collect batch data
        records = batch_df.collect()
        batch_count = len(records)

        print(f"Records in batch: {batch_count:,}")

        # Process each record
        duplicates_in_batch = 0
        for row in records:
            # Convert Row to dictionary
            record = row.asDict()

            # Generate unique identifier using correct column names
            # Use tpep_pickup_datetime, tpep_dropoff_datetime, VendorID, PULocationID
            trip_id = f"{record.get('VendorID')}_{record.get('tpep_pickup_datetime')}_{record.get('tpep_dropoff_datetime')}_{record.get('PULocationID')}_{record.get('DOLocationID')}"

            # Check for duplicates using Bloom Filter
            is_duplicate = self.bloom_filter.add(trip_id)
            if is_duplicate:
                duplicates_in_batch += 1
                self.stats['duplicates'] += 1

            # Add to Reservoir Sample
            self.reservoir.add(record)

        # Update statistics
        self.stats['total_records'] += batch_count
        self.stats['total_batches'] += 1

        batch_time = time.time() - batch_start

        # Print batch summary
        print(f"\n📊 Batch Summary:")
        print(f"   ├─ Records processed: {batch_count:,}")
        print(f"   ├─ Duplicates detected: {duplicates_in_batch}")
        print(f"   ├─ Processing time: {batch_time:.2f}s")
        print(f"   └─ Throughput: {batch_count/batch_time:.0f} records/sec")

        # Print cumulative statistics every 10 batches
        if batch_id % 10 == 0 and batch_id > 0:
            self._print_statistics()

    def _print_statistics(self):
        """Print cumulative statistics"""
        print(f"\n{'='*60}")
        print(f"📈 CUMULATIVE STATISTICS")
        print(f"{'='*60}")

        # Overall stats
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        print(f"\n⏱️  Runtime: {elapsed:.1f}s")
        print(f"📦 Total Batches: {self.stats['total_batches']}")
        print(f"📝 Total Records: {self.stats['total_records']:,}")
        print(f"🔄 Duplicates: {self.stats['duplicates']}")
        print(f"⚡ Avg Throughput: {self.stats['total_records']/elapsed:.0f} records/sec")

        # Reservoir Sampling stats
        print(f"\n🎲 RESERVOIR SAMPLING:")
        reservoir_stats = self.reservoir.get_statistics()
        if 'reservoir_stats' in reservoir_stats:
            rs = reservoir_stats['reservoir_stats']
            print(f"   ├─ Sample size: {rs['sample_count']:,}")
            print(f"   ├─ Avg fare: ${rs['avg_fare']:.2f}")
            print(f"   └─ Avg distance: {rs['avg_distance']:.2f} miles")

        if 'stream_stats' in reservoir_stats:
            ss = reservoir_stats['stream_stats']
            print(f"\n   Stream-wide stats:")
            print(f"   ├─ Avg fare: ${ss['avg_fare']:.2f}")
            print(f"   └─ Avg distance: {ss['avg_distance']:.2f} miles")

        if 'top_hours' in reservoir_stats:
            print(f"\n   Top 5 Busiest Hours:")
            for i, (hour, count) in enumerate(list(reservoir_stats['top_hours'].items())[:5], 1):
                print(f"   {i}. Hour {hour:02d}: {count:,} trips")

        # Bloom Filter stats
        print(f"\n🌸 BLOOM FILTER:")
        bloom_stats = self.bloom_filter.get_statistics()
        print(f"   ├─ Elements added: {bloom_stats['usage']['elements_added']:,}")
        print(f"   ├─ Duplicates found: {bloom_stats['usage']['duplicates_detected']:,}")
        print(f"   ├─ Duplicate rate: {bloom_stats['usage']['duplicate_rate']*100:.2f}%")
        print(f"   ├─ False positive rate: {bloom_stats['performance']['actual_fp_rate']*100:.4f}%")
        print(f"   ├─ Fill ratio: {bloom_stats['performance']['fill_ratio']*100:.2f}%")
        print(f"   └─ Memory: {bloom_stats['performance']['memory_mb']:.2f} MB")

    def save_results(self, output_dir="./streaming_results"):
        """Save final results to files"""
        os.makedirs(output_dir, exist_ok=True)

        # Save reservoir sample
        reservoir_file = f"{output_dir}/reservoir_sample.json"
        self.reservoir.save_reservoir(reservoir_file)

        # Save Bloom Filter stats
        bloom_file = f"{output_dir}/bloom_filter_stats.json"
        self.bloom_filter.save_stats(bloom_file)

        # Save overall statistics
        stats_file = f"{output_dir}/streaming_stats.json"
        final_stats = {
            'run_metadata': {
                'start_time': str(self.stats['start_time']),
                'end_time': str(datetime.now()),
                'duration_seconds': (datetime.now() - self.stats['start_time']).total_seconds()
            },
            'processing': self.stats,
            'reservoir_sampling': self.reservoir.get_statistics(),
            'bloom_filter': self.bloom_filter.get_statistics()
        }

        with open(stats_file, 'w') as f:
            json.dump(final_stats, f, indent=2, default=str)

        print(f"\n💾 Results saved to {output_dir}/")
        return output_dir


def main():
    """Main streaming simulation"""

    # Configuration
    S3_INPUT = "s3a://data228-bigdata-nyc/staging/"  # Use s3a:// for S3A file system
    RESERVOIR_SIZE = 10000
    MAX_FILES_PER_TRIGGER = 5  # Process 5 files per micro-batch
    OUTPUT_DIR = "./streaming_results"

    print(f"""
    ========================================
    StreamTide Local Streaming Simulator
    ========================================
    S3 Input: {S3_INPUT}
    Reservoir Size: {RESERVOIR_SIZE:,}
    Files per batch: {MAX_FILES_PER_TRIGGER}
    Output: {OUTPUT_DIR}
    ========================================
    """)

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Create stream processor
    processor = StreamProcessor(reservoir_size=RESERVOIR_SIZE)

    try:
        # TEST MODE: Read from local file (bypasses S3A config issues)
        print(f"🧪 TEST MODE: Reading 2025 file from LOCAL storage")

        # Use local downloaded file
        test_file = "/Users/shreya/SJSU-Github/DA228/streaming/sample_data/yellow_tripdata_2025-01.parquet"
        print(f"  Reading: {test_file}")

        df = spark.read.parquet(test_file)

        total_records = df.count()
        print(f"\n✅ Total records to stream: {total_records:,}")
        print(f"📊 Schema:")
        df.printSchema()

        # Process in batches to simulate streaming
        batch_size = 10000
        num_batches = (total_records // batch_size) + 1

        # TEST MODE: Only process first 10 batches
        test_batches = 10 if num_batches > 10 else num_batches

        print(f"\n🚀 Starting streaming simulation")
        print(f"   Processing {test_batches} batches of {batch_size:,} records each (TEST MODE)\n")

        for batch_id in range(test_batches):
            offset = batch_id * batch_size
            batch_df = df.limit(batch_size).offset(offset)

            # Process batch
            processor.process_batch(batch_df, batch_id)

            # Small delay to simulate real-time streaming
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\n\n Interrupted by user")

    finally:
        # Print final statistics
        processor._print_statistics()

        # Save results
        output_dir = processor.save_results(OUTPUT_DIR)

        print(f"\n✅ Streaming simulation complete!")
        print(f"📁 Results saved to: {output_dir}/")

        spark.stop()


if __name__ == "__main__":
    main()
