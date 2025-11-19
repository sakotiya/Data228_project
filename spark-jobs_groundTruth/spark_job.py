
#!/usr/bin/env python3
"""
Spark Structured Streaming Simulator for NYC Taxi Data
Simulates real-time streaming from S3 data using Reservoir Sampling and Bloom Filter
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, monotonically_increasing_id
from pyspark.sql.types import *
import time
import json
from datetime import datetime
import sys
import os
import boto3
import builtins 

# Import our streaming algorithms
sys.path.append(os.path.dirname(__file__))
from reservoir_sampling import ReservoirSampler
from bloom_filter import BloomFilter
from dgim import DGIM


def create_spark_session():
    # Ensure Spark picks up your custom Hadoop configuration
    os.environ["HADOOP_CONF_DIR"] = "/Users/ramreddy/Documents/Fall2025/Data_228/stream"
    print(f"✅ Using HADOOP_CONF_DIR={os.environ['HADOOP_CONF_DIR']}")

    # ---- Spark session setup ----
    spark = (
        SparkSession.builder
        .appName("StreamTide-Local-Simulator")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.profile.ProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.aws.profile", "Admin_228")
        .config("spark.hadoop.fs.s3a.connection.timeout.seconds", "60")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout.seconds", "60")
        .config("spark.hadoop.fs.s3a.impl.disable.seconds.config", "true")
        .getOrCreate()
    )

    # ---- Hadoop configuration ----
    hconf = spark._jsc.hadoopConfiguration()

    # Forcefully override at runtime to avoid 60s parsing
    for key in [
        "fs.s3a.connection.timeout.seconds",
        "fs.s3a.connection.establish.timeout.seconds",
    ]:
        try:
            old_val = hconf.get(key)
            if old_val:
                print(f"🧹 Overriding {key} (was {old_val}) → 60")
            hconf.set(key, "60")
        except Exception as e:
            print(f"⚠️ Could not override {key}: {e}")

    # Disable any Hadoop 'seconds' parsing logic globally
    hconf.setBoolean("fs.s3a.impl.disable.seconds.config", True)

    # Verify everything is set
    print(" Runtime S3A conf snapshot:")
    for key in [
        "fs.s3a.connection.timeout",
        "fs.s3a.connection.establish.timeout",
        "fs.s3a.connection.timeout.seconds",
        "fs.s3a.connection.establish.timeout.seconds",
        "fs.s3a.impl.disable.seconds.config",
    ]:
        print(f"   {key} = {hconf.get(key)}")

    spark.sparkContext.setLogLevel("ERROR")
    print(" Spark session created successfully with S3A support.")
    return spark

class StreamProcessor:
    """Process streaming data with Reservoir Sampling and Bloom Filter"""

    def __init__(self, reservoir_size=10000, expected_records=1000000):
        self.reservoir = ReservoirSampler(reservoir_size=reservoir_size)
        self.bloom_filter = BloomFilter(
            expected_elements=expected_records,
            false_positive_rate=0.01
        )
        self.dgim = DGIM(window_size=1000)  # or whatever window size you want
        self.stats = {
            'start_time': datetime.now(),
            'total_records': 0,
            'total_batches': 0,
            'duplicates': 0
        }

    # -----------------------------------------------------
    # Batch-level processing
    # -----------------------------------------------------
    def process_batch(self, batch_df, batch_id):
        """Process each micro-batch from the stream."""
        print(f"\n{'='*70}")
        print(f"📦 Processing Batch #{batch_id}")
        print(f"{'='*70}")

        batch_start = time.time()
        records = batch_df.collect()
        batch_count = len(records)
        duplicates_in_batch = 0

        # Fast path for empty DataFrames
        if batch_count == 0:
            print("⚠️ Empty batch — skipping.")
            return

        for row in records:
            record = row.asDict()
            # Generate unique trip identifier (robust for all taxi types)
            trip_id = "_".join(
                str(record.get(col, "NA"))
                for col in ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"]
            )

            # Deduplication check
            if self.bloom_filter.add(trip_id):
                duplicates_in_batch += 1
                self.stats["duplicates"] += 1

            # Add record to reservoir sample
            self.reservoir.add(record)
            bit = 1 if record.get("fare_amount", 0) > 0 else 0
            self.dgim.add(bit)


        # Update cumulative stats
        self.stats["total_records"] += batch_count
        self.stats["total_batches"] += 1
        self.stats["dgim_estimate"] = self.dgim.estimate()

        batch_time = time.time() - batch_start
        print(f"✅ Batch complete: {batch_count:,} records | ⏱️ {batch_time:.2f}s | "
              f"⚡ {batch_count/batch_time:.0f} rec/s | 🌀 Duplicates: {duplicates_in_batch}")

        # Print cumulative every 5–10 batches for progress tracking
        if batch_id % 10 == 0 and batch_id > 0:
            self._print_statistics()

    # -----------------------------------------------------
    # Summary metrics
    # -----------------------------------------------------
    def _print_statistics(self):
        """Print cumulative statistics so far."""
        elapsed = (datetime.now() - self.stats["start_time"]).total_seconds()
        print(f"\n{'='*70}\n📈 CUMULATIVE STATISTICS\n{'='*70}")
        print(f"🕒 Runtime: {elapsed:.1f}s")
        print(f"📦 Total Batches: {self.stats['total_batches']}")
        print(f"📝 Total Records: {self.stats['total_records']:,}")
        print(f"🔄 Duplicates: {self.stats['duplicates']}")
        print(f"⚡ Throughput: {self.stats['total_records']/max(elapsed,1):.0f} rec/s")
        print(f"📊 DGIM Estimate: {self.stats.get('dgim_estimate', 0)}")

        # Reservoir stats
        rs = self.reservoir.get_statistics()
        if "reservoir_stats" in rs:
            r = rs["reservoir_stats"]
            print(f"\n🎲 Reservoir:")
            print(f"   ├─ Sample size: {r['sample_count']:,}")
            print(f"   ├─ Avg fare: ${r['avg_fare']:.2f}")
            print(f"   └─ Avg distance: {r['avg_distance']:.2f} miles")

        if "top_hours" in rs:
            print(f"\n⏰ Top 5 Busy Hours:")
            for i, (hour, count) in enumerate(list(rs["top_hours"].items())[:5], 1):
                print(f"   {i}. Hour {hour:02d}: {count:,} trips")

        # Bloom filter stats
        bf = self.bloom_filter.get_statistics()
        print(f"\n🌸 Bloom Filter:")
        print(f"   ├─ Elements added: {bf['usage']['elements_added']:,}")
        print(f"   ├─ Duplicates detected: {bf['usage']['duplicates_detected']:,}")
        print(f"   ├─ False positive rate: {bf['performance']['actual_fp_rate']*100:.4f}%")
        print(f"   └─ Memory usage: {bf['performance']['memory_mb']:.2f} MB")

        dgim_estimate = self.dgim.query(100)  # count of '1's in last 100 entries
        print(f"\n🧮 DGIM Approximation:")
        print(f"   ├─ Window size: {self.dgim.window_size}")
        print(f"   ├─ Recent 100-bit 1-count: {dgim_estimate}")
        print(f"   └─ Total bits processed: {self.dgim.count}")
    # -----------------------------------------------------
    # Save results to disk
    # -----------------------------------------------------
    def save_results(self, output_dir="./streaming_results"):
        """Persist statistics and sample results to disk."""
        os.makedirs(output_dir, exist_ok=True)

        # Save Reservoir + Bloom data
        self.reservoir.save_reservoir(f"{output_dir}/reservoir_sample.json")
        self.bloom_filter.save_stats(f"{output_dir}/bloom_filter_stats.json")
        self.dgim.save_stats(f"{output_dir}/dgim_stats.json")

        # Save summary stats
        final_stats = {
            "run_metadata": {
                "start_time": str(self.stats["start_time"]),
                "end_time": str(datetime.now()),
                "duration_seconds": (datetime.now() - self.stats["start_time"]).total_seconds(),
            },
            "processing": self.stats,
            "reservoir_sampling": self.reservoir.get_statistics(),
            "bloom_filter": self.bloom_filter.get_statistics(),
            "dgim": self.dgim.get_statistics(),
        }

        with open(f"{output_dir}/streaming_summary.json", "w") as f:
            json.dump(final_stats, f, indent=2, default=str)

        print(f"\n💾 Results saved successfully → {output_dir}/")
        return output_dir
    
    
def main():
    """Main streaming simulation — processes all 2025 Parquet files from S3 and uploads results to S3"""

    # Configuration
    S3_BUCKET = "data228-bigdata-nyc"
    S3_PREFIX = "staging/2025/"
    OUTPUT_S3_PREFIX = "streaming_results/Admin_228/"
    RESERVOIR_SIZE = 10000
    BATCH_SIZE = 10000
    REGION = "us-east-1"

    print(f"""
    =============================================
    StreamTide 2025 Full-Year Streaming Simulator
    =============================================
    S3 Input  : s3://{S3_BUCKET}/{S3_PREFIX}
    S3 Output : s3://{S3_BUCKET}/{OUTPUT_S3_PREFIX}
    Reservoir : {RESERVOIR_SIZE:,}
    Batch Size: {BATCH_SIZE:,}
    =============================================
    """)

    # ---- Create Spark session ----
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ---- Initialize stream processor ----
    processor = StreamProcessor(reservoir_size=RESERVOIR_SIZE)

    try:
        # -----------------------------------------------------
        # 1️⃣ List *all* 2025 Parquet files in the bucket
        # -----------------------------------------------------
        s3 = boto3.client("s3", region_name=REGION)
        continuation_token = None
        files_2025 = []

        print("🔍 Scanning S3 for all 2025 Parquet files...")

        while True:
            if continuation_token:
                response = s3.list_objects_v2(
                    Bucket=S3_BUCKET, Prefix=S3_PREFIX, ContinuationToken=continuation_token
                )
            else:
                response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)

            for obj in response.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet") and "2025" in key:
                    files_2025.append(f"s3a://{S3_BUCKET}/{key}")

            if response.get("IsTruncated"):
                continuation_token = response["NextContinuationToken"]
            else:
                break

        if not files_2025:
            print("⚠️ No 2025 Parquet files found in your S3 bucket.")
            return

        print(f"✅ Found {len(files_2025)} Parquet files containing '2025':")
        for f in files_2025:
            print(f"   - {f}")

        # -----------------------------------------------------
        # 2️⃣ Process each file sequentially
        # -----------------------------------------------------
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number, col, monotonically_increasing_id

        batch_id = 0
        for file_path in sorted(files_2025):
            print(f"\n🚀 Reading {file_path}")
            df = spark.read.parquet(file_path)
            total_records = df.count()
            num_batches = (total_records // BATCH_SIZE) + 1

            print(f"📦 Total Records: {total_records:,} → 🔄 {num_batches} batches")

            # Add sequential index to simulate offset
            df_indexed = df.withColumn("row_num", (monotonically_increasing_id() % total_records).cast("long"))

            for b in range(num_batches):
                start = b * BATCH_SIZE
                end = start + BATCH_SIZE
                batch_df = df_indexed.filter((col("row_num") >= start) & (col("row_num") < end)).drop("row_num")

                processor.process_batch(batch_df, batch_id)
                batch_id += 1
                time.sleep(0.1)  # simulate streaming delay

        # -----------------------------------------------------
        # 3️⃣ Save and upload results to S3
        # -----------------------------------------------------
        print("\n💾 Saving results locally...")
        local_dir = processor.save_results("./streaming_results")

        print("📤 Uploading results to S3...")
        for file_name in os.listdir(local_dir):
            local_path = os.path.join(local_dir, file_name)
            s3_key = OUTPUT_S3_PREFIX + file_name
            s3.upload_file(local_path, S3_BUCKET, s3_key)
            print(f"   → Uploaded {file_name} to s3://{S3_BUCKET}/{s3_key}")

        print("\n✅ All result files uploaded to S3 successfully!")
        print(f"📁 Final S3 Output: s3://{S3_BUCKET}/{OUTPUT_S3_PREFIX}")

    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user. Saving progress...")
        processor.save_results("./streaming_results")

    finally:
        spark.stop()
        print("🧹 Spark session stopped cleanly.")


if __name__ == "__main__":
    main()