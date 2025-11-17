#!/usr/bin/env python3
"""
Advanced Streaming Simulator with All 6 Algorithms
Processes NYC Taxi data using multiple streaming algorithms simultaneously
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import json
from datetime import datetime
import sys
import os

# Import all streaming algorithms
sys.path.append(os.path.dirname(__file__))
from reservoir_sampling import ReservoirSampler
from bloom_filter import BloomFilter
from hyperloglog import HyperLogLog
from dgim import DGIM
from computing_moments import MomentEstimator
from lsh import LSH


def create_spark_session():
    """Create Spark session for local streaming"""
    # Unset HADOOP env vars to prevent Java compatibility issues
    os.environ.pop('HADOOP_CONF_DIR', None)
    os.environ.pop('HADOOP_HOME', None)

    spark = SparkSession.builder \
        .appName("StreamTide-Advanced-Simulator") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    return spark


class AdvancedStreamProcessor:
    """Process streaming data with all 6 algorithms"""

    def __init__(self, reservoir_size=10000):
        # Algorithm 1: Reservoir Sampling
        self.reservoir = ReservoirSampler(reservoir_size=reservoir_size)

        # Algorithm 2: Bloom Filter (duplicate detection)
        self.bloom_filter = BloomFilter(
            expected_elements=1000000,
            false_positive_rate=0.01
        )

        # Algorithm 3: HyperLogLog (distinct count estimation)
        self.hyperloglog_zones = HyperLogLog(precision=14)  # For zones
        self.hyperloglog_vendors = HyperLogLog(precision=12)  # For vendors

        # Algorithm 4: DGIM (sliding window - busy periods)
        self.dgim = DGIM(window_size=1000)  # Last 1000 trips

        # Algorithm 5: Computing Moments (frequency distribution)
        self.moments_zones = MomentEstimator(num_variables=50)

        # Algorithm 6: LSH (similar routes)
        self.lsh = LSH(num_hash_functions=100, num_bands=20)

        # Route tracking for LSH
        self.route_zones = {}  # trip_id -> set of zones

        # Statistics
        self.stats = {
            'start_time': datetime.now(),
            'total_records': 0,
            'total_batches': 0,
            'duplicates': 0,
            'busy_trips': 0  # For DGIM
        }

    def process_batch(self, batch_df, batch_id):
        """Process each micro-batch"""
        print(f"\n{'='*70}")
        print(f" Processing Batch #{batch_id}")
        print(f"{'='*70}")

        batch_start = time.time()
        records = batch_df.collect()
        batch_count = len(records)

        print(f"Records in batch: {batch_count:,}")

        duplicates_in_batch = 0

        for row in records:
            record = row.asDict()

            # Generate unique trip identifier
            trip_id = f"{record.get('VendorID')}_{record.get('tpep_pickup_datetime')}_{record.get('tpep_dropoff_datetime')}_{record.get('PULocationID')}_{record.get('DOLocationID')}"

            # ===== Algorithm 1 & 2: Reservoir Sampling + Bloom Filter =====
            is_duplicate = self.bloom_filter.add(trip_id)
            if is_duplicate:
                duplicates_in_batch += 1
                self.stats['duplicates'] += 1
            self.reservoir.add(record)

            # ===== Algorithm 3: HyperLogLog =====
            # Track distinct zones
            pu_zone = str(record.get('PULocationID', 'unknown'))
            do_zone = str(record.get('DOLocationID', 'unknown'))
            self.hyperloglog_zones.add(pu_zone)
            self.hyperloglog_zones.add(do_zone)

            # Track distinct vendors
            vendor = str(record.get('VendorID', 'unknown'))
            self.hyperloglog_vendors.add(vendor)

            # ===== Algorithm 4: DGIM =====
            # Track busy periods (1 if fare > $20, 0 otherwise)
            fare = float(record.get('fare_amount', 0))
            is_busy = 1 if fare > 20 else 0
            self.dgim.add_bit(is_busy)
            if is_busy:
                self.stats['busy_trips'] += 1

            # ===== Algorithm 5: Computing Moments =====
            # Track zone frequency distribution
            self.moments_zones.add(pu_zone)

            # ===== Algorithm 6: LSH =====
            # Build route signatures (set of zones for this trip)
            # Simplified: just pickup and dropoff for now
            route_set = {pu_zone, do_zone}
            self.route_zones[trip_id] = route_set

        # Update statistics
        self.stats['total_records'] += batch_count
        self.stats['total_batches'] += 1

        batch_time = time.time() - batch_start

        # Print batch summary
        print(f"\n Batch Summary:")
        print(f"   ├─ Records processed: {batch_count:,}")
        print(f"   ├─ Duplicates detected: {duplicates_in_batch}")
        print(f"   ├─ Processing time: {batch_time:.2f}s")
        print(f"   └─ Throughput: {batch_count/batch_time:.0f} records/sec")

        # Print cumulative stats every 5 batches
        if (batch_id + 1) % 5 == 0:
            self._print_statistics()

    def _print_statistics(self):
        """Print comprehensive statistics from all algorithms"""
        print(f"\n{'='*70}")
        print(f" COMPREHENSIVE STREAMING ANALYTICS")
        print(f"{'='*70}")

        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()

        print(f"\n  Overall Performance:")
        print(f"   ├─ Runtime: {elapsed:.1f}s")
        print(f"   ├─ Total Records: {self.stats['total_records']:,}")
        print(f"   ├─ Batches: {self.stats['total_batches']}")
        print(f"   └─ Throughput: {self.stats['total_records']/elapsed:.0f} records/sec")

        # Algorithm 1: Reservoir Sampling
        reservoir_stats = self.reservoir.get_statistics()
        if 'reservoir_stats' in reservoir_stats:
            print(f"\n  RESERVOIR SAMPLING:")
            rs = reservoir_stats['reservoir_stats']
            print(f"   ├─ Sample size: {rs['sample_count']:,}")
            print(f"   ├─ Avg fare: ${rs['avg_fare']:.2f}")
            print(f"   └─ Avg distance: {rs['avg_distance']:.2f} miles")

        # Algorithm 2: Bloom Filter
        bloom_stats = self.bloom_filter.get_statistics()
        print(f"\n  BLOOM FILTER (Duplicate Detection):")
        print(f"   ├─ Duplicates found: {bloom_stats['usage']['duplicates_detected']:,}")
        print(f"   ├─ Duplicate rate: {bloom_stats['usage']['duplicate_rate']*100:.2f}%")
        print(f"   └─ Memory: {bloom_stats['performance']['memory_mb']:.2f} MB")

        # Algorithm 3: HyperLogLog
        distinct_zones = self.hyperloglog_zones.count()
        distinct_vendors = self.hyperloglog_vendors.count()
        print(f"\n  HYPERLOGLOG (Cardinality Estimation):")
        print(f"   ├─ Distinct zones: ~{distinct_zones}")
        print(f"   ├─ Distinct vendors: ~{distinct_vendors}")
        hll_stats = self.hyperloglog_zones.get_statistics()
        print(f"   └─ Theoretical error: ±{hll_stats['theoretical_std_error_percentage']:.2f}%")

        # Algorithm 4: DGIM
        busy_count = self.dgim.count_ones()
        print(f"\n DGIM (Sliding Window Analysis):")
        print(f"   ├─ Window size: {self.dgim.window_size} trips")
        print(f"   ├─ Busy trips in window: ~{busy_count}")
        print(f"   └─ Busy ratio: {busy_count/self.dgim.window_size*100:.1f}%")

        # Algorithm 5: Computing Moments
        moment_stats = self.moments_zones.get_statistics()
        print(f"\n COMPUTING MOMENTS (Distribution Analysis):")
        if 'moments' in moment_stats:
            f2_data = moment_stats['moments'].get('F2', {})
            print(f"   ├─ Distinct zones (F0): {moment_stats['distinct_elements']}")
            print(f"   ├─ Total trips (F1): {moment_stats['total_elements']:,}")
            if 'insights' in moment_stats:
                print(f"   └─ Avg frequency: {moment_stats['insights']['average_frequency']:.2f}")

        # Algorithm 6: LSH (simplified stats)
        print(f"\n LSH (Similarity Detection):")
        print(f"   ├─ Routes indexed: {len(self.route_zones):,}")
        print(f"   └─ Ready for similarity queries")

    def save_results(self, output_dir="./streaming_results_advanced"):
        """Save all results"""
        os.makedirs(output_dir, exist_ok=True)

        # Save individual algorithm results
        self.reservoir.save_reservoir(f"{output_dir}/reservoir_sample.json")
        self.bloom_filter.save_stats(f"{output_dir}/bloom_filter_stats.json")
        self.hyperloglog_zones.save_stats(f"{output_dir}/hyperloglog_zones.json")
        self.hyperloglog_vendors.save_stats(f"{output_dir}/hyperloglog_vendors.json")
        self.dgim.save_stats(f"{output_dir}/dgim_stats.json")
        self.moments_zones.save_stats(f"{output_dir}/moments_stats.json")
        self.lsh.save_stats(f"{output_dir}/lsh_stats.json")

        # Save comprehensive summary
        summary = {
            'run_metadata': {
                'start_time': str(self.stats['start_time']),
                'end_time': str(datetime.now()),
                'duration_seconds': (datetime.now() - self.stats['start_time']).total_seconds()
            },
            'processing': self.stats,
            'algorithms': {
                'reservoir_sampling': self.reservoir.get_statistics(),
                'bloom_filter': self.bloom_filter.get_statistics(),
                'hyperloglog_zones': self.hyperloglog_zones.get_statistics(),
                'hyperloglog_vendors': self.hyperloglog_vendors.get_statistics(),
                'dgim': self.dgim.get_statistics(),
                'moments': self.moments_zones.get_statistics(),
                'lsh': self.lsh.get_statistics()
            }
        }

        with open(f"{output_dir}/complete_summary.json", 'w') as f:
            json.dump(summary, f, indent=2, default=str)

        print(f"\n All results saved to {output_dir}/")
        return output_dir


def main():
    """Main streaming simulation"""

    # Configuration
    LOCAL_FILE = "/Users/shreya/SJSU-Github/DA228/streaming/sample_data/yellow_tripdata_2025-01.parquet"
    RESERVOIR_SIZE = 10000
    OUTPUT_DIR = "./streaming_results_advanced"

    print(f"""
    ========================================
    StreamTide Advanced Streaming Simulator
    ========================================
    Input: {LOCAL_FILE}
    Algorithms: 6 (Reservoir, Bloom, HLL, DGIM, Moments, LSH)
    Output: {OUTPUT_DIR}
    ========================================
    """)

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Create processor with all algorithms
    processor = AdvancedStreamProcessor(reservoir_size=RESERVOIR_SIZE)

    try:
        print(f" Reading data from local file...")
        df = spark.read.parquet(LOCAL_FILE)

        total_records = df.count()
        print(f" Total records available: {total_records:,}")

        # Process in batches
        batch_size = 10000
        test_batches = 10  # Process 100K records

        print(f"\n Starting simulation with {test_batches} batches\n")

        for batch_id in range(test_batches):
            offset = batch_id * batch_size
            batch_df = df.limit(batch_size).offset(offset)

            processor.process_batch(batch_df, batch_id)
            time.sleep(0.05)

    except KeyboardInterrupt:
        print("\n\n Interrupted by user")

    finally:
        # Final statistics
        processor._print_statistics()

        # Save results
        output_dir = processor.save_results(OUTPUT_DIR)

        print(f"\n Advanced streaming simulation complete!")
        print(f"  Results: {output_dir}/")

        spark.stop()


if __name__ == "__main__":
    main()
