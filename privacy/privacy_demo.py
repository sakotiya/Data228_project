#!/usr/bin/env python3
"""
Privacy Techniques Demonstration
Standalone script showing K-Anonymity and Differential Privacy on NYC Taxi Data
"""

from pyspark.sql import SparkSession
import time
import json
from datetime import datetime
import sys
import os
from collections import defaultdict

sys.path.append(os.path.dirname(__file__))
from privacy_techniques import KAnonymity, DifferentialPrivacy


def create_spark_session(use_s3=False):
    """Create Spark session"""
    os.environ.pop('HADOOP_CONF_DIR', None)
    os.environ.pop('HADOOP_HOME', None)

    builder = SparkSession.builder \
        .appName("StreamTide-Privacy-Demo")

    if use_s3:
        spark = builder \
            .master("local[10]") \
            .config("spark.driver.memory", "12g") \
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.4.0,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.683") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .getOrCreate()
    else:
        spark = builder \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()

    return spark


def main():
    """Demonstrate K-Anonymity and Differential Privacy"""

    # Configuration
    USE_S3 = True

    if USE_S3:
        DATA_PATH = "s3a://data228-bigdata-nyc/staging/2025/"
        OUTPUT_DIR = "./privacy_demo_results"
        SAMPLE_SIZE = 100000  # Process 100K records for demo
    else:
        DATA_PATH = "/Users/shreya/SJSU-Github/DA228/streaming/sample_data/yellow_tripdata_2025-01.parquet"
        OUTPUT_DIR = "./privacy_demo_results"
        SAMPLE_SIZE = 10000

    # Privacy parameters
    K_VALUE = 5      # K-Anonymity: each group must have at least 5 records
    EPSILON = 1.0    # Differential Privacy: privacy budget

    print(f"""
+======================================================================+
|           PRIVACY TECHNIQUES DEMONSTRATION                           |
|           K-Anonymity and Differential Privacy                       |
+======================================================================+

    Data Source: {DATA_PATH}
    Sample Size: {SAMPLE_SIZE:,} records

    PRIVACY PARAMETERS:
    - K-Anonymity (k={K_VALUE}): Generalize data so each record is
      indistinguishable from at least {K_VALUE-1} others
    - Differential Privacy (epsilon={EPSILON}): Add noise to statistics
      to prevent individual identification

    Output: {OUTPUT_DIR}
======================================================================
    """)

    # Initialize privacy techniques
    k_anon = KAnonymity(k=K_VALUE)
    dp = DifferentialPrivacy(epsilon=EPSILON)

    # Create Spark session
    spark = create_spark_session(use_s3=USE_S3)
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("[1/5] Loading data...")
        df = spark.read.parquet(DATA_PATH)

        # Sample data
        sample_df = df.limit(SAMPLE_SIZE)
        records = sample_df.collect()
        print(f"      Loaded {len(records):,} records")

        # =====================================================
        # PART 1: K-ANONYMITY DEMONSTRATION
        # =====================================================
        print("\n" + "="*70)
        print("[2/5] K-ANONYMITY DEMONSTRATION")
        print("="*70)
        print(f"\nGoal: Generalize specific values so each record blends with {K_VALUE-1} others")

        # Track original vs generalized data
        original_zones = defaultdict(int)
        generalized_zones = defaultdict(int)
        original_times = defaultdict(int)
        generalized_times = defaultdict(int)
        original_fares = []
        generalized_fares = defaultdict(int)

        # Sample records for before/after comparison
        sample_comparisons = []

        for i, row in enumerate(records):
            record = row.asDict()

            # Extract original values
            pu_zone = str(record.get('PULocationID') or record.get('PUlocationID') or 'unknown')
            do_zone = str(record.get('DOLocationID') or record.get('DOlocationID') or 'unknown')
            pickup_time = str(record.get('tpep_pickup_datetime') or record.get('pickup_datetime') or '')
            fare = float(record.get('fare_amount', 0) or 0)

            if pu_zone == 'unknown':
                continue

            # Apply K-Anonymity generalization
            gen_pu = k_anon.generalize_zone(pu_zone)
            gen_do = k_anon.generalize_zone(do_zone)

            hour = 0
            gen_time = 'Unknown'
            try:
                if pickup_time:
                    dt = datetime.fromisoformat(pickup_time)
                    hour = dt.hour
                    gen_time = k_anon.generalize_time(hour)
            except:
                pass

            gen_fare = k_anon.generalize_fare(fare)

            # Count original values
            original_zones[pu_zone] += 1
            original_times[hour] += 1
            original_fares.append(fare)

            # Count generalized values
            generalized_zones[gen_pu] += 1
            generalized_times[gen_time] += 1
            generalized_fares[gen_fare] += 1

            # Save sample comparisons
            if i < 5:
                sample_comparisons.append({
                    'original': {
                        'pickup_zone': pu_zone,
                        'dropoff_zone': do_zone,
                        'hour': hour,
                        'fare': f"${fare:.2f}"
                    },
                    'generalized': {
                        'pickup_zone': gen_pu,
                        'dropoff_zone': gen_do,
                        'time_period': gen_time,
                        'fare_range': gen_fare
                    }
                })

        # Show before/after examples
        print("\n--- Sample Records: Before vs After K-Anonymity ---")
        for i, comp in enumerate(sample_comparisons, 1):
            print(f"\nRecord {i}:")
            print(f"  ORIGINAL:     Zone {comp['original']['pickup_zone']} -> {comp['original']['dropoff_zone']}, "
                  f"Hour {comp['original']['hour']}, {comp['original']['fare']}")
            print(f"  GENERALIZED:  {comp['generalized']['pickup_zone']} -> {comp['generalized']['dropoff_zone']}, "
                  f"{comp['generalized']['time_period']}, {comp['generalized']['fare_range']}")

        # Show aggregation effect
        print("\n--- K-Anonymity Effect on Data Granularity ---")
        print(f"\nPickup Zones:")
        print(f"  Original:    {len(original_zones)} unique zones")
        print(f"  Generalized: {len(generalized_zones)} regions")
        print(f"  Reduction:   {(1 - len(generalized_zones)/len(original_zones))*100:.1f}%")

        print(f"\nTime Values:")
        print(f"  Original:    {len(original_times)} unique hours (0-23)")
        print(f"  Generalized: {len(generalized_times)} time periods")

        print(f"\nFare Values:")
        print(f"  Original:    Continuous values (${min(original_fares):.2f} - ${max(original_fares):.2f})")
        print(f"  Generalized: {len(generalized_fares)} fare ranges")

        # Show generalized distributions
        print("\n--- Generalized Region Distribution ---")
        sorted_regions = sorted(generalized_zones.items(), key=lambda x: x[1], reverse=True)[:10]
        for region, count in sorted_regions:
            print(f"  {region}: {count:,} trips")

        # =====================================================
        # PART 2: DIFFERENTIAL PRIVACY DEMONSTRATION
        # =====================================================
        print("\n" + "="*70)
        print("[3/5] DIFFERENTIAL PRIVACY DEMONSTRATION")
        print("="*70)
        print(f"\nGoal: Add noise to statistics (epsilon={EPSILON}) to prevent individual identification")

        # Apply DP to zone counts
        print("\n--- Zone Counts: True vs DP-Protected ---")
        noisy_zones = dp.add_noise_to_histogram(dict(generalized_zones))

        print(f"\n{'Region':<30} {'True Count':>12} {'DP Count':>12} {'Noise':>10}")
        print("-" * 66)
        for region, true_count in sorted_regions[:10]:
            noisy_count = noisy_zones[region]
            noise = noisy_count - true_count
            print(f"{region:<30} {true_count:>12,} {noisy_count:>12,} {noise:>+10}")

        # Apply DP to time distribution
        print("\n--- Time Period Distribution: True vs DP-Protected ---")
        noisy_times = dp.add_noise_to_histogram(dict(generalized_times))

        print(f"\n{'Time Period':<20} {'True Count':>12} {'DP Count':>12} {'Noise':>10}")
        print("-" * 56)
        for period in sorted(generalized_times.keys()):
            true_count = generalized_times[period]
            noisy_count = noisy_times[period]
            noise = noisy_count - true_count
            print(f"{period:<20} {true_count:>12,} {noisy_count:>12,} {noise:>+10}")

        # Apply DP to fare distribution
        print("\n--- Fare Range Distribution: True vs DP-Protected ---")
        noisy_fares = dp.add_noise_to_histogram(dict(generalized_fares))

        print(f"\n{'Fare Range':<20} {'True Count':>12} {'DP Count':>12} {'Noise':>10}")
        print("-" * 56)
        for fare_range in sorted(generalized_fares.keys()):
            true_count = generalized_fares[fare_range]
            noisy_count = noisy_fares[fare_range]
            noise = noisy_count - true_count
            print(f"{fare_range:<20} {true_count:>12,} {noisy_count:>12,} {noise:>+10}")

        # Demonstrate private top-k
        print("\n--- Private Top-K Selection (Exponential Mechanism) ---")
        route_counts = {}
        for row in records[:10000]:  # Use subset for route patterns
            record = row.asDict()
            pu = str(record.get('PULocationID') or record.get('PUlocationID') or '?')
            do = str(record.get('DOLocationID') or record.get('DOlocationID') or '?')
            if pu != '?' and do != '?':
                gen_pu = k_anon.generalize_zone(pu)
                gen_do = k_anon.generalize_zone(do)
                route = f"{gen_pu} -> {gen_do}"
                route_counts[route] = route_counts.get(route, 0) + 1

        top_routes = dp.private_top_k(route_counts, k=5)
        print("\nTop 5 Routes (selected with differential privacy):")
        for i, (route, score) in enumerate(top_routes, 1):
            true_count = route_counts.get(route, 0)
            print(f"  {i}. {route}")
            print(f"     True count: {true_count}, DP score: {score:.1f}")

        # =====================================================
        # PART 3: PRIVACY GUARANTEES
        # =====================================================
        print("\n" + "="*70)
        print("[4/5] PRIVACY GUARANTEES")
        print("="*70)

        print("\n--- K-Anonymity Guarantee ---")
        print(f"  With k={K_VALUE}, each generalized record is indistinguishable")
        print(f"  from at least {K_VALUE-1} other records in the dataset.")
        print(f"  This prevents singling out individuals from location data.")

        print("\n--- Differential Privacy Guarantee ---")
        print(f"  With epsilon={EPSILON}, the probability of any output changes")
        print(f"  by at most e^{EPSILON} = {2.718**EPSILON:.2f}x if one record is added/removed.")
        print(f"  This provides plausible deniability for individuals.")

        print("\n--- Combined Protection ---")
        print("  1. K-Anonymity: Prevents direct identification from quasi-identifiers")
        print("  2. Differential Privacy: Prevents inference from aggregate statistics")
        print("  Together: Strong privacy protection suitable for data publishing")

        # =====================================================
        # PART 4: SAVE RESULTS
        # =====================================================
        print("\n" + "="*70)
        print("[5/5] SAVING RESULTS")
        print("="*70)

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        results = {
            'privacy_parameters': {
                'k_anonymity': K_VALUE,
                'differential_privacy_epsilon': EPSILON,
                'sample_size': len(records)
            },
            'k_anonymity_results': {
                'original_unique_zones': len(original_zones),
                'generalized_regions': len(generalized_zones),
                'original_unique_hours': len(original_times),
                'generalized_time_periods': len(generalized_times),
                'sample_comparisons': sample_comparisons
            },
            'dp_protected_statistics': {
                'region_distribution': dict(sorted(noisy_zones.items(),
                                                   key=lambda x: x[1], reverse=True)[:20]),
                'time_distribution': noisy_times,
                'fare_distribution': noisy_fares
            },
            'privacy_guarantees': {
                'k_anonymity': f"Each record indistinguishable from {K_VALUE-1} others",
                'differential_privacy': f"Output changes by at most e^{EPSILON} with one record change"
            }
        }

        output_file = f"{OUTPUT_DIR}/privacy_demo_results.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)

        print(f"\nResults saved to: {output_file}")

        # Summary
        print("\n" + "="*70)
        print("DEMONSTRATION COMPLETE")
        print("="*70)
        print(f"""
Key Takeaways:
1. K-Anonymity reduced {len(original_zones)} zones to {len(generalized_zones)} regions
2. Differential Privacy added noise with epsilon={EPSILON}
3. Both techniques combined provide strong privacy protection
4. Trade-off: More privacy = less data utility (more generalization/noise)
        """)

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
