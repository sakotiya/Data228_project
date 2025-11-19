#!/usr/bin/env python3
"""
LSH (Locality Sensitive Hashing) Stream Simulator
Focuses on finding similar items in streaming data
Identifies similar taxi routes, trip patterns, and passenger behaviors
"""

from pyspark.sql import SparkSession
import time
import json
from datetime import datetime
import sys
import os

# Import LSH algorithm
sys.path.append(os.path.dirname(__file__))
from lsh import LSH, CustomerBehaviorProfile


def create_spark_session(use_s3=False):
    """Create Spark session for streaming"""
    os.environ.pop('HADOOP_CONF_DIR', None)
    os.environ.pop('HADOOP_HOME', None)

    builder = SparkSession.builder \
        .appName("StreamTide-LSH-Simulator")

    if use_s3:
        # S3 configuration for large-scale processing
        spark = builder \
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
    else:
        # Local configuration
        spark = builder \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()

    return spark


class LSHStreamProcessor:
    """Process streaming data with LSH for similarity detection"""

    def __init__(self, num_hash_functions=100, num_bands=20):
        # LSH for route similarity (zone patterns)
        self.lsh_routes = LSH(num_hash_functions=num_hash_functions, num_bands=num_bands)

        # LSH for time-based patterns (hours visited)
        self.lsh_time_patterns = LSH(num_hash_functions=num_hash_functions, num_bands=num_bands)

        # LSH for customer behavior patterns
        self.lsh_customer_behavior = LSH(num_hash_functions=num_hash_functions, num_bands=num_bands)

        # Store trip data for similarity queries
        self.trip_routes = {}  # trip_id -> set of zones
        self.vendor_zones = {}  # vendor_id -> set of zones they serve
        self.time_patterns = {}  # date -> set of hours with trips

        # Customer behavior tracking (using pickup zone + hour as proxy for customer)
        self.customer_profiles = {}  # customer_key -> CustomerBehaviorProfile
        self.behavior_signatures = {}  # customer_key -> behavior signature set

        # Track similar pairs found
        self.similar_routes = []
        self.similar_vendors = []
        self.similar_customers = []

        # Statistics
        self.stats = {
            'start_time': datetime.now(),
            'total_records': 0,
            'total_batches': 0,
            'batch_times': [],
            'routes_indexed': 0,
            'vendors_tracked': 0,
            'customers_tracked': 0,
            'similarity_checks': 0
        }

        # Popular routes tracking
        self.popular_routes = []
        self.similar_route_groups = []

        # Customer behavior patterns
        self.customer_behavior_clusters = []

        self.num_hash_functions = num_hash_functions
        self.num_bands = num_bands

    def process_batch(self, batch_df, batch_id):
        """Process each micro-batch"""
        print(f"\n{'='*70}")
        print(f"🔍 Processing Batch #{batch_id}")
        print(f"{'='*70}")

        batch_start = time.time()
        records = batch_df.collect()
        batch_count = len(records)

        print(f"Records in batch: {batch_count:,}")

        routes_in_batch = 0
        new_vendors = 0
        new_customers = 0

        for row in records:
            record = row.asDict()

            # Extract fields - handle both Yellow Taxi and FHV (staging) schemas
            # Yellow Taxi: PULocationID, DOLocationID, VendorID, tpep_pickup_datetime
            # FHV/Staging: PUlocationID, DOlocationID, dispatching_base_num, pickup_datetime
            pu_zone = str(record.get('PULocationID') or record.get('PUlocationID') or 'unknown')
            do_zone = str(record.get('DOLocationID') or record.get('DOlocationID') or 'unknown')
            vendor = str(record.get('VendorID') or record.get('dispatching_base_num') or 'unknown')
            pickup_time = str(record.get('tpep_pickup_datetime') or record.get('pickup_datetime') or '')

            # Extract additional fields for customer behavior (may not exist in FHV data)
            fare = float(record.get('fare_amount', 0) or 0)
            tip = float(record.get('tip_amount', 0) or 0)
            total = float(record.get('total_amount', 0) or 0)
            distance = float(record.get('trip_distance', 0) or 0)
            payment_type = str(record.get('payment_type', 'unknown'))

            # Create trip identifier
            trip_id = f"trip_{self.stats['total_records'] + routes_in_batch}"

            # Build route signature (set of zones)
            route_zones = {pu_zone, do_zone}

            # Only index non-trivial routes (more than 1 unique zone)
            if len(route_zones) > 1 and 'unknown' not in route_zones:
                self.trip_routes[trip_id] = route_zones
                self.lsh_routes.add_set(trip_id, route_zones)
                routes_in_batch += 1
                self.stats['routes_indexed'] += 1

            # Track vendor zones
            if vendor not in self.vendor_zones:
                self.vendor_zones[vendor] = set()
                new_vendors += 1
                self.stats['vendors_tracked'] += 1

            self.vendor_zones[vendor].add(pu_zone)
            self.vendor_zones[vendor].add(do_zone)

            # Track time patterns and build customer behavior profiles
            try:
                if pickup_time:
                    dt = datetime.fromisoformat(pickup_time)
                    date_key = dt.strftime('%Y-%m-%d')
                    hour = str(dt.hour)

                    if date_key not in self.time_patterns:
                        self.time_patterns[date_key] = set()
                    self.time_patterns[date_key].add(hour)

                    # Build customer behavior profile
                    # Use pickup zone + hour as proxy for "customer type"
                    customer_key = f"zone_{pu_zone}_hour_{hour}"

                    if customer_key not in self.customer_profiles:
                        self.customer_profiles[customer_key] = CustomerBehaviorProfile()
                        new_customers += 1
                        self.stats['customers_tracked'] += 1

                    profile = self.customer_profiles[customer_key]
                    profile.pickup_zones.add(pu_zone)
                    profile.dropoff_zones.add(do_zone)
                    profile.trip_hours.add(hour)
                    profile.trip_count += 1

                    # Categorize fare
                    if fare < 10:
                        profile.fare_ranges.add('low')
                    elif fare < 25:
                        profile.fare_ranges.add('medium')
                    elif fare < 50:
                        profile.fare_ranges.add('high')
                    else:
                        profile.fare_ranges.add('premium')

                    # Categorize tip behavior
                    if total > 0:
                        tip_pct = (tip / total) * 100
                        if tip_pct == 0:
                            profile.tip_behavior.add('no_tip')
                        elif tip_pct < 10:
                            profile.tip_behavior.add('low_tip')
                        elif tip_pct < 20:
                            profile.tip_behavior.add('standard_tip')
                        else:
                            profile.tip_behavior.add('generous_tip')

                    # Categorize distance
                    if distance < 2:
                        profile.trip_distances.add('short')
                    elif distance < 5:
                        profile.trip_distances.add('medium')
                    else:
                        profile.trip_distances.add('long')

                    # Payment type
                    profile.payment_types.add(payment_type)

            except:
                pass

        # Update statistics
        self.stats['total_records'] += batch_count
        self.stats['total_batches'] += 1

        batch_time = time.time() - batch_start
        self.stats['batch_times'].append(batch_time)

        # Print batch summary
        print(f"\n📊 Batch Summary:")
        print(f"   ├─ Records processed: {batch_count:,}")
        print(f"   ├─ Routes indexed: {routes_in_batch}")
        print(f"   ├─ New vendors: {new_vendors}")
        print(f"   ├─ New customer profiles: {new_customers}")
        print(f"   ├─ Processing time: {batch_time:.2f}s")
        print(f"   └─ Throughput: {batch_count/batch_time:.0f} records/sec" if batch_time > 0 else "   └─ Throughput: N/A")

        # Perform similarity analysis every 5 batches
        if (batch_id + 1) % 5 == 0:
            self._find_similar_items()
            self._find_popular_routes()
            self._find_customer_behaviors()
            self._print_similarity_analysis()

    def _find_similar_items(self):
        """Find similar routes and vendors using LSH"""
        print(f"\n🔍 Running similarity detection...")

        # Find similar routes (sample a few for demonstration)
        sample_trip_ids = list(self.trip_routes.keys())[-100:]  # Last 100 routes

        for trip_id in sample_trip_ids[:10]:  # Check similarity for 10 routes
            route_set = self.trip_routes[trip_id]
            candidates = self.lsh_routes.find_similar(trip_id, route_set)

            for candidate_id in candidates:
                if candidate_id != trip_id and candidate_id in self.trip_routes:
                    # Calculate actual Jaccard similarity
                    candidate_set = self.trip_routes[candidate_id]
                    similarity = self._jaccard_similarity(route_set, candidate_set)

                    if similarity >= 0.5:  # At least 50% similar
                        self.similar_routes.append({
                            'trip1': trip_id,
                            'trip2': candidate_id,
                            'similarity': similarity,
                            'zones1': list(route_set),
                            'zones2': list(candidate_set)
                        })

        self.stats['similarity_checks'] += len(sample_trip_ids[:10])

        # Also find similar vendors
        self._find_similar_vendors()

    def _find_popular_routes(self):
        """Find popular routes using LSH clustering"""
        print(f"🔥 Finding popular routes...")

        # Method 1: Find exact popular route patterns
        self.popular_routes = self.lsh_routes.find_route_popularity(
            self.trip_routes, top_n=20
        )

        # Method 2: Find similar route groups (clusters)
        if len(self.trip_routes) > 100:
            # Sample for performance
            sample_routes = dict(list(self.trip_routes.items())[-1000:])
            self.similar_route_groups = self.lsh_routes.find_similar_route_groups(
                sample_routes, similarity_threshold=0.5
            )
        else:
            self.similar_route_groups = self.lsh_routes.find_similar_route_groups(
                self.trip_routes, similarity_threshold=0.5
            )

    def _find_customer_behaviors(self):
        """Find similar customer behavior patterns using LSH"""
        print(f"👥 Analyzing customer behavior patterns...")

        # Index customer behavior profiles in LSH
        for customer_key, profile in list(self.customer_profiles.items())[-500:]:  # Last 500 for performance
            if profile.trip_count >= 2:  # Only profiles with multiple trips
                behavior_sig = profile.get_behavior_signature()
                if len(behavior_sig) >= 3:  # Need meaningful signature
                    self.behavior_signatures[customer_key] = behavior_sig
                    self.lsh_customer_behavior.add_set(customer_key, behavior_sig)

        # Find similar customer behaviors
        self.similar_customers = []
        sample_customers = list(self.behavior_signatures.keys())[-50:]  # Sample for performance

        for customer_key in sample_customers:
            sig = self.behavior_signatures[customer_key]
            candidates = self.lsh_customer_behavior.find_similar(customer_key, sig)

            for candidate_key in candidates[:5]:  # Top 5 similar
                if candidate_key != customer_key and candidate_key in self.behavior_signatures:
                    candidate_sig = self.behavior_signatures[candidate_key]

                    # Calculate similarity
                    intersection = len(sig & candidate_sig)
                    union = len(sig | candidate_sig)
                    similarity = intersection / union if union > 0 else 0

                    if similarity >= 0.4:  # 40% similarity threshold
                        self.similar_customers.append({
                            'customer1': customer_key,
                            'customer2': candidate_key,
                            'similarity': similarity,
                            'profile1': self.customer_profiles[customer_key],
                            'profile2': self.customer_profiles[candidate_key]
                        })

        # Identify customer behavior clusters
        self._cluster_customer_behaviors()

    def _cluster_customer_behaviors(self):
        """Cluster customers by behavior patterns"""
        # Group by dominant behavior characteristics
        behavior_groups = {
            'morning_commuters': [],
            'evening_commuters': [],
            'business_travelers': [],
            'budget_conscious': [],
            'premium_riders': [],
            'generous_tippers': [],
            'short_trips': [],
            'long_distance': []
        }

        for customer_key, profile in self.customer_profiles.items():
            sig = profile.get_behavior_signature()

            # Classify by time
            if 'period_morning_rush' in sig:
                behavior_groups['morning_commuters'].append(customer_key)
            if 'period_evening_rush' in sig:
                behavior_groups['evening_commuters'].append(customer_key)

            # Classify by fare
            if 'fare_premium' in sig or 'fare_high' in sig:
                behavior_groups['premium_riders'].append(customer_key)
            if 'fare_low' in sig:
                behavior_groups['budget_conscious'].append(customer_key)

            # Classify by tip
            if 'tip_generous_tip' in sig:
                behavior_groups['generous_tippers'].append(customer_key)

            # Classify by distance
            if 'dist_short' in sig:
                behavior_groups['short_trips'].append(customer_key)
            if 'dist_long' in sig:
                behavior_groups['long_distance'].append(customer_key)

        # Store cluster sizes
        self.customer_behavior_clusters = [
            {'name': name, 'count': len(members), 'sample': members[:5]}
            for name, members in behavior_groups.items()
            if len(members) > 0
        ]
        self.customer_behavior_clusters.sort(key=lambda x: x['count'], reverse=True)

    def _find_similar_vendors(self):
        """Find similar vendors (vendors serving similar zone sets)"""
        if len(self.vendor_zones) > 1:
            vendors = list(self.vendor_zones.keys())[:5]  # Sample 5 vendors
            for i, vendor1 in enumerate(vendors):
                for vendor2 in vendors[i+1:]:
                    zones1 = self.vendor_zones[vendor1]
                    zones2 = self.vendor_zones[vendor2]

                    similarity = self._jaccard_similarity(zones1, zones2)
                    if similarity >= 0.3:  # At least 30% overlap
                        self.similar_vendors.append({
                            'vendor1': vendor1,
                            'vendor2': vendor2,
                            'similarity': similarity,
                            'zones1': len(zones1),
                            'zones2': len(zones2),
                            'common_zones': len(zones1 & zones2)
                        })

    def _jaccard_similarity(self, set1, set2):
        """Calculate Jaccard similarity between two sets"""
        if len(set1) == 0 and len(set2) == 0:
            return 1.0
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0

    def _print_similarity_analysis(self):
        """Print LSH similarity analysis"""
        print(f"\n{'='*70}")
        print(f"🔗 LSH SIMILARITY ANALYSIS")
        print(f"{'='*70}")

        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()

        print(f"\n⏱️  Performance:")
        print(f"   ├─ Runtime: {elapsed:.1f}s")
        print(f"   ├─ Total Records: {self.stats['total_records']:,}")
        print(f"   └─ Throughput: {self.stats['total_records']/elapsed:.0f} records/sec")

        print(f"\n⚙️  LSH Configuration:")
        print(f"   ├─ Hash Functions: {self.num_hash_functions}")
        print(f"   ├─ Bands: {self.num_bands}")
        print(f"   ├─ Rows per Band: {self.num_hash_functions // self.num_bands}")
        lsh_stats = self.lsh_routes.get_statistics()
        print(f"   └─ Buckets: {lsh_stats['structure']['total_buckets']}")

        print(f"\n📊 Indexing Statistics:")
        print(f"   ├─ Routes Indexed: {self.stats['routes_indexed']:,}")
        print(f"   ├─ Vendors Tracked: {self.stats['vendors_tracked']}")
        print(f"   ├─ Customer Profiles: {self.stats['customers_tracked']}")
        print(f"   ├─ Time Patterns: {len(self.time_patterns)} days")
        print(f"   └─ Similarity Checks: {self.stats['similarity_checks']}")

        # Show POPULAR ROUTES (main feature)
        print(f"\n🔥 POPULAR ROUTES (Most Frequent Patterns):")
        if self.popular_routes:
            for i, route in enumerate(self.popular_routes[:10], 1):
                zones_str = ' → '.join(route['zones'][:4])
                if len(route['zones']) > 4:
                    zones_str += f" (+{len(route['zones'])-4} more)"
                print(f"\n   #{i} - {route['count']} trips")
                print(f"   └─ Zones: {zones_str}")
        else:
            print(f"   └─ No popular routes found yet")

        # Show SIMILAR ROUTE GROUPS (clusters)
        print(f"\n📊 SIMILAR ROUTE GROUPS (LSH Clusters):")
        if self.similar_route_groups:
            for i, group in enumerate(self.similar_route_groups[:5], 1):
                print(f"\n   Cluster {i}: {group['size']} similar trips")
                print(f"   ├─ Unique zones covered: {group['num_unique_zones']}")
                print(f"   └─ Sample zones: {group['zones'][:5]}")
        else:
            print(f"   └─ No route clusters found yet")

        # Show similar route pairs
        print(f"\n🎯 Similar Route Pairs:")
        if self.similar_routes:
            # Show top 5 most similar
            sorted_routes = sorted(self.similar_routes,
                                 key=lambda x: x['similarity'],
                                 reverse=True)[:5]
            for i, pair in enumerate(sorted_routes, 1):
                print(f"\n   Pair {i}:")
                print(f"   ├─ Routes: {pair['trip1']} ↔ {pair['trip2']}")
                print(f"   ├─ Similarity: {pair['similarity']:.2%}")
                print(f"   ├─ Zones 1: {pair['zones1']}")
                print(f"   └─ Zones 2: {pair['zones2']}")
        else:
            print(f"   └─ No similar routes found yet (threshold: 50% Jaccard)")

        # Show similar vendors
        print(f"\n🚕 Similar Vendors (Zone Overlap):")
        if self.similar_vendors:
            sorted_vendors = sorted(self.similar_vendors,
                                  key=lambda x: x['similarity'],
                                  reverse=True)[:5]
            for i, pair in enumerate(sorted_vendors, 1):
                print(f"\n   Pair {i}:")
                print(f"   ├─ Vendors: {pair['vendor1']} ↔ {pair['vendor2']}")
                print(f"   ├─ Similarity: {pair['similarity']:.2%}")
                print(f"   └─ Common Zones: {pair['common_zones']}")
        else:
            print(f"   └─ No vendor overlap found yet (threshold: 30% Jaccard)")

        # Show vendor zone coverage
        print(f"\n📍 Vendor Zone Coverage:")
        sorted_vendors = sorted(self.vendor_zones.items(),
                              key=lambda x: len(x[1]),
                              reverse=True)[:5]
        for vendor, zones in sorted_vendors:
            print(f"   ├─ Vendor {vendor}: {len(zones)} unique zones")

        # Show CUSTOMER BEHAVIOR PATTERNS
        print(f"\n👥 CUSTOMER BEHAVIOR PATTERNS:")
        if self.customer_behavior_clusters:
            for cluster in self.customer_behavior_clusters[:8]:
                name = cluster['name'].replace('_', ' ').title()
                print(f"   ├─ {name}: {cluster['count']} profiles")
        else:
            print(f"   └─ No customer patterns found yet")

        # Show similar customer behaviors
        print(f"\n🔗 Similar Customer Behaviors:")
        if self.similar_customers:
            sorted_customers = sorted(self.similar_customers,
                                     key=lambda x: x['similarity'],
                                     reverse=True)[:5]
            for i, pair in enumerate(sorted_customers, 1):
                c1 = pair['customer1'].replace('zone_', 'Z').replace('_hour_', '@')
                c2 = pair['customer2'].replace('zone_', 'Z').replace('_hour_', '@')
                p1 = pair['profile1']
                p2 = pair['profile2']
                print(f"\n   Pair {i}:")
                print(f"   ├─ Profiles: {c1} ↔ {c2}")
                print(f"   ├─ Similarity: {pair['similarity']:.2%}")
                print(f"   ├─ Trips: {p1.trip_count} vs {p2.trip_count}")
                print(f"   └─ Behaviors: {list(p1.fare_ranges)[:2]}, {list(p1.tip_behavior)[:2]}")
        else:
            print(f"   └─ No similar customers found yet (threshold: 40%)")

    def save_results(self, output_dir="./streaming_results_lsh"):
        """Save LSH results"""
        os.makedirs(output_dir, exist_ok=True)

        # Prepare vendor zone data
        vendor_coverage = {
            vendor: {
                'num_zones': len(zones),
                'zones': list(zones)[:50]  # Limit to first 50 for readability
            }
            for vendor, zones in self.vendor_zones.items()
        }

        # Compile summary
        summary = {
            'run_metadata': {
                'start_time': str(self.stats['start_time']),
                'end_time': str(datetime.now()),
                'duration_seconds': (datetime.now() - self.stats['start_time']).total_seconds(),
                'total_records': self.stats['total_records'],
                'total_batches': self.stats['total_batches']
            },
            'configuration': {
                'num_hash_functions': self.num_hash_functions,
                'num_bands': self.num_bands,
                'rows_per_band': self.num_hash_functions // self.num_bands
            },
            'indexing_stats': {
                'routes_indexed': self.stats['routes_indexed'],
                'vendors_tracked': self.stats['vendors_tracked'],
                'time_patterns': len(self.time_patterns),
                'similarity_checks': self.stats['similarity_checks']
            },
            'popular_routes': self.popular_routes[:20],
            'similar_route_groups': self.similar_route_groups[:20],
            'similar_routes': sorted(self.similar_routes,
                                   key=lambda x: x['similarity'],
                                   reverse=True)[:20],  # Top 20
            'similar_vendors': sorted(self.similar_vendors,
                                    key=lambda x: x['similarity'],
                                    reverse=True)[:20],
            'vendor_coverage': vendor_coverage,
            'customer_behavior_clusters': self.customer_behavior_clusters,
            'similar_customers_count': len(self.similar_customers),
            'lsh_statistics': self.lsh_routes.get_statistics(),
            'performance': {
                'avg_batch_time': sum(self.stats['batch_times']) / len(self.stats['batch_times']) if self.stats['batch_times'] else 0,
                'throughput_records_per_sec': self.stats['total_records'] / sum(self.stats['batch_times']) if self.stats['batch_times'] else 0
            }
        }

        # Save LSH structure stats
        self.lsh_routes.save_stats(f"{output_dir}/lsh_routes_stats.json")

        # Save complete summary
        with open(f"{output_dir}/lsh_summary.json", 'w') as f:
            json.dump(summary, f, indent=2, default=str)

        # Save similar routes in separate file for easy access
        with open(f"{output_dir}/similar_routes.json", 'w') as f:
            json.dump({'similar_routes': summary['similar_routes']}, f, indent=2)

        # Save vendor analysis
        with open(f"{output_dir}/vendor_analysis.json", 'w') as f:
            json.dump({
                'vendor_coverage': vendor_coverage,
                'similar_vendors': summary['similar_vendors']
            }, f, indent=2)

        # Save popular routes analysis
        with open(f"{output_dir}/popular_routes.json", 'w') as f:
            json.dump({
                'popular_routes': summary['popular_routes'],
                'similar_route_groups': summary['similar_route_groups'],
                'total_routes_indexed': self.stats['routes_indexed']
            }, f, indent=2)

        print(f"\n✅ All LSH results saved to {output_dir}/")
        print(f"   ├─ lsh_summary.json - Complete analysis")
        print(f"   ├─ popular_routes.json - Most frequent route patterns")
        print(f"   ├─ similar_routes.json - Similar route pairs")
        print(f"   └─ vendor_analysis.json - Vendor zone coverage")
        return output_dir


def main():
    """Main LSH streaming simulation"""

    # Configuration - Choose data source
    USE_S3 = True  # Set to False for local testing

    if USE_S3:
        # S3 Configuration for full 2025 dataset
        DATA_PATH = "s3a://data228-bigdata-nyc/staging/2025/"
        OUTPUT_DIR = "./streaming_results_lsh_s3"
        BATCH_SIZE = 1000000  # 1M records per batch
        MAX_BATCHES = 20  # Process 20M records
    else:
        # Local Configuration for testing
        DATA_PATH = "/Users/shreya/SJSU-Github/DA228/streaming/sample_data/yellow_tripdata_2025-01.parquet"
        OUTPUT_DIR = "./streaming_results_lsh"
        BATCH_SIZE = 10000
        MAX_BATCHES = 10

    NUM_HASH_FUNCTIONS = 100
    NUM_BANDS = 20

    print(f"""
╔════════════════════════════════════════════════════════════════════╗
║         StreamTide LSH Similarity Detection Simulator             ║
║              Customer Behavior Pattern Analysis                    ║
╚════════════════════════════════════════════════════════════════════╝
    Input: {DATA_PATH}
    Mode: {'S3 (Large Scale)' if USE_S3 else 'Local (Testing)'}
    Algorithm: LSH with MinHash
    Hash Functions: {NUM_HASH_FUNCTIONS}
    Bands: {NUM_BANDS}
    Batch Size: {BATCH_SIZE:,}
    Max Batches: {MAX_BATCHES}
    Output: {OUTPUT_DIR}
    Purpose: Find popular routes, similar patterns & customer behaviors
════════════════════════════════════════════════════════════════════
    """)

    # Create Spark session
    spark = create_spark_session(use_s3=USE_S3)
    spark.sparkContext.setLogLevel("WARN")

    # Create LSH processor
    processor = LSHStreamProcessor(
        num_hash_functions=NUM_HASH_FUNCTIONS,
        num_bands=NUM_BANDS
    )

    try:
        print(f"📁 Reading data from {'S3' if USE_S3 else 'local file'}...")
        df = spark.read.parquet(DATA_PATH)

        total_records = df.count()
        print(f"✅ Total records available: {total_records:,}")

        # Debug: Show schema to identify correct column names
        print(f"\n📋 Data Schema:")
        df.printSchema()
        print(f"\n📋 Sample columns: {df.columns[:10]}")

        # Calculate number of batches
        num_batches = min(MAX_BATCHES, (total_records // BATCH_SIZE) + 1)
        print(f"📊 Will process {num_batches} batches of {BATCH_SIZE:,} records each")
        print(f"📊 Total records to process: ~{num_batches * BATCH_SIZE:,}")

        print(f"\n🚀 Starting LSH streaming simulation...\n")

        # Convert to RDD for better batch processing
        rdd = df.rdd.zipWithIndex()

        for batch_id in range(num_batches):
            start_idx = batch_id * BATCH_SIZE
            end_idx = start_idx + BATCH_SIZE

            # Filter records for this batch
            batch_rdd = rdd.filter(lambda x: start_idx <= x[1] < end_idx).map(lambda x: x[0])

            if batch_rdd.isEmpty():
                print(f"\n⚠️  No more records at batch {batch_id}")
                break

            # Convert back to DataFrame
            batch_df = spark.createDataFrame(batch_rdd, df.schema)

            processor.process_batch(batch_df, batch_id)
            time.sleep(0.01)  # Small delay between batches

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")

    finally:
        # Final similarity check
        processor._find_similar_items()
        processor._find_popular_routes()
        processor._find_customer_behaviors()

        # Final statistics
        processor._print_similarity_analysis()

        # Save results
        output_dir = processor.save_results(OUTPUT_DIR)

        print(f"\n✨ LSH streaming simulation complete!")
        print(f"📂 Results: {output_dir}/")

        spark.stop()


if __name__ == "__main__":
    main()
