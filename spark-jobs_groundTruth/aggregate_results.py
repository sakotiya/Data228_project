#!/usr/bin/env python3
"""
Aggregate existing ground truth summary files
Reads per-file summaries and creates aggregated results + MASTER_INDEX
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json

def create_spark_session():
    return SparkSession.builder \
        .appName("StreamTide-Aggregate") \
        .getOrCreate()

def main():
    OUTPUT_BASE = "s3://data228-bigdata-nyc/ground-truth"

    print("Reading existing summary files and creating aggregations...")

    spark = create_spark_session()
    import builtins

    # Read all existing summary JSON files
    all_summaries = []
    for file_type in ['yellow', 'green', 'fhv', 'fhvhv']:
        try:
            # Read all part files in summary/{type}/*/ directories
            path = f"{OUTPUT_BASE}/summary/{file_type}/*/*"

            # List all files matching the pattern
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark.sparkContext._jvm.java.net.URI(OUTPUT_BASE), hadoop_conf
            )

            # Get all part files
            file_pattern = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
            file_statuses = fs.globStatus(file_pattern)

            if file_statuses:
                for status in file_statuses:
                    file_path = str(status.getPath())
                    if 'part-' in file_path and '_SUCCESS' not in file_path:
                        try:
                            # Read the single file
                            df_file = spark.read.text(file_path)
                            rows = df_file.collect()

                            # Skip empty part files
                            if len(rows) == 0:
                                continue

                            content = rows[0].value
                            summary = json.loads(content)
                            summary['file_type'] = file_type
                            all_summaries.append(summary)
                        except Exception as e:
                            # Skip errors silently (likely empty files or formatting issues)
                            pass

            print(f"  Found {len([s for s in all_summaries if s.get('file_type')==file_type])} {file_type} files")
        except Exception as e:
            print(f"  No {file_type} files found: {e}")

    print(f"\nTotal summaries loaded: {len(all_summaries)}")

    # Group by type and aggregate
    by_type = {}
    for summary in all_summaries:
        ftype = summary.get('file_type', 'unknown')
        if ftype not in by_type:
            by_type[ftype] = []
        by_type[ftype].append(summary)

    print("\n" + "="*80)
    print("AGGREGATING BY FILE TYPE")
    print("="*80 + "\n")

    # Aggregate each type
    for file_type, results in by_type.items():
        print(f"📈 {file_type.upper()} ({len(results)} files)...")

        # Aggregate hourly stats
        all_hourly = []
        for r in results:
            if 'hourly_stats' in r and r['hourly_stats']:
                all_hourly.extend(r['hourly_stats'])

        if all_hourly:
            hourly_df = spark.createDataFrame(all_hourly)

            agg_exprs = [builtins.sum(col('trip_count')).alias('trip_count')]
            if 'avg_fare' in hourly_df.columns:
                agg_exprs.append(mean('avg_fare').alias('avg_fare'))
            if 'avg_distance' in hourly_df.columns:
                agg_exprs.append(mean('avg_distance').alias('avg_distance'))

            agg_hourly = hourly_df.groupBy('hour').agg(*agg_exprs).orderBy('hour')

            # Save (delete first if exists)
            output_path = f"{OUTPUT_BASE}/hourly_stats/{file_type}_aggregated"
            try:
                # Try to delete existing
                hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
                fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                    spark.sparkContext._jvm.java.net.URI(output_path), hadoop_conf
                )
                fs.delete(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(output_path), True)
            except:
                pass

            hourly_json = [row.asDict() for row in agg_hourly.collect()]
            hourly_str = json.dumps(hourly_json, indent=2, default=str)
            spark.sparkContext.parallelize([hourly_str]).saveAsTextFile(output_path)
            print(f"  ✓ Hourly stats saved")

        # Aggregate overall summary
        total_summary = {
            'file_type': file_type,
            'total_files': len(results),
            'total_trips': builtins.sum(r.get('total_trips', 0) for r in results)
        }

        revenues = [r.get('total_revenue', 0) for r in results if 'total_revenue' in r]
        if revenues:
            total_summary['total_revenue'] = builtins.sum(revenues)

        distances = [r.get('total_distance', 0) for r in results if 'total_distance' in r]
        if distances:
            total_summary['total_distance'] = builtins.sum(distances)

        avg_fares = [r.get('avg_fare') for r in results if 'avg_fare' in r and r.get('avg_fare')]
        if avg_fares:
            total_summary['avg_fare'] = builtins.sum(avg_fares) / len(avg_fares)

        # Save overall summary
        output_path = f"{OUTPUT_BASE}/summary/{file_type}_overall_summary"
        try:
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark.sparkContext._jvm.java.net.URI(output_path), hadoop_conf
            )
            fs.delete(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(output_path), True)
        except:
            pass

        summary_str = json.dumps(total_summary, indent=2, default=str)
        spark.sparkContext.parallelize([summary_str]).saveAsTextFile(output_path)

        print(f"  Total trips: {total_summary['total_trips']:,}")
        if 'total_revenue' in total_summary:
            print(f"  Total revenue: ${total_summary['total_revenue']:,.2f}")
        if 'avg_fare' in total_summary:
            print(f"  Avg fare: ${total_summary['avg_fare']:.2f}")
        print(f"  ✓ Overall summary saved\n")

    # Create MASTER_INDEX
    print("Creating MASTER_INDEX...")
    from datetime import datetime

    master_index = {
        'generated_at': datetime.now().isoformat(),
        'total_files_processed': len(all_summaries),
        'file_types': {ft: len(results) for ft, results in by_type.items()},
        'files': []
    }

    for summary in all_summaries:
        master_index['files'].append({
            'file_name': summary.get('file_name', 'unknown'),
            'file_type': summary.get('file_type', 'unknown'),
            'total_trips': summary.get('total_trips', 0),
            'has_temporal': 'hourly_stats' in summary,
            'has_fare': 'fare_distribution' in summary,
            'has_spatial': 'top_pickup_zones' in summary
        })

    # Delete old MASTER_INDEX if exists
    output_path = f"{OUTPUT_BASE}/MASTER_INDEX"
    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jvm.java.net.URI(output_path), hadoop_conf
        )
        fs.delete(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(output_path), True)
    except:
        pass

    index_str = json.dumps(master_index, indent=2, default=str)
    spark.sparkContext.parallelize([index_str]).saveAsTextFile(output_path)

    print(f"✓ MASTER_INDEX created")
    print(f"\n{'='*80}")
    print("✅ AGGREGATION COMPLETE")
    print(f"{'='*80}\n")
    print(f"Results:")
    print(f"  Total files: {len(all_summaries)}")
    print(f"  File types: {list(by_type.keys())}")
    print(f"\nOutput:")
    print(f"  {OUTPUT_BASE}/hourly_stats/")
    print(f"  {OUTPUT_BASE}/summary/*_overall_summary/")
    print(f"  {OUTPUT_BASE}/MASTER_INDEX/")

    spark.stop()

if __name__ == "__main__":
    main()
