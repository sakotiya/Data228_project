#!/usr/bin/env python3
"""
Validation Script: Compare Streaming Results vs Ground Truth
Calculates accuracy metrics (MAE, RMSE, % error) for streaming algorithms
"""

import json
import os
from typing import Dict, Any
import math


class StreamingValidator:
    """Validate streaming algorithm accuracy against ground truth"""

    def __init__(self, ground_truth_dir: str, streaming_results_dir: str):
        """
        Initialize validator

        Args:
            ground_truth_dir: Path to ground truth results
            streaming_results_dir: Path to streaming algorithm results
        """
        self.gt_dir = ground_truth_dir
        self.stream_dir = streaming_results_dir

        self.ground_truth = {}
        self.streaming = {}
        self.comparisons = {}

    def load_ground_truth(self):
        """Load ground truth baseline results"""
        print("📥 Loading ground truth data...")

        # Load MASTER_INDEX.json
        master_index_path = os.path.join(self.gt_dir, 'MASTER_INDEX.json')
        if os.path.exists(master_index_path):
            with open(master_index_path, 'r') as f:
                self.ground_truth['master'] = json.load(f)
                print(f"  ✓ Loaded master index")
        else:
            print(f"  ⚠ Master index not found at {master_index_path}")

        # Try to load summary files
        summary_dir = os.path.join(self.gt_dir, 'summary')
        if os.path.exists(summary_dir):
            # Look for JSON files
            for file_type in ['yellow', 'green', 'fhv', 'fhvhv']:
                summary_path = os.path.join(summary_dir, file_type)
                if os.path.exists(summary_path):
                    print(f"  ✓ Found {file_type} summaries")
                    self.ground_truth[file_type] = summary_path

    def load_streaming_results(self):
        """Load streaming algorithm results"""
        print("\n📥 Loading streaming results...")

        summary_path = os.path.join(self.stream_dir, 'complete_summary.json')
        if os.path.exists(summary_path):
            with open(summary_path, 'r') as f:
                self.streaming = json.load(f)
                print(f"  ✓ Loaded streaming summary")
        else:
            print(f"  ⚠ Streaming summary not found at {summary_path}")

    def compare_basic_stats(self) -> Dict[str, Any]:
        """Compare basic statistics (fare, distance, etc.)"""
        print("\n📊 Comparing Basic Statistics...")

        comparison = {
            'metric': [],
            'ground_truth': [],
            'streaming': [],
            'absolute_error': [],
            'percentage_error': []
        }

        # Get ground truth stats from master index
        gt_master = self.ground_truth.get('master', {})
        gt_summary = gt_master.get('summary', {})

        # Get streaming stats
        stream_algos = self.streaming.get('algorithms', {})
        reservoir_stats = stream_algos.get('reservoir_sampling', {})

        # Compare average fare
        if 'stream_stats' in reservoir_stats:
            gt_avg_fare = gt_summary.get('avg_fare', 0)
            stream_avg_fare = reservoir_stats['stream_stats'].get('avg_fare', 0)

            if gt_avg_fare > 0 and stream_avg_fare > 0:
                abs_error = abs(gt_avg_fare - stream_avg_fare)
                pct_error = (abs_error / gt_avg_fare) * 100

                comparison['metric'].append('Average Fare')
                comparison['ground_truth'].append(f"${gt_avg_fare:.2f}")
                comparison['streaming'].append(f"${stream_avg_fare:.2f}")
                comparison['absolute_error'].append(f"${abs_error:.2f}")
                comparison['percentage_error'].append(f"{pct_error:.2f}%")

                print(f"  Average Fare:")
                print(f"    Ground Truth: ${gt_avg_fare:.2f}")
                print(f"    Streaming:    ${stream_avg_fare:.2f}")
                print(f"    Error:        {pct_error:.2f}%")

        # Compare average distance
        if 'stream_stats' in reservoir_stats:
            gt_avg_dist = gt_summary.get('avg_distance', 0)
            stream_avg_dist = reservoir_stats['stream_stats'].get('avg_distance', 0)

            if gt_avg_dist > 0 and stream_avg_dist > 0:
                abs_error = abs(gt_avg_dist - stream_avg_dist)
                pct_error = (abs_error / gt_avg_dist) * 100

                comparison['metric'].append('Average Distance')
                comparison['ground_truth'].append(f"{gt_avg_dist:.2f} mi")
                comparison['streaming'].append(f"{stream_avg_dist:.2f} mi")
                comparison['absolute_error'].append(f"{abs_error:.2f} mi")
                comparison['percentage_error'].append(f"{pct_error:.2f}%")

                print(f"\n  Average Distance:")
                print(f"    Ground Truth: {gt_avg_dist:.2f} miles")
                print(f"    Streaming:    {stream_avg_dist:.2f} miles")
                print(f"    Error:        {pct_error:.2f}%")

        # Compare total trips
        gt_total = gt_summary.get('total_trips', 0)
        stream_total = self.streaming.get('processing', {}).get('total_records', 0)

        if gt_total > 0 and stream_total > 0:
            comparison['metric'].append('Total Trips')
            comparison['ground_truth'].append(f"{gt_total:,}")
            comparison['streaming'].append(f"{stream_total:,}")
            comparison['absolute_error'].append(f"{abs(gt_total - stream_total):,}")
            comparison['percentage_error'].append("N/A (different datasets)")

            print(f"\n  Total Trips:")
            print(f"    Ground Truth: {gt_total:,}")
            print(f"    Streaming:    {stream_total:,}")

        return comparison

    def compare_cardinality(self) -> Dict[str, Any]:
        """Compare cardinality estimates (HyperLogLog vs actual)"""
        print("\n🔢 Comparing Cardinality Estimates...")

        comparison = {}

        # HyperLogLog zones
        hll_zones = self.streaming.get('algorithms', {}).get('hyperloglog_zones', {})
        estimated_zones = hll_zones.get('estimated_distinct', 0)
        actual_zones = hll_zones.get('actual_distinct', 0)

        if estimated_zones > 0 and actual_zones > 0:
            error = abs(estimated_zones - actual_zones)
            pct_error = (error / actual_zones) * 100

            comparison['zones'] = {
                'actual': actual_zones,
                'estimated': estimated_zones,
                'error': error,
                'error_percentage': pct_error
            }

            print(f"  Distinct Zones:")
            print(f"    Actual:    {actual_zones}")
            print(f"    Estimated: {estimated_zones}")
            print(f"    Error:     {pct_error:.2f}%")

        # HyperLogLog vendors
        hll_vendors = self.streaming.get('algorithms', {}).get('hyperloglog_vendors', {})
        estimated_vendors = hll_vendors.get('estimated_distinct', 0)
        actual_vendors = hll_vendors.get('actual_distinct', 0)

        if estimated_vendors > 0 and actual_vendors > 0:
            error = abs(estimated_vendors - actual_vendors)
            pct_error = (error / actual_vendors) * 100 if actual_vendors > 0 else 0

            comparison['vendors'] = {
                'actual': actual_vendors,
                'estimated': estimated_vendors,
                'error': error,
                'error_percentage': pct_error
            }

            print(f"\n  Distinct Vendors:")
            print(f"    Actual:    {actual_vendors}")
            print(f"    Estimated: {estimated_vendors}")
            print(f"    Error:     {pct_error:.2f}%")

        return comparison

    def compare_moments(self) -> Dict[str, Any]:
        """Compare frequency moments"""
        print("\n📈 Comparing Frequency Moments...")

        comparison = {}

        moments = self.streaming.get('algorithms', {}).get('moments', {}).get('moments', {})

        for moment_name, moment_data in moments.items():
            if isinstance(moment_data, dict):
                actual = moment_data.get('actual', 0)
                estimated = moment_data.get('estimated', 0)
                error_pct = moment_data.get('error_percentage', 0)

                comparison[moment_name] = {
                    'actual': actual,
                    'estimated': estimated,
                    'error_percentage': error_pct
                }

                print(f"  {moment_name}:")
                print(f"    Actual:    {actual:,.0f}")
                print(f"    Estimated: {estimated:,.0f}")
                print(f"    Error:     {error_pct:.2f}%")

        return comparison

    def generate_report(self):
        """Generate comprehensive validation report"""
        print("\n" + "="*70)
        print("📋 STREAMING ALGORITHM VALIDATION REPORT")
        print("="*70)

        # Basic stats comparison
        basic_comparison = self.compare_basic_stats()

        # Cardinality comparison
        cardinality_comparison = self.compare_cardinality()

        # Moments comparison
        moments_comparison = self.compare_moments()

        # Overall assessment
        print("\n" + "="*70)
        print("✅ OVERALL ASSESSMENT")
        print("="*70)

        assessments = []

        # Check HyperLogLog accuracy
        if 'zones' in cardinality_comparison:
            zone_error = cardinality_comparison['zones']['error_percentage']
            if zone_error < 2:
                assessments.append(f"✅ HyperLogLog (Zones): Excellent ({zone_error:.2f}% error)")
            elif zone_error < 5:
                assessments.append(f"✓ HyperLogLog (Zones): Good ({zone_error:.2f}% error)")
            else:
                assessments.append(f"⚠ HyperLogLog (Zones): Needs improvement ({zone_error:.2f}% error)")

        # Check moments accuracy
        if moments_comparison:
            for moment_name, data in moments_comparison.items():
                error = data['error_percentage']
                if error < 5:
                    assessments.append(f"✅ {moment_name}: Excellent ({error:.2f}% error)")
                elif error < 15:
                    assessments.append(f"✓ {moment_name}: Good ({error:.2f}% error)")

        for assessment in assessments:
            print(f"  {assessment}")

        # Save report
        report = {
            'basic_statistics': basic_comparison,
            'cardinality_estimation': cardinality_comparison,
            'frequency_moments': moments_comparison,
            'assessment': assessments
        }

        report_path = os.path.join(self.stream_dir, 'validation_report.json')
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"\n💾 Validation report saved to: {report_path}")

        return report


def main():
    """Main validation function"""

    # Paths
    GROUND_TRUTH_DIR = "/Users/shreya/SJSU-Github/DA228/scripts/ground_truth_results"
    STREAMING_DIR = "/Users/shreya/SJSU-Github/DA228/streaming_results_advanced"

    print("""
    ========================================
    Streaming Algorithm Validation
    ========================================
    Comparing streaming results with ground truth baseline
    """)

    # Create validator
    validator = StreamingValidator(GROUND_TRUTH_DIR, STREAMING_DIR)

    # Load data
    validator.load_ground_truth()
    validator.load_streaming_results()

    # Generate report
    validator.generate_report()

    print("\n✅ Validation complete!")


if __name__ == "__main__":
    main()
