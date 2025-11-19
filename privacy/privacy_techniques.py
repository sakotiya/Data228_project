#!/usr/bin/env python3
"""
Privacy Techniques for Streaming Data
Implements K-Anonymity and Differential Privacy
"""

import random
import math
from collections import defaultdict
from typing import Dict, List, Set, Any
import hashlib


# =============================================================================
# K-ANONYMITY IMPLEMENTATION
# =============================================================================

class KAnonymity:
    """
    K-Anonymity implementation for location data protection

    Ensures each record is indistinguishable from at least k-1 other records
    by generalizing quasi-identifiers (like location zones)
    """

    def __init__(self, k: int = 5):
        """
        Initialize K-Anonymity

        Args:
            k: Minimum group size (each record indistinguishable from k-1 others)
        """
        self.k = k

        # Zone to region mappings (generalization hierarchy)
        # NYC zones grouped into broader regions
        self.zone_hierarchy = self._build_zone_hierarchy()

        # Track counts for equivalence classes
        self.equivalence_classes = defaultdict(list)
        self.processed_count = 0
        self.suppressed_count = 0

    def _build_zone_hierarchy(self) -> Dict[str, str]:
        """
        Build generalization hierarchy for NYC zones
        Maps specific zones to broader regions
        """
        # Simplified NYC zone groupings
        # In practice, use actual zone lookup data
        zone_to_region = {}

        # Manhattan regions
        manhattan_zones = list(range(4, 13)) + list(range(13, 25)) + list(range(41, 44)) + \
                         list(range(45, 49)) + list(range(50, 53)) + [74, 75, 79, 87, 88, 90] + \
                         list(range(100, 105)) + list(range(107, 114)) + list(range(116, 129)) + \
                         list(range(137, 144)) + list(range(148, 154)) + list(range(158, 167)) + \
                         list(range(186, 195)) + list(range(202, 210)) + list(range(224, 235)) + \
                         list(range(236, 244)) + list(range(246, 250)) + [261, 262, 263]

        for zone in manhattan_zones:
            if zone < 50:
                zone_to_region[str(zone)] = "Manhattan-Downtown"
            elif zone < 150:
                zone_to_region[str(zone)] = "Manhattan-Midtown"
            else:
                zone_to_region[str(zone)] = "Manhattan-Uptown"

        # Brooklyn regions
        brooklyn_zones = list(range(14, 18)) + list(range(21, 27)) + list(range(29, 35)) + \
                        list(range(36, 40)) + list(range(54, 67)) + list(range(69, 78)) + \
                        list(range(80, 86)) + list(range(89, 98)) + list(range(106, 107)) + \
                        list(range(108, 112)) + list(range(123, 126)) + list(range(133, 135)) + \
                        list(range(149, 151)) + list(range(154, 158)) + list(range(165, 168)) + \
                        list(range(177, 182)) + list(range(188, 191)) + list(range(195, 198)) + \
                        list(range(210, 218)) + list(range(219, 223)) + list(range(225, 228)) + \
                        list(range(240, 244)) + list(range(248, 251)) + list(range(254, 258))

        for zone in brooklyn_zones:
            zone_to_region[str(zone)] = "Brooklyn"

        # Queens regions
        queens_zones = [2, 7, 8, 9, 10, 15, 16, 17, 19, 27, 28, 30, 38, 53, 56, 57, 64, 70, 73,
                       82, 83, 86, 92, 93, 95, 96, 98, 101, 102, 117, 121, 122, 129, 130, 131,
                       132, 134, 135, 138, 139, 145, 146, 157, 160, 171, 173, 175, 179, 180,
                       193, 196, 197, 198, 201, 203, 205, 207, 215, 216, 218, 219, 223, 226,
                       252, 253, 258, 260]

        for zone in queens_zones:
            zone_to_region[str(zone)] = "Queens"

        # Bronx regions
        bronx_zones = [3, 18, 20, 31, 32, 46, 47, 51, 58, 59, 60, 69, 78, 81, 94, 119, 126,
                      136, 147, 159, 167, 168, 169, 174, 182, 183, 184, 199, 200, 208, 212,
                      213, 220, 235, 240, 241, 242, 247, 248, 250, 254, 259]

        for zone in bronx_zones:
            zone_to_region[str(zone)] = "Bronx"

        # Staten Island
        si_zones = [5, 6, 23, 24, 44, 84, 85, 115, 118, 156, 172, 176, 187, 204, 206, 214,
                   221, 245, 251]

        for zone in si_zones:
            zone_to_region[str(zone)] = "Staten Island"

        # Airports
        zone_to_region["1"] = "Airport-Newark"
        zone_to_region["132"] = "Airport-JFK"
        zone_to_region["138"] = "Airport-LaGuardia"

        return zone_to_region

    def generalize_zone(self, zone: str) -> str:
        """
        Generalize a specific zone to a broader region

        Args:
            zone: Specific zone ID

        Returns:
            Generalized region name
        """
        return self.zone_hierarchy.get(zone, "Other")

    def generalize_time(self, hour: int) -> str:
        """
        Generalize specific hour to time period

        Args:
            hour: Hour of day (0-23)

        Returns:
            Generalized time period
        """
        if 6 <= hour < 10:
            return "Morning-Rush"
        elif 10 <= hour < 16:
            return "Midday"
        elif 16 <= hour < 20:
            return "Evening-Rush"
        elif 20 <= hour < 24:
            return "Night"
        else:
            return "Late-Night"

    def generalize_fare(self, fare: float) -> str:
        """
        Generalize specific fare to range

        Args:
            fare: Exact fare amount

        Returns:
            Generalized fare range
        """
        if fare < 10:
            return "Low-$0-10"
        elif fare < 25:
            return "Medium-$10-25"
        elif fare < 50:
            return "High-$25-50"
        else:
            return "Premium-$50+"

    def anonymize_record(self, record: Dict) -> Dict:
        """
        Apply K-Anonymity generalization to a record

        Args:
            record: Original record with quasi-identifiers

        Returns:
            Anonymized record with generalized values
        """
        self.processed_count += 1

        anonymized = record.copy()

        # Generalize location (quasi-identifier)
        if 'pickup_zone' in record or 'pu_zone' in record:
            zone = str(record.get('pickup_zone') or record.get('pu_zone', 'unknown'))
            anonymized['pickup_region'] = self.generalize_zone(zone)
            # Remove specific zone
            anonymized.pop('pickup_zone', None)
            anonymized.pop('pu_zone', None)

        if 'dropoff_zone' in record or 'do_zone' in record:
            zone = str(record.get('dropoff_zone') or record.get('do_zone', 'unknown'))
            anonymized['dropoff_region'] = self.generalize_zone(zone)
            # Remove specific zone
            anonymized.pop('dropoff_zone', None)
            anonymized.pop('do_zone', None)

        # Generalize time (quasi-identifier)
        if 'hour' in record:
            anonymized['time_period'] = self.generalize_time(int(record['hour']))
            # Remove specific hour
            del anonymized['hour']

        # Generalize fare (sensitive attribute protection)
        if 'fare' in record or 'fare_amount' in record:
            fare = float(record.get('fare') or record.get('fare_amount', 0))
            anonymized['fare_range'] = self.generalize_fare(fare)
            # Remove specific fare
            anonymized.pop('fare', None)
            anonymized.pop('fare_amount', None)

        # Create equivalence class key
        eq_key = (
            anonymized.get('pickup_region', ''),
            anonymized.get('dropoff_region', ''),
            anonymized.get('time_period', '')
        )

        self.equivalence_classes[eq_key].append(anonymized)

        return anonymized

    def check_k_anonymity(self) -> Dict:
        """
        Check if dataset satisfies K-Anonymity

        Returns:
            Report on K-Anonymity compliance
        """
        violations = []
        compliant_classes = 0
        total_records_compliant = 0

        for eq_key, records in self.equivalence_classes.items():
            if len(records) < self.k:
                violations.append({
                    'equivalence_class': eq_key,
                    'count': len(records),
                    'shortfall': self.k - len(records)
                })
            else:
                compliant_classes += 1
                total_records_compliant += len(records)

        return {
            'k_value': self.k,
            'total_equivalence_classes': len(self.equivalence_classes),
            'compliant_classes': compliant_classes,
            'violations': len(violations),
            'records_compliant': total_records_compliant,
            'total_records': self.processed_count,
            'compliance_rate': compliant_classes / len(self.equivalence_classes) if self.equivalence_classes else 0,
            'violation_details': violations[:10]  # Top 10 violations
        }

    def suppress_small_groups(self) -> int:
        """
        Suppress (remove) equivalence classes smaller than k

        Returns:
            Number of records suppressed
        """
        suppressed = 0
        keys_to_remove = []

        for eq_key, records in self.equivalence_classes.items():
            if len(records) < self.k:
                suppressed += len(records)
                keys_to_remove.append(eq_key)

        for key in keys_to_remove:
            del self.equivalence_classes[key]

        self.suppressed_count = suppressed
        return suppressed

    def get_statistics(self) -> Dict:
        """Get K-Anonymity statistics"""
        return {
            'k_value': self.k,
            'processed_records': self.processed_count,
            'suppressed_records': self.suppressed_count,
            'equivalence_classes': len(self.equivalence_classes),
            'compliance': self.check_k_anonymity()
        }


# =============================================================================
# DIFFERENTIAL PRIVACY IMPLEMENTATION
# =============================================================================

class DifferentialPrivacy:
    """
    Differential Privacy implementation for aggregate statistics

    Adds calibrated noise to query results to protect individual privacy
    while maintaining statistical utility
    """

    def __init__(self, epsilon: float = 1.0, delta: float = 1e-5):
        """
        Initialize Differential Privacy

        Args:
            epsilon: Privacy budget (lower = more privacy, less utility)
            delta: Probability of privacy breach (for approximate DP)
        """
        self.epsilon = epsilon
        self.delta = delta
        self.queries_answered = 0
        self.total_noise_added = 0

    def _laplace_noise(self, sensitivity: float) -> float:
        """
        Generate Laplace noise for epsilon-differential privacy

        Args:
            sensitivity: Query sensitivity (max change from one record)

        Returns:
            Random noise value
        """
        scale = sensitivity / self.epsilon
        # Laplace distribution: f(x) = (1/2b) * exp(-|x|/b)
        u = random.random() - 0.5
        noise = -scale * math.copysign(1, u) * math.log(1 - 2 * abs(u))
        return noise

    def _gaussian_noise(self, sensitivity: float) -> float:
        """
        Generate Gaussian noise for (epsilon, delta)-differential privacy

        Args:
            sensitivity: Query sensitivity

        Returns:
            Random noise value
        """
        # Gaussian mechanism for (epsilon, delta)-DP
        sigma = sensitivity * math.sqrt(2 * math.log(1.25 / self.delta)) / self.epsilon
        noise = random.gauss(0, sigma)
        return noise

    def add_noise_to_count(self, true_count: int, sensitivity: int = 1) -> int:
        """
        Add noise to a count query

        Args:
            true_count: Actual count value
            sensitivity: How much one record can change the count (usually 1)

        Returns:
            Noisy count
        """
        noise = self._laplace_noise(sensitivity)
        noisy_count = max(0, int(round(true_count + noise)))

        self.queries_answered += 1
        self.total_noise_added += abs(noise)

        return noisy_count

    def add_noise_to_sum(self, true_sum: float, max_value: float) -> float:
        """
        Add noise to a sum query

        Args:
            true_sum: Actual sum value
            max_value: Maximum possible value of one record (sensitivity)

        Returns:
            Noisy sum
        """
        noise = self._laplace_noise(max_value)
        noisy_sum = max(0, true_sum + noise)

        self.queries_answered += 1
        self.total_noise_added += abs(noise)

        return noisy_sum

    def add_noise_to_mean(self, true_mean: float, count: int,
                          min_val: float, max_val: float) -> float:
        """
        Add noise to a mean query using noisy sum / noisy count

        Args:
            true_mean: Actual mean value
            count: Number of records
            min_val: Minimum possible value
            max_val: Maximum possible value

        Returns:
            Noisy mean
        """
        # Reconstruct sum
        true_sum = true_mean * count

        # Add noise to sum and count separately
        sensitivity_sum = max_val - min_val
        noisy_sum = self.add_noise_to_sum(true_sum, sensitivity_sum)
        noisy_count = self.add_noise_to_count(count)

        if noisy_count <= 0:
            return 0

        noisy_mean = noisy_sum / noisy_count

        # Clip to valid range
        return max(min_val, min(max_val, noisy_mean))

    def add_noise_to_histogram(self, histogram: Dict[str, int],
                               sensitivity: int = 1) -> Dict[str, int]:
        """
        Add noise to each bin of a histogram

        Args:
            histogram: Dictionary of bin -> count
            sensitivity: Sensitivity per bin

        Returns:
            Noisy histogram
        """
        noisy_histogram = {}

        for bin_name, count in histogram.items():
            noisy_count = self.add_noise_to_count(count, sensitivity)
            noisy_histogram[bin_name] = noisy_count

        return noisy_histogram

    def private_top_k(self, items: Dict[str, int], k: int) -> List[tuple]:
        """
        Find top-k items with differential privacy using exponential mechanism

        Args:
            items: Dictionary of item -> count
            k: Number of top items to return

        Returns:
            List of (item, noisy_count) tuples
        """
        if not items:
            return []

        results = []
        remaining = items.copy()

        for _ in range(min(k, len(items))):
            if not remaining:
                break

            # Exponential mechanism: probability proportional to exp(epsilon * score / 2)
            scores = {}
            max_score = max(remaining.values())

            for item, count in remaining.items():
                # Normalize score and apply exponential mechanism
                normalized_score = count / max_score if max_score > 0 else 0
                scores[item] = math.exp(self.epsilon * normalized_score / 2)

            # Sample based on scores
            total_score = sum(scores.values())
            r = random.random() * total_score
            cumulative = 0
            selected_item = None

            for item, score in scores.items():
                cumulative += score
                if r <= cumulative:
                    selected_item = item
                    break

            if selected_item:
                # Add noise to the count
                noisy_count = self.add_noise_to_count(remaining[selected_item])
                results.append((selected_item, noisy_count))
                del remaining[selected_item]

        return results

    def get_statistics(self) -> Dict:
        """Get Differential Privacy statistics"""
        return {
            'epsilon': self.epsilon,
            'delta': self.delta,
            'queries_answered': self.queries_answered,
            'average_noise': self.total_noise_added / self.queries_answered if self.queries_answered > 0 else 0
        }


# =============================================================================
# PRIVACY-PRESERVING STREAM PROCESSOR
# =============================================================================

class PrivacyPreservingProcessor:
    """
    Combines K-Anonymity and Differential Privacy for streaming data
    """

    def __init__(self, k: int = 5, epsilon: float = 1.0):
        """
        Initialize privacy-preserving processor

        Args:
            k: K-Anonymity parameter
            epsilon: Differential Privacy budget
        """
        self.k_anonymity = KAnonymity(k=k)
        self.differential_privacy = DifferentialPrivacy(epsilon=epsilon)

        # Aggregated statistics
        self.route_counts = defaultdict(int)
        self.zone_counts = defaultdict(int)
        self.hourly_counts = defaultdict(int)
        self.fare_stats = defaultdict(list)

        self.processed_count = 0

    def process_record(self, record: Dict) -> Dict:
        """
        Process a record with privacy protection

        Args:
            record: Original record

        Returns:
            Privacy-protected record
        """
        self.processed_count += 1

        # Apply K-Anonymity (generalization)
        anonymized = self.k_anonymity.anonymize_record(record)

        # Track statistics for DP queries
        route_key = f"{anonymized.get('pickup_region', 'Unknown')}->{anonymized.get('dropoff_region', 'Unknown')}"
        self.route_counts[route_key] += 1

        self.zone_counts[anonymized.get('pickup_region', 'Unknown')] += 1
        self.hourly_counts[anonymized.get('time_period', 'Unknown')] += 1

        return anonymized

    def get_private_popular_routes(self, top_k: int = 10) -> List[tuple]:
        """
        Get top-k popular routes with differential privacy

        Args:
            top_k: Number of routes to return

        Returns:
            List of (route, noisy_count) with DP protection
        """
        return self.differential_privacy.private_top_k(dict(self.route_counts), top_k)

    def get_private_zone_histogram(self) -> Dict[str, int]:
        """
        Get zone distribution with DP noise

        Returns:
            Noisy histogram of zone counts
        """
        return self.differential_privacy.add_noise_to_histogram(dict(self.zone_counts))

    def get_private_hourly_distribution(self) -> Dict[str, int]:
        """
        Get hourly distribution with DP noise

        Returns:
            Noisy histogram of hourly counts
        """
        return self.differential_privacy.add_noise_to_histogram(dict(self.hourly_counts))

    def get_privacy_report(self) -> Dict:
        """
        Get comprehensive privacy report

        Returns:
            Report on privacy techniques applied
        """
        return {
            'total_processed': self.processed_count,
            'k_anonymity_stats': self.k_anonymity.get_statistics(),
            'differential_privacy_stats': self.differential_privacy.get_statistics(),
            'private_popular_routes': self.get_private_popular_routes(10),
            'private_zone_distribution': self.get_private_zone_histogram(),
            'private_hourly_distribution': self.get_private_hourly_distribution()
        }


# =============================================================================
# DEMO / TEST
# =============================================================================

if __name__ == "__main__":
    import json

    print("="*60)
    print("Privacy Techniques Demo")
    print("="*60)

    # Initialize privacy processor
    processor = PrivacyPreservingProcessor(k=5, epsilon=1.0)

    # Simulate taxi trip records
    print("\nProcessing sample records...")

    sample_records = [
        {'pu_zone': '161', 'do_zone': '237', 'hour': 8, 'fare_amount': 15.50},
        {'pu_zone': '162', 'do_zone': '236', 'hour': 9, 'fare_amount': 22.00},
        {'pu_zone': '161', 'do_zone': '237', 'hour': 8, 'fare_amount': 14.00},
        {'pu_zone': '132', 'do_zone': '138', 'hour': 14, 'fare_amount': 52.00},
        {'pu_zone': '138', 'do_zone': '237', 'hour': 17, 'fare_amount': 45.00},
        {'pu_zone': '161', 'do_zone': '237', 'hour': 8, 'fare_amount': 16.00},
        {'pu_zone': '79', 'do_zone': '144', 'hour': 22, 'fare_amount': 28.00},
        {'pu_zone': '161', 'do_zone': '236', 'hour': 9, 'fare_amount': 18.50},
        {'pu_zone': '132', 'do_zone': '237', 'hour': 15, 'fare_amount': 48.00},
        {'pu_zone': '161', 'do_zone': '237', 'hour': 8, 'fare_amount': 15.00},
    ]

    # Process records with privacy protection
    for record in sample_records:
        anonymized = processor.process_record(record)
        print(f"  Original: Zone {record['pu_zone']}->{record['do_zone']}, Hour {record['hour']}, Fare ${record['fare_amount']}")
        print(f"  Anonymized: {anonymized.get('pickup_region')}->{anonymized.get('dropoff_region')}, {anonymized.get('time_period')}, {anonymized.get('fare_range')}")
        print()

    # Get privacy report
    report = processor.get_privacy_report()

    print("\n" + "="*60)
    print("Privacy Report")
    print("="*60)

    print(f"\nK-Anonymity (k={report['k_anonymity_stats']['k_value']}):")
    print(f"  Processed records: {report['k_anonymity_stats']['processed_records']}")
    print(f"  Equivalence classes: {report['k_anonymity_stats']['equivalence_classes']}")
    compliance = report['k_anonymity_stats']['compliance']
    print(f"  Compliance rate: {compliance['compliance_rate']*100:.1f}%")

    print(f"\nDifferential Privacy (ε={report['differential_privacy_stats']['epsilon']}):")
    print(f"  Queries answered: {report['differential_privacy_stats']['queries_answered']}")
    print(f"  Average noise added: {report['differential_privacy_stats']['average_noise']:.2f}")

    print(f"\nPrivate Popular Routes (with DP noise):")
    for route, count in report['private_popular_routes']:
        print(f"  {route}: {count} trips")

    print(f"\nPrivate Zone Distribution (with DP noise):")
    for zone, count in report['private_zone_distribution'].items():
        print(f"  {zone}: {count} pickups")

    print(f"\nPrivate Hourly Distribution (with DP noise):")
    for period, count in report['private_hourly_distribution'].items():
        print(f"  {period}: {count} trips")
