#!/usr/bin/env python3
"""
Computing Moments Algorithm for Data Streams
Estimates k-th moments of data distributions using AMS (Alon-Matias-Szegedy) algorithm
"""

import random
import hashlib
from collections import defaultdict
from typing import Dict, List
import json
import math


class MomentEstimator:
    """
    AMS Algorithm for computing k-th frequency moments

    The k-th moment is: Σ(fi^k) where fi is the frequency of element i

    Special cases:
    - 0th moment (F0): Number of distinct elements
    - 1st moment (F1): Total number of elements
    - 2nd moment (F2): Gini index / surprise number
    """

    def __init__(self, num_variables: int = 100):
        """
        Initialize moment estimator

        Args:
            num_variables: Number of random variables to use (reduces variance)
        """
        self.num_variables = num_variables
        self.variables = []  # List of (element, value) tuples
        self.frequencies = defaultdict(int)  # Actual frequencies (for validation)
        self.total_elements = 0

        # Initialize random variables
        for i in range(num_variables):
            self.variables.append({
                'X': None,  # Selected element
                'value': 0   # Current value
            })

    def _select_random_element(self, seed: int) -> int:
        """
        Generate a random selection decision

        Args:
            seed: Random seed

        Returns:
            Random integer
        """
        random.seed(seed + self.total_elements)
        return random.randint(0, self.total_elements + 1)

    def add(self, element: str):
        """
        Add an element to the stream

        Args:
            element: Element identifier
        """
        self.total_elements += 1
        self.frequencies[element] += 1

        # Update each random variable
        for i, var in enumerate(self.variables):
            # Reservoir sampling: select this element with probability 1/n
            if var['X'] is None or random.random() < (1.0 / self.total_elements):
                var['X'] = element
                var['value'] = 1
            elif var['X'] == element:
                var['value'] += 1

    def estimate_moment(self, k: int) -> float:
        """
        Estimate the k-th frequency moment

        Args:
            k: Moment order (0, 1, or 2)

        Returns:
            Estimated k-th moment
        """
        if k == 0:
            # F0: Number of distinct elements
            return len(self.frequencies)

        if k == 1:
            # F1: Total number of elements
            return self.total_elements

        # For k >= 2, use AMS estimator
        estimates = []
        for var in self.variables:
            if var['X'] is not None:
                # Estimate: n * (value^k - (value-1)^k)
                estimate = self.total_elements * (
                    var['value'] ** k - (var['value'] - 1) ** k
                )
                estimates.append(estimate)

        # Return median of estimates to reduce variance
        if not estimates:
            return 0

        estimates.sort()
        median_index = len(estimates) // 2
        return estimates[median_index]

    def actual_moment(self, k: int) -> float:
        """
        Calculate actual k-th moment (for validation)

        Args:
            k: Moment order

        Returns:
            Actual k-th moment
        """
        if k == 0:
            return len(self.frequencies)

        if k == 1:
            return sum(self.frequencies.values())

        # k >= 2
        return sum(freq ** k for freq in self.frequencies.values())

    def get_statistics(self) -> dict:
        """
        Get comprehensive statistics

        Returns:
            Dictionary with statistics
        """
        stats = {
            'total_elements': self.total_elements,
            'distinct_elements': len(self.frequencies),
            'num_variables': self.num_variables,
            'moments': {}
        }

        for k in [0, 1, 2]:
            estimated = self.estimate_moment(k)
            actual = self.actual_moment(k)

            stats['moments'][f'F{k}'] = {
                'estimated': estimated,
                'actual': actual,
                'error': abs(estimated - actual),
                'error_percentage': abs(estimated - actual) / actual * 100 if actual > 0 else 0
            }

        # Additional insights from moments
        if self.total_elements > 0:
            # Gini coefficient (related to F2)
            f2 = self.actual_moment(2)
            n = self.total_elements
            gini = math.sqrt(f2) / n if n > 0 else 0

            stats['insights'] = {
                'average_frequency': self.total_elements / len(self.frequencies) if self.frequencies else 0,
                'surprise_number': f2,  # F2 is the "surprise number"
                'gini_coefficient': gini
            }

        return stats

    def get_frequency_distribution(self) -> dict:
        """Get top frequencies"""
        sorted_freqs = sorted(
            self.frequencies.items(),
            key=lambda x: x[1],
            reverse=True
        )
        return dict(sorted_freqs[:10])

    def save_stats(self, filepath: str):
        """Save statistics to JSON file"""
        stats = self.get_statistics()
        stats['top_frequencies'] = self.get_frequency_distribution()

        with open(filepath, 'w') as f:
            json.dump(stats, f, indent=2)
        print(f"💾 Moments stats saved to {filepath}")

    def __repr__(self):
        f0 = len(self.frequencies)
        f1 = self.total_elements
        f2_est = self.estimate_moment(2)
        return f"MomentEstimator(F0={f0:,}, F1={f1:,}, F2≈{f2_est:,.0f})"


if __name__ == "__main__":
    # Test the Computing Moments algorithm
    print("Testing Computing Moments Algorithm...")

    # Test 1: Uniform distribution
    print("\n--- Test 1: Uniform distribution (each element appears once) ---")
    moment1 = MomentEstimator(num_variables=50)

    for i in range(1000):
        moment1.add(f"element_{i}")

    stats1 = moment1.get_statistics()
    print(f"Total elements: {stats1['total_elements']:,}")
    print(f"Distinct elements: {stats1['distinct_elements']:,}")
    print(f"\nMoments:")
    for k, data in stats1['moments'].items():
        print(f"  {k}: estimated={data['estimated']:,.0f}, actual={data['actual']:,.0f}, error={data['error_percentage']:.2f}%")

    # Test 2: Skewed distribution (power law)
    print("\n--- Test 2: Skewed distribution (Zipf's law) ---")
    moment2 = MomentEstimator(num_variables=50)

    # Zipfian distribution: element i appears with frequency proportional to 1/i
    for i in range(1, 101):
        frequency = int(1000 / i)  # Zipf distribution
        for _ in range(frequency):
            moment2.add(f"element_{i}")

    stats2 = moment2.get_statistics()
    print(f"Total elements: {stats2['total_elements']:,}")
    print(f"Distinct elements: {stats2['distinct_elements']:,}")
    print(f"\nMoments:")
    for k, data in stats2['moments'].items():
        print(f"  {k}: estimated={data['estimated']:,.0f}, actual={data['actual']:,.0f}, error={data['error_percentage']:.2f}%")

    print(f"\nInsights:")
    print(f"  Average frequency: {stats2['insights']['average_frequency']:.2f}")
    print(f"  Surprise number (F2): {stats2['insights']['surprise_number']:,.0f}")
    print(f"  Gini coefficient: {stats2['insights']['gini_coefficient']:.4f}")

    print(f"\nTop 5 frequencies:")
    for elem, freq in list(moment2.get_frequency_distribution().items())[:5]:
        print(f"  {elem}: {freq}")

    # Test 3: Real-world simulation - taxi zones
    print("\n--- Test 3: NYC Taxi zones simulation ---")
    moment3 = MomentEstimator(num_variables=100)

    # Simulate: Popular zones get more trips
    zone_popularity = {
        161: 1000,  # Midtown
        237: 800,   # Upper East Side
        138: 700,   # LaGuardia
        132: 600,   # JFK
        79: 500     # East Village
    }

    # Add popular zones
    for zone, trips in zone_popularity.items():
        for _ in range(trips):
            moment3.add(f"zone_{zone}")

    # Add less popular zones
    for zone in range(1, 50):
        if zone not in zone_popularity:
            for _ in range(random.randint(1, 50)):
                moment3.add(f"zone_{zone}")

    stats3 = moment3.get_statistics()
    print(f"Total pickups: {stats3['total_elements']:,}")
    print(f"Distinct zones: {stats3['distinct_elements']:,}")
    print(f"\nMoments:")
    for k, data in stats3['moments'].items():
        print(f"  {k}: estimated={data['estimated']:,.0f}, actual={data['actual']:,.0f}, error={data['error_percentage']:.2f}%")

    print(f"\n{moment3}")
