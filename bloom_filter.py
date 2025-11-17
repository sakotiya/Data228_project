#!/usr/bin/env python3
"""
Bloom Filter for Duplicate Detection in Streaming Data
Space-efficient probabilistic data structure for set membership testing
"""

import math
import hashlib
from typing import List, Optional
import json

class BloomFilter:
    """
    Bloom Filter implementation for duplicate detection

    Features:
    - Configurable false positive rate
    - Multiple hash functions
    - Memory-efficient bit array
    - Statistics tracking
    """

    def __init__(self, expected_elements: int = 1000000, false_positive_rate: float = 0.01):
        """
        Initialize Bloom Filter

        Args:
            expected_elements: Expected number of elements to add
            false_positive_rate: Desired false positive probability (0-1)
        """
        self.n = expected_elements
        self.p = false_positive_rate

        # Calculate optimal bit array size (m) and hash functions (k)
        # m = -n * ln(p) / (ln(2)^2)
        # k = (m/n) * ln(2)
        self.m = self._optimal_size(expected_elements, false_positive_rate)
        self.k = self._optimal_hash_count(self.m, expected_elements)

        # Bit array (using list of booleans for simplicity)
        self.bit_array = [False] * self.m

        # Statistics
        self.stats = {
            'elements_added': 0,
            'duplicates_detected': 0,
            'false_positives_estimated': 0,
            'queries': 0,
            'bit_array_size': self.m,
            'hash_functions': self.k,
            'expected_elements': expected_elements,
            'target_fp_rate': false_positive_rate
        }

    @staticmethod
    def _optimal_size(n: int, p: float) -> int:
        """Calculate optimal bit array size"""
        m = -(n * math.log(p)) / (math.log(2) ** 2)
        return int(math.ceil(m))

    @staticmethod
    def _optimal_hash_count(m: int, n: int) -> int:
        """Calculate optimal number of hash functions"""
        k = (m / n) * math.log(2)
        return max(1, int(math.ceil(k)))

    def _hash(self, item: str, seed: int) -> int:
        """
        Generate hash value for item with given seed

        Args:
            item: String to hash
            seed: Hash function seed

        Returns:
            Hash value within bit array range
        """
        # Use MD5 with seed for multiple hash functions
        hash_input = f"{item}:{seed}".encode('utf-8')
        hash_digest = hashlib.md5(hash_input).hexdigest()
        hash_int = int(hash_digest, 16)
        return hash_int % self.m

    def add(self, item: str) -> bool:
        """
        Add item to Bloom Filter

        Args:
            item: String identifier (e.g., trip_id)

        Returns:
            True if item was potentially duplicate, False if definitely new
        """
        # Check if item exists before adding
        was_present = self.contains(item)

        # Add item by setting bits for all k hash functions
        for i in range(self.k):
            index = self._hash(item, i)
            self.bit_array[index] = True

        self.stats['elements_added'] += 1

        if was_present:
            self.stats['duplicates_detected'] += 1

        return was_present

    def contains(self, item: str) -> bool:
        """
        Check if item might be in the set

        Args:
            item: String identifier to check

        Returns:
            True if item might be present (could be false positive)
            False if item is definitely not present
        """
        self.stats['queries'] += 1

        # Check all k hash positions
        for i in range(self.k):
            index = self._hash(item, i)
            if not self.bit_array[index]:
                return False  # Definitely not present

        return True  # Might be present (could be false positive)

    def get_statistics(self) -> dict:
        """
        Get Bloom Filter statistics

        Returns:
            Dictionary with statistics and performance metrics
        """
        # Calculate actual false positive rate
        bits_set = sum(self.bit_array)
        fill_ratio = bits_set / self.m if self.m > 0 else 0

        # Actual FP rate: (1 - e^(-kn/m))^k
        if self.stats['elements_added'] > 0:
            actual_fp_rate = (1 - math.exp(
                -self.k * self.stats['elements_added'] / self.m
            )) ** self.k
        else:
            actual_fp_rate = 0

        return {
            'configuration': {
                'bit_array_size': self.m,
                'hash_functions': self.k,
                'expected_elements': self.n,
                'target_fp_rate': self.p
            },
            'usage': {
                'elements_added': self.stats['elements_added'],
                'duplicates_detected': self.stats['duplicates_detected'],
                'queries_performed': self.stats['queries'],
                'duplicate_rate': self.stats['duplicates_detected'] / self.stats['elements_added']
                    if self.stats['elements_added'] > 0 else 0
            },
            'performance': {
                'bits_set': bits_set,
                'fill_ratio': fill_ratio,
                'actual_fp_rate': actual_fp_rate,
                'memory_bytes': self.m // 8,
                'memory_mb': (self.m // 8) / (1024 * 1024)
            }
        }

    def reset(self):
        """Clear the Bloom Filter"""
        self.bit_array = [False] * self.m
        self.stats = {
            'elements_added': 0,
            'duplicates_detected': 0,
            'false_positives_estimated': 0,
            'queries': 0,
            'bit_array_size': self.m,
            'hash_functions': self.k,
            'expected_elements': self.n,
            'target_fp_rate': self.p
        }

    def save_stats(self, filepath: str):
        """Save statistics to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.get_statistics(), f, indent=2)
        print(f"💾 Bloom Filter stats saved to {filepath}")

    def __repr__(self):
        bits_set = sum(self.bit_array)
        return (f"BloomFilter(m={self.m}, k={self.k}, "
                f"elements={self.stats['elements_added']}, "
                f"fill={bits_set}/{self.m})")


class CountingBloomFilter(BloomFilter):
    """
    Counting Bloom Filter - supports deletion
    Uses counters instead of bits
    """

    def __init__(self, expected_elements: int = 1000000, false_positive_rate: float = 0.01):
        super().__init__(expected_elements, false_positive_rate)
        # Replace bit array with counter array
        self.counter_array = [0] * self.m

    def add(self, item: str) -> bool:
        """Add item and increment counters"""
        was_present = self.contains(item)

        for i in range(self.k):
            index = self._hash(item, i)
            self.counter_array[index] += 1

        self.stats['elements_added'] += 1

        if was_present:
            self.stats['duplicates_detected'] += 1

        return was_present

    def contains(self, item: str) -> bool:
        """Check if item exists"""
        self.stats['queries'] += 1

        for i in range(self.k):
            index = self._hash(item, i)
            if self.counter_array[index] == 0:
                return False

        return True

    def remove(self, item: str) -> bool:
        """
        Remove item from filter

        Returns:
            True if item was likely present, False otherwise
        """
        if not self.contains(item):
            return False

        for i in range(self.k):
            index = self._hash(item, i)
            if self.counter_array[index] > 0:
                self.counter_array[index] -= 1

        return True


if __name__ == "__main__":
    # Test Bloom Filter
    print("Testing Bloom Filter...")

    # Create filter expecting 1M elements with 1% FP rate
    bf = BloomFilter(expected_elements=1000000, false_positive_rate=0.01)
    print(f"\n{bf}")

    # Add some test data
    test_ids = [f"trip_{i}" for i in range(10000)]
    duplicates_found = 0

    for trip_id in test_ids:
        if bf.add(trip_id):
            duplicates_found += 1

    print(f"\nAdded {len(test_ids)} unique items")
    print(f"False duplicates detected: {duplicates_found}")

    # Test with actual duplicates
    print("\nTesting duplicate detection...")
    duplicate_count = 0
    for trip_id in test_ids[:100]:  # Re-add first 100
        if bf.add(trip_id):
            duplicate_count += 1

    print(f"Correctly detected duplicates: {duplicate_count}/100")

    # Print statistics
    print("\nBloom Filter Statistics:")
    stats = bf.get_statistics()
    print(json.dumps(stats, indent=2))

    # Test Counting Bloom Filter
    print("\n" + "="*50)
    print("Testing Counting Bloom Filter...")
    cbf = CountingBloomFilter(expected_elements=1000, false_positive_rate=0.01)

    cbf.add("trip_123")
    print(f"Contains 'trip_123': {cbf.contains('trip_123')}")

    cbf.remove("trip_123")
    print(f"After removal, contains 'trip_123': {cbf.contains('trip_123')}")
