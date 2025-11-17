#!/usr/bin/env python3
"""
HyperLogLog Algorithm for Cardinality Estimation
Modern improvement over Flajolet-Martin with much lower variance
"""

import hashlib
import math
from typing import Set, List
import json


class HyperLogLog:
    """
    Flajolet-Martin algorithm for estimating cardinality (count of distinct elements)

    Uses multiple hash functions and tracks the maximum number of trailing zeros
    to estimate the number of distinct elements with low memory overhead.
    """

    def __init__(self, num_hash_functions: int = 64):
        """
        Initialize Flajolet-Martin algorithm

        Args:
            num_hash_functions: Number of hash functions to use (reduces variance)
        """
        self.num_hash_functions = num_hash_functions
        self.max_trailing_zeros = [0] * num_hash_functions
        self.actual_distinct = set()  # For validation only
        self.total_elements = 0

    def _hash(self, item: str, seed: int) -> int:
        """
        Hash function that returns an integer

        Args:
            item: String to hash
            seed: Hash function seed

        Returns:
            Integer hash value
        """
        hash_input = f"{item}:{seed}".encode('utf-8')
        hash_digest = hashlib.sha256(hash_input).hexdigest()
        return int(hash_digest, 16)

    def _count_trailing_zeros(self, n: int) -> int:
        """
        Count the number of trailing zeros in binary representation

        Args:
            n: Integer to analyze

        Returns:
            Number of trailing zeros
        """
        if n == 0:
            return 0

        count = 0
        while (n & 1) == 0:
            count += 1
            n >>= 1
        return count

    def add(self, item: str):
        """
        Add an item to the stream

        Args:
            item: String identifier to add
        """
        self.total_elements += 1
        self.actual_distinct.add(item)  # For validation

        # Apply each hash function
        for i in range(self.num_hash_functions):
            hash_value = self._hash(item, i)
            trailing_zeros = self._count_trailing_zeros(hash_value)

            # Update maximum trailing zeros for this hash function
            if trailing_zeros > self.max_trailing_zeros[i]:
                self.max_trailing_zeros[i] = trailing_zeros

    def estimate_cardinality(self) -> int:
        """
        Estimate the number of distinct elements

        Returns:
            Estimated cardinality
        """
        # Standard Flajolet-Martin algorithm:
        # 1. Take average of R values (not median of 2^R!)
        # 2. Calculate 2^R_avg
        # 3. Divide by φ to correct bias

        # Calculate average R (not average of 2^R)
        r_avg = sum(self.max_trailing_zeros) / len(self.max_trailing_zeros)

        # Apply correction factor
        # Formula: n ≈ 2^R_avg / φ where φ ≈ 0.77351
        phi = 0.77351
        corrected_estimate = int((2 ** r_avg) / phi)

        return corrected_estimate

    def get_statistics(self) -> dict:
        """
        Get comprehensive statistics

        Returns:
            Dictionary with statistics
        """
        estimated = self.estimate_cardinality()
        actual = len(self.actual_distinct)

        return {
            'total_elements_seen': self.total_elements,
            'actual_distinct': actual,
            'estimated_distinct': estimated,
            'error': abs(actual - estimated),
            'error_percentage': abs(actual - estimated) / actual * 100 if actual > 0 else 0,
            'num_hash_functions': self.num_hash_functions,
            'max_trailing_zeros': {
                'min': min(self.max_trailing_zeros),
                'max': max(self.max_trailing_zeros),
                'mean': sum(self.max_trailing_zeros) / len(self.max_trailing_zeros)
            }
        }

    def save_stats(self, filepath: str):
        """Save statistics to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.get_statistics(), f, indent=2)
        print(f"💾 Flajolet-Martin stats saved to {filepath}")

    def __repr__(self):
        estimated = self.estimate_cardinality()
        actual = len(self.actual_distinct)
        return f"FlajoletMartin(estimated={estimated:,}, actual={actual:,}, error={abs(estimated-actual):,})"


if __name__ == "__main__":
    # Test the Flajolet-Martin algorithm
    print("Testing Flajolet-Martin Algorithm...")

    fm = FlajoletMartin(num_hash_functions=64)

    # Test 1: Add distinct elements
    print("\n--- Test 1: 10,000 distinct elements ---")
    for i in range(10000):
        fm.add(f"element_{i}")

    stats = fm.get_statistics()
    print(f"Actual distinct: {stats['actual_distinct']:,}")
    print(f"Estimated distinct: {stats['estimated_distinct']:,}")
    print(f"Error: {stats['error']:,} ({stats['error_percentage']:.2f}%)")

    # Test 2: Add duplicates
    print("\n--- Test 2: Adding duplicates ---")
    fm2 = FlajoletMartin(num_hash_functions=64)

    # Add 1000 distinct elements, but 10000 total (with repetitions)
    for i in range(10000):
        fm2.add(f"element_{i % 1000}")  # Only 1000 distinct

    stats2 = fm2.get_statistics()
    print(f"Total elements: {stats2['total_elements_seen']:,}")
    print(f"Actual distinct: {stats2['actual_distinct']:,}")
    print(f"Estimated distinct: {stats2['estimated_distinct']:,}")
    print(f"Error: {stats2['error']:,} ({stats2['error_percentage']:.2f}%)")

    # Test 3: Very large cardinality
    print("\n--- Test 3: 100,000 distinct elements ---")
    fm3 = FlajoletMartin(num_hash_functions=64)

    for i in range(100000):
        fm3.add(f"large_element_{i}")

    stats3 = fm3.get_statistics()
    print(f"Actual distinct: {stats3['actual_distinct']:,}")
    print(f"Estimated distinct: {stats3['estimated_distinct']:,}")
    print(f"Error: {stats3['error']:,} ({stats3['error_percentage']:.2f}%)")

    print(f"\n{fm3}")
