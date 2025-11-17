#!/usr/bin/env python3
"""
HyperLogLog Algorithm for Cardinality Estimation
Modern, accurate algorithm with ~1.04/sqrt(m) standard error
"""

import hashlib
import math
from typing import Set
import json


class HyperLogLog:
    """
    HyperLogLog algorithm for cardinality estimation

    Features:
    - Very accurate: standard error ≈ 1.04/sqrt(m)
    - Memory efficient: uses m registers of log2(log2(n)) bits each
    - Uses harmonic mean for combining estimates
    """

    def __init__(self, precision: int = 14):
        """
        Initialize HyperLogLog

        Args:
            precision: Number of bits for addressing (4-16)
                      m = 2^precision registers
                      Higher precision = better accuracy but more memory
                      Default 14 → 16384 registers → ~1.625% standard error
        """
        if precision < 4 or precision > 16:
            raise ValueError("Precision must be between 4 and 16")

        self.precision = precision
        self.m = 1 << precision  # 2^precision registers
        self.registers = [0] * self.m

        # Alpha correction constant for bias correction
        if self.m >= 128:
            self.alpha = 0.7213 / (1 + 1.079 / self.m)
        elif self.m >= 64:
            self.alpha = 0.709
        elif self.m >= 32:
            self.alpha = 0.697
        elif self.m >= 16:
            self.alpha = 0.673
        else:
            self.alpha = 0.5

        # For validation
        self.actual_distinct = set()
        self.total_elements = 0

    def _hash(self, item: str) -> int:
        """
        Hash function that returns a 64-bit integer

        Args:
            item: String to hash

        Returns:
            64-bit integer hash
        """
        hash_bytes = hashlib.sha256(item.encode('utf-8')).digest()
        # Take first 8 bytes for 64-bit hash
        return int.from_bytes(hash_bytes[:8], byteorder='big')

    def _leading_zeros(self, bits: int, max_width: int = 64) -> int:
        """
        Count leading zeros in binary representation

        Args:
            bits: Integer to analyze
            max_width: Maximum bit width (default 64)

        Returns:
            Number of leading zeros + 1 (ρ value in HLL)
        """
        if bits == 0:
            return max_width - self.precision + 1

        # Count leading zeros
        leading_zeros = 0
        for i in range(max_width - self.precision - 1, -1, -1):
            if bits & (1 << i):
                break
            leading_zeros += 1

        return leading_zeros + 1

    def add(self, item: str):
        """
        Add an item to HyperLogLog

        Args:
            item: String identifier
        """
        self.total_elements += 1
        self.actual_distinct.add(item)  # For validation

        # Get hash value
        hash_value = self._hash(item)

        # Use first 'precision' bits for register index
        register_index = hash_value & ((1 << self.precision) - 1)

        # Use remaining bits to count leading zeros
        remaining_bits = hash_value >> self.precision

        # Count leading zeros + 1 (ρ value)
        rho = self._leading_zeros(remaining_bits, 64 - self.precision)

        # Update register with maximum ρ
        if rho > self.registers[register_index]:
            self.registers[register_index] = rho

    def count(self) -> int:
        """
        Estimate the cardinality

        Returns:
            Estimated number of distinct elements
        """
        # Calculate raw estimate using harmonic mean
        raw_estimate = self.alpha * (self.m ** 2) / sum(2 ** (-x) for x in self.registers)

        # Apply bias corrections for different ranges
        if raw_estimate <= 2.5 * self.m:
            # Small range correction
            zeros = self.registers.count(0)
            if zeros != 0:
                return int(self.m * math.log(self.m / zeros))

        if raw_estimate <= (1.0/30.0) * (1 << 32):
            # No correction
            return int(raw_estimate)
        else:
            # Large range correction
            return int(-1 * (1 << 32) * math.log(1 - raw_estimate / (1 << 32)))

    def merge(self, other: 'HyperLogLog'):
        """
        Merge another HyperLogLog into this one

        Args:
            other: Another HyperLogLog instance
        """
        if self.precision != other.precision:
            raise ValueError("Cannot merge HyperLogLogs with different precision")

        # Take maximum of each register
        for i in range(self.m):
            self.registers[i] = max(self.registers[i], other.registers[i])

    def get_statistics(self) -> dict:
        """
        Get comprehensive statistics

        Returns:
            Dictionary with statistics
        """
        estimated = self.count()
        actual = len(self.actual_distinct)

        # Theoretical standard error
        theoretical_error = 1.04 / math.sqrt(self.m)

        return {
            'precision': self.precision,
            'num_registers': self.m,
            'total_elements_seen': self.total_elements,
            'actual_distinct': actual,
            'estimated_distinct': estimated,
            'error': abs(actual - estimated),
            'error_percentage': abs(actual - estimated) / actual * 100 if actual > 0 else 0,
            'theoretical_std_error_percentage': theoretical_error * 100,
            'registers_used': sum(1 for r in self.registers if r > 0),
            'registers_used_percentage': sum(1 for r in self.registers if r > 0) / self.m * 100,
            'memory_bytes': self.m  # Approximate, each register is ~1 byte
        }

    def save_stats(self, filepath: str):
        """Save statistics to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.get_statistics(), f, indent=2)
        print(f"💾 HyperLogLog stats saved to {filepath}")

    def __repr__(self):
        estimated = self.count()
        actual = len(self.actual_distinct)
        return f"HyperLogLog(precision={self.precision}, estimated={estimated:,}, actual={actual:,}, error={abs(estimated-actual):,})"


if __name__ == "__main__":
    # Test HyperLogLog
    print("Testing HyperLogLog Algorithm...")

    # Test 1: 10,000 distinct elements
    print("\n--- Test 1: 10,000 distinct elements ---")
    hll1 = HyperLogLog(precision=14)

    for i in range(10000):
        hll1.add(f"element_{i}")

    stats1 = hll1.get_statistics()
    print(f"Actual distinct: {stats1['actual_distinct']:,}")
    print(f"Estimated distinct: {stats1['estimated_distinct']:,}")
    print(f"Error: {stats1['error']:,} ({stats1['error_percentage']:.2f}%)")
    print(f"Theoretical std error: ±{stats1['theoretical_std_error_percentage']:.2f}%")

    # Test 2: Adding duplicates
    print("\n--- Test 2: 1,000 distinct with duplicates ---")
    hll2 = HyperLogLog(precision=14)

    # Add 1000 distinct elements, but 10000 total (with repetitions)
    for i in range(10000):
        hll2.add(f"element_{i % 1000}")  # Only 1000 distinct

    stats2 = hll2.get_statistics()
    print(f"Total elements: {stats2['total_elements_seen']:,}")
    print(f"Actual distinct: {stats2['actual_distinct']:,}")
    print(f"Estimated distinct: {stats2['estimated_distinct']:,}")
    print(f"Error: {stats2['error']:,} ({stats2['error_percentage']:.2f}%)")

    # Test 3: Very large cardinality
    print("\n--- Test 3: 100,000 distinct elements ---")
    hll3 = HyperLogLog(precision=14)

    for i in range(100000):
        hll3.add(f"large_element_{i}")

    stats3 = hll3.get_statistics()
    print(f"Actual distinct: {stats3['actual_distinct']:,}")
    print(f"Estimated distinct: {stats3['estimated_distinct']:,}")
    print(f"Error: {stats3['error']:,} ({stats3['error_percentage']:.2f}%)")

    # Test 4: Merge test
    print("\n--- Test 4: Merging two HyperLogLogs ---")
    hll4a = HyperLogLog(precision=12)
    hll4b = HyperLogLog(precision=12)

    # Add different elements to each
    for i in range(5000):
        hll4a.add(f"set_a_{i}")
    for i in range(3000, 8000):  # Overlapping
        hll4b.add(f"set_a_{i}")

    print(f"HLL A estimate: {hll4a.count():,}")
    print(f"HLL B estimate: {hll4b.count():,}")

    # Merge
    hll4a.merge(hll4b)
    print(f"Merged estimate: {hll4a.count():,}")
    print(f"Expected: ~8,000 (union of 0-4999 and 3000-7999)")

    # Test 5: Different precisions
    print("\n--- Test 5: Comparing different precisions ---")
    test_data = [f"test_{i}" for i in range(50000)]

    for precision in [10, 12, 14, 16]:
        hll = HyperLogLog(precision=precision)
        for item in test_data:
            hll.add(item)

        stats = hll.get_statistics()
        print(f"Precision {precision} (m={stats['num_registers']:,}): "
              f"estimate={stats['estimated_distinct']:,}, "
              f"error={stats['error_percentage']:.2f}%, "
              f"memory={stats['memory_bytes']:,} bytes")

    print(f"\n{hll3}")
