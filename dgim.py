#!/usr/bin/env python3
"""
DGIM Algorithm (Datar-Gionis-Indyk-Motwani)
Counts the number of 1s in a sliding window over a binary stream
"""

from collections import deque
from typing import List, Tuple
import json
import time


class Bucket:
    """Represents a bucket in DGIM algorithm"""

    def __init__(self, size: int, timestamp: int):
        """
        Initialize a bucket

        Args:
            size: Number of 1s in this bucket (power of 2)
            timestamp: Timestamp when this bucket ends
        """
        self.size = size
        self.timestamp = timestamp

    def __repr__(self):
        return f"Bucket(size={self.size}, ts={self.timestamp})"


class DGIM:
    """
    DGIM Algorithm for counting 1s in a sliding window

    Features:
    - Counts 1s in last N bits with O(log^2 N) space
    - Approximate count with max error of 50%
    - Maintains buckets of exponentially increasing sizes
    """

    def __init__(self, window_size: int = 10000):
        """
        Initialize DGIM algorithm

        Args:
            window_size: Size of the sliding window (N)
        """
        self.window_size = window_size
        self.buckets = []  # List of buckets, sorted by timestamp
        self.current_timestamp = 0
        self.total_bits_seen = 0
        self.total_ones_seen = 0

    def add_bit(self, bit: int):
        """
        Add a bit to the stream

        Args:
            bit: 0 or 1
        """
        self.total_bits_seen += 1
        self.current_timestamp += 1

        if bit == 1:
            self.total_ones_seen += 1
            # Create a new bucket of size 1
            self._create_bucket(1, self.current_timestamp)

        # Remove buckets that have fallen out of the window
        self._expire_old_buckets()

    def _create_bucket(self, size: int, timestamp: int):
        """
        Create a new bucket and merge if necessary

        Args:
            size: Bucket size
            timestamp: Bucket timestamp
        """
        # Add new bucket
        new_bucket = Bucket(size, timestamp)
        self.buckets.append(new_bucket)

        # Merge buckets of the same size if there are more than 2
        self._merge_buckets()

    def _merge_buckets(self):
        """
        Merge buckets to maintain DGIM invariant:
        At most 2 buckets of each size (power of 2)
        """
        # Group buckets by size
        size_counts = {}
        for bucket in self.buckets:
            size_counts[bucket.size] = size_counts.get(bucket.size, 0) + 1

        # Merge if more than 2 buckets of same size
        for size in sorted(size_counts.keys()):
            while size_counts.get(size, 0) > 2:
                # Find the two oldest buckets of this size
                buckets_of_size = [b for b in self.buckets if b.size == size]
                buckets_of_size.sort(key=lambda x: x.timestamp)

                if len(buckets_of_size) >= 2:
                    # Remove the two oldest
                    oldest1 = buckets_of_size[0]
                    oldest2 = buckets_of_size[1]

                    self.buckets.remove(oldest1)
                    self.buckets.remove(oldest2)

                    # Create a merged bucket with double size
                    merged_size = size * 2
                    # Use the newer timestamp
                    merged_timestamp = max(oldest1.timestamp, oldest2.timestamp)
                    merged_bucket = Bucket(merged_size, merged_timestamp)
                    self.buckets.append(merged_bucket)

                    # Update count
                    size_counts[size] -= 2
                    size_counts[merged_size] = size_counts.get(merged_size, 0) + 1
                else:
                    break

    def _expire_old_buckets(self):
        """Remove buckets that are outside the sliding window"""
        cutoff_timestamp = self.current_timestamp - self.window_size

        # Remove buckets with timestamp <= cutoff
        self.buckets = [b for b in self.buckets if b.timestamp > cutoff_timestamp]

    def count_ones(self) -> int:
        """
        Estimate the number of 1s in the current window

        Returns:
            Estimated count of 1s in the window
        """
        if not self.buckets:
            return 0

        # Sum all complete buckets
        total = sum(b.size for b in self.buckets[:-1])

        # Add half of the oldest bucket (approximation)
        if self.buckets:
            total += self.buckets[0].size // 2

        return total

    def get_statistics(self) -> dict:
        """
        Get comprehensive statistics

        Returns:
            Dictionary with statistics
        """
        estimated_ones = self.count_ones()

        # Calculate actual ones in window (for validation)
        # This would require storing the actual window, which defeats the purpose
        # So we approximate based on total

        return {
            'window_size': self.window_size,
            'current_timestamp': self.current_timestamp,
            'total_bits_seen': self.total_bits_seen,
            'total_ones_seen': self.total_ones_seen,
            'estimated_ones_in_window': estimated_ones,
            'num_buckets': len(self.buckets),
            'bucket_sizes': [b.size for b in sorted(self.buckets, key=lambda x: x.timestamp)],
            'memory_usage_buckets': len(self.buckets),
            'theoretical_max_buckets': int(2 * (self.window_size.bit_length() ** 2))
        }

    def save_stats(self, filepath: str):
        """Save statistics to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.get_statistics(), f, indent=2)
        print(f"💾 DGIM stats saved to {filepath}")

    def __repr__(self):
        return f"DGIM(window={self.window_size}, buckets={len(self.buckets)}, estimated_ones={self.count_ones()})"


if __name__ == "__main__":
    # Test the DGIM algorithm
    print("Testing DGIM Algorithm...")

    # Test 1: Simple alternating pattern
    print("\n--- Test 1: Alternating 0s and 1s ---")
    dgim = DGIM(window_size=100)

    for i in range(200):
        dgim.add_bit(i % 2)  # Alternating 0, 1, 0, 1, ...

    stats = dgim.get_statistics()
    print(f"Window size: {stats['window_size']}")
    print(f"Total bits seen: {stats['total_bits_seen']}")
    print(f"Estimated 1s in window: {stats['estimated_ones_in_window']}")
    print(f"Number of buckets: {stats['num_buckets']}")
    print(f"Expected: ~50 ones (half of 100)")

    # Test 2: All 1s
    print("\n--- Test 2: All 1s ---")
    dgim2 = DGIM(window_size=1000)

    for i in range(2000):
        dgim2.add_bit(1)

    stats2 = dgim2.get_statistics()
    print(f"Estimated 1s in window: {stats2['estimated_ones_in_window']}")
    print(f"Expected: ~1000 ones")
    print(f"Number of buckets: {stats2['num_buckets']}")

    # Test 3: Sparse 1s
    print("\n--- Test 3: Sparse 1s (10% density) ---")
    dgim3 = DGIM(window_size=1000)

    for i in range(5000):
        bit = 1 if i % 10 == 0 else 0  # 10% are 1s
        dgim3.add_bit(bit)

    stats3 = dgim3.get_statistics()
    print(f"Estimated 1s in window: {stats3['estimated_ones_in_window']}")
    print(f"Expected: ~100 ones (10% of 1000)")
    print(f"Number of buckets: {stats3['num_buckets']}")

    # Test 4: Real-world simulation - busy hours
    print("\n--- Test 4: Busy hours simulation ---")
    dgim4 = DGIM(window_size=24 * 60)  # 24 hours in minutes

    # Simulate taxi demand: 1 = busy, 0 = not busy
    for minute in range(24 * 60 * 2):  # 2 days
        hour = (minute // 60) % 24
        # Busy during rush hours (7-9 AM, 5-7 PM)
        is_busy = (7 <= hour <= 9) or (17 <= hour <= 19)
        dgim4.add_bit(1 if is_busy else 0)

    stats4 = dgim4.get_statistics()
    print(f"Estimated busy minutes in last 24h: {stats4['estimated_ones_in_window']}")
    print(f"Expected: ~300 minutes (5 hours of rush)")
    print(f"{dgim4}")
