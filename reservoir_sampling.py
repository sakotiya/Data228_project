#!/usr/bin/env python3
"""
Reservoir Sampling Algorithm for Streaming Data
Maintains a fixed-size random sample from an unbounded stream
"""

import random
from typing import List, Dict, Any
import json

class ReservoirSampler:
    """
    Implements Reservoir Sampling (Algorithm R)
    Maintains k samples from n items with uniform probability k/n
    """

    def __init__(self, reservoir_size: int = 10000):
        """
        Initialize reservoir sampler

        Args:
            reservoir_size: Maximum number of samples to maintain
        """
        self.k = reservoir_size
        self.reservoir: List[Dict] = []
        self.n = 0  # Total items seen

        # Statistics tracking
        self.stats = {
            'total_processed': 0,
            'reservoir_size': reservoir_size,
            'fare_sum': 0.0,
            'distance_sum': 0.0,
            'trip_count': 0,
            'hourly_counts': {},
            'zone_counts': {}
        }

    def add(self, item: Dict[str, Any]) -> bool:
        """
        Add item to reservoir using Algorithm R

        Args:
            item: Data record to potentially add

        Returns:
            True if item was added to reservoir
        """
        self.n += 1
        self.stats['total_processed'] += 1

        # Update running statistics
        self._update_stats(item)

        # First k items always go into reservoir
        if len(self.reservoir) < self.k:
            self.reservoir.append(item)
            return True

        # For items k+1 onwards, random replacement
        # Item has probability k/n of being included
        j = random.randint(0, self.n - 1)

        if j < self.k:
            self.reservoir[j] = item
            return True

        return False

    def _update_stats(self, item: Dict[str, Any]):
        """Update running statistics from incoming item"""
        try:
            # Fare statistics
            if 'fare_amount' in item and item['fare_amount']:
                self.stats['fare_sum'] += float(item['fare_amount'])
                self.stats['trip_count'] += 1

            # Distance statistics
            if 'trip_distance' in item and item['trip_distance']:
                self.stats['distance_sum'] += float(item['trip_distance'])

            # Hourly pattern
            if 'tpep_pickup_datetime' in item:
                # Extract hour from datetime string
                dt_str = str(item['tpep_pickup_datetime'])
                if 'T' in dt_str:
                    hour = int(dt_str.split('T')[1].split(':')[0])
                elif ' ' in dt_str:
                    hour = int(dt_str.split(' ')[1].split(':')[0])
                else:
                    hour = 0

                self.stats['hourly_counts'][hour] = \
                    self.stats['hourly_counts'].get(hour, 0) + 1

            # Zone demand
            if 'PULocationID' in item:
                zone = item['PULocationID']
                self.stats['zone_counts'][zone] = \
                    self.stats['zone_counts'].get(zone, 0) + 1

        except Exception as e:
            # Skip problematic records
            pass

    def get_sample(self) -> List[Dict]:
        """Return current reservoir sample"""
        return self.reservoir.copy()

    def get_statistics(self) -> Dict[str, Any]:
        """
        Calculate and return statistics from reservoir

        Returns:
            Dictionary with computed statistics
        """
        result = {
            'total_records_processed': self.n,
            'reservoir_size': len(self.reservoir),
            'sampling_rate': len(self.reservoir) / self.n if self.n > 0 else 0
        }

        # Calculate averages from reservoir
        if len(self.reservoir) > 0:
            fares = [float(r.get('fare_amount', 0)) for r in self.reservoir
                    if r.get('fare_amount')]
            distances = [float(r.get('trip_distance', 0)) for r in self.reservoir
                        if r.get('trip_distance')]

            result['reservoir_stats'] = {
                'avg_fare': sum(fares) / len(fares) if fares else 0,
                'avg_distance': sum(distances) / len(distances) if distances else 0,
                'sample_count': len(self.reservoir)
            }

        # Calculate averages from stream (running stats)
        if self.stats['trip_count'] > 0:
            result['stream_stats'] = {
                'avg_fare': self.stats['fare_sum'] / self.stats['trip_count'],
                'avg_distance': self.stats['distance_sum'] / self.stats['trip_count'],
                'total_trips': self.stats['trip_count']
            }

        # Top 10 busiest hours
        if self.stats['hourly_counts']:
            sorted_hours = sorted(
                self.stats['hourly_counts'].items(),
                key=lambda x: x[1],
                reverse=True
            )
            result['top_hours'] = dict(sorted_hours[:10])

        # Top 10 busiest zones
        if self.stats['zone_counts']:
            sorted_zones = sorted(
                self.stats['zone_counts'].items(),
                key=lambda x: x[1],
                reverse=True
            )
            result['top_zones'] = dict(sorted_zones[:10])

        return result

    def reset(self):
        """Clear reservoir and statistics"""
        self.reservoir = []
        self.n = 0
        self.stats = {
            'total_processed': 0,
            'reservoir_size': self.k,
            'fare_sum': 0.0,
            'distance_sum': 0.0,
            'trip_count': 0,
            'hourly_counts': {},
            'zone_counts': {}
        }

    def save_reservoir(self, filepath: str):
        """Save reservoir to JSON file"""
        with open(filepath, 'w') as f:
            json.dump({
                'reservoir': self.reservoir,
                'metadata': {
                    'n': self.n,
                    'k': self.k,
                    'statistics': self.get_statistics()
                }
            }, f, indent=2, default=str)
        print(f"💾 Reservoir saved to {filepath}")

    def __repr__(self):
        return f"ReservoirSampler(k={self.k}, n={self.n}, fill={len(self.reservoir)}/{self.k})"


if __name__ == "__main__":
    # Test the reservoir sampler
    print("Testing Reservoir Sampler...")

    sampler = ReservoirSampler(reservoir_size=100)

    # Simulate 10,000 records
    for i in range(10000):
        item = {
            'trip_id': f'trip_{i}',
            'fare_amount': random.uniform(5, 100),
            'trip_distance': random.uniform(0.5, 20),
            'pickup_datetime': f'2025-01-01 {random.randint(0,23):02d}:00:00',
            'pickup_location_id': random.randint(1, 265)
        }
        sampler.add(item)

    print(f"\n{sampler}")
    print(f"\nStatistics:")
    stats = sampler.get_statistics()
    print(json.dumps(stats, indent=2, default=str))
