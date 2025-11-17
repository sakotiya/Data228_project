#!/usr/bin/env python3
"""
Locality Sensitive Hashing (LSH) for Finding Similar Items
Uses MinHash for Jaccard similarity estimation
"""

import hashlib
import random
from collections import defaultdict
from typing import Set, List, Dict, Tuple
import json


class MinHash:
    """
    MinHash algorithm for estimating Jaccard similarity between sets
    """

    def __init__(self, num_hash_functions: int = 100):
        """
        Initialize MinHash

        Args:
            num_hash_functions: Number of hash functions (signature size)
        """
        self.num_hash_functions = num_hash_functions
        self.hash_seeds = [random.randint(0, 2**32 - 1) for _ in range(num_hash_functions)]

    def _hash(self, item: str, seed: int) -> int:
        """Hash function"""
        hash_input = f"{item}:{seed}".encode('utf-8')
        return int(hashlib.md5(hash_input).hexdigest(), 16)

    def compute_signature(self, item_set: Set[str]) -> List[int]:
        """
        Compute MinHash signature for a set

        Args:
            item_set: Set of items

        Returns:
            MinHash signature (list of hash values)
        """
        signature = []

        for seed in self.hash_seeds:
            # Min hash value for this hash function
            min_hash = float('inf')

            for item in item_set:
                hash_val = self._hash(item, seed)
                if hash_val < min_hash:
                    min_hash = hash_val

            signature.append(min_hash if min_hash != float('inf') else 0)

        return signature

    def estimate_similarity(self, sig1: List[int], sig2: List[int]) -> float:
        """
        Estimate Jaccard similarity between two signatures

        Args:
            sig1: First signature
            sig2: Second signature

        Returns:
            Estimated Jaccard similarity [0, 1]
        """
        if len(sig1) != len(sig2):
            raise ValueError("Signatures must have same length")

        matches = sum(1 for i in range(len(sig1)) if sig1[i] == sig2[i])
        return matches / len(sig1)


class LSH:
    """
    Locality Sensitive Hashing for finding similar items efficiently

    Uses MinHash signatures and banding technique to find candidate pairs
    """

    def __init__(self, num_hash_functions: int = 100, num_bands: int = 20):
        """
        Initialize LSH

        Args:
            num_hash_functions: Number of hash functions for MinHash
            num_bands: Number of bands for LSH (more bands = higher recall, lower precision)
        """
        self.num_hash_functions = num_hash_functions
        self.num_bands = num_bands
        self.rows_per_band = num_hash_functions // num_bands

        self.minhash = MinHash(num_hash_functions)

        # Storage
        self.signatures = {}  # item_id -> signature
        self.buckets = defaultdict(lambda: defaultdict(set))  # band -> bucket_hash -> set of item_ids

        # Statistics
        self.num_items = 0
        self.num_comparisons = 0
        self.candidate_pairs = set()

    def add_item(self, item_id: str, item_set: Set[str]):
        """
        Add an item to the LSH index

        Args:
            item_id: Unique identifier for the item
            item_set: Set of features/elements for this item
        """
        # Compute MinHash signature
        signature = self.minhash.compute_signature(item_set)
        self.signatures[item_id] = signature

        # Add to LSH buckets (banding technique)
        for band_idx in range(self.num_bands):
            start = band_idx * self.rows_per_band
            end = start + self.rows_per_band

            # Get band portion of signature
            band_signature = tuple(signature[start:end])

            # Hash the band to get bucket
            bucket_hash = hash(band_signature)

            # Add item to this bucket
            self.buckets[band_idx][bucket_hash].add(item_id)

        self.num_items += 1

    def find_similar_items(self, item_id: str, similarity_threshold: float = 0.5) -> List[Tuple[str, float]]:
        """
        Find items similar to the given item

        Args:
            item_id: Item to find similar items for
            similarity_threshold: Minimum similarity score [0, 1]

        Returns:
            List of (similar_item_id, similarity_score) tuples
        """
        if item_id not in self.signatures:
            return []

        # Find candidate pairs using LSH buckets
        candidates = set()

        signature = self.signatures[item_id]

        # Check all bands
        for band_idx in range(self.num_bands):
            start = band_idx * self.rows_per_band
            end = start + self.rows_per_band

            band_signature = tuple(signature[start:end])
            bucket_hash = hash(band_signature)

            # Get all items in same bucket
            if bucket_hash in self.buckets[band_idx]:
                candidates.update(self.buckets[band_idx][bucket_hash])

        # Remove self
        candidates.discard(item_id)

        # Compute actual similarities for candidates
        similar_items = []

        for candidate_id in candidates:
            self.num_comparisons += 1
            candidate_sig = self.signatures[candidate_id]

            similarity = self.minhash.estimate_similarity(signature, candidate_sig)

            if similarity >= similarity_threshold:
                similar_items.append((candidate_id, similarity))

        # Sort by similarity (descending)
        similar_items.sort(key=lambda x: x[1], reverse=True)

        return similar_items

    def find_all_similar_pairs(self, similarity_threshold: float = 0.5) -> List[Tuple[str, str, float]]:
        """
        Find all pairs of similar items

        Args:
            similarity_threshold: Minimum similarity

        Returns:
            List of (item1_id, item2_id, similarity) tuples
        """
        similar_pairs = []
        checked_pairs = set()

        for item_id in self.signatures.keys():
            similar_items = self.find_similar_items(item_id, similarity_threshold)

            for similar_id, similarity in similar_items:
                # Avoid duplicate pairs (A,B) and (B,A)
                pair = tuple(sorted([item_id, similar_id]))

                if pair not in checked_pairs:
                    checked_pairs.add(pair)
                    similar_pairs.append((item_id, similar_id, similarity))

        return similar_pairs

    def get_statistics(self) -> dict:
        """Get LSH statistics"""
        total_buckets = sum(len(buckets) for buckets in self.buckets.values())

        return {
            'num_items': self.num_items,
            'num_hash_functions': self.num_hash_functions,
            'num_bands': self.num_bands,
            'rows_per_band': self.rows_per_band,
            'total_buckets': total_buckets,
            'num_comparisons': self.num_comparisons,
            'candidate_pairs': len(self.candidate_pairs),
            'average_bucket_size': total_buckets / self.num_bands if self.num_bands > 0 else 0
        }

    def save_stats(self, filepath: str):
        """Save statistics to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.get_statistics(), f, indent=2)
        print(f"💾 LSH stats saved to {filepath}")

    def __repr__(self):
        return f"LSH(items={self.num_items}, bands={self.num_bands}, signatures={self.num_hash_functions})"


if __name__ == "__main__":
    # Test LSH
    print("Testing Locality Sensitive Hashing...")

    lsh = LSH(num_hash_functions=100, num_bands=20)

    # Test 1: Similar taxi routes
    print("\n--- Test 1: Finding similar taxi routes ---")

    # Define some routes as sets of zones visited
    routes = {
        'route_1': {161, 162, 163, 164, 237},  # Midtown route
        'route_2': {161, 162, 163, 237},       # Similar midtown route
        'route_3': {138, 139, 140, 132},       # Airport route
        'route_4': {138, 139, 132},            # Similar airport route
        'route_5': {79, 80, 81, 82, 83},       # East Village route
        'route_6': {161, 237, 236, 170}        # Different midtown route
    }

    # Add routes to LSH
    for route_id, zones in routes.items():
        lsh.add_item(route_id, zones)

    # Find similar routes
    print("\nSimilar routes to route_1 (Midtown):")
    similar = lsh.find_similar_items('route_1', similarity_threshold=0.3)
    for sim_id, similarity in similar:
        print(f"  {sim_id}: {similarity:.2f} similarity")

    print("\nSimilar routes to route_3 (Airport):")
    similar = lsh.find_similar_items('route_3', similarity_threshold=0.3)
    for sim_id, similarity in similar:
        print(f"  {sim_id}: {similarity:.2f} similarity")

    # Find all similar pairs
    print("\nAll similar pairs (threshold=0.4):")
    all_pairs = lsh.find_all_similar_pairs(similarity_threshold=0.4)
    for item1, item2, sim in all_pairs:
        print(f"  {item1} <-> {item2}: {sim:.2f}")

    # Test 2: Similar passenger patterns
    print("\n--- Test 2: Similar passenger behavior patterns ---")

    lsh2 = LSH(num_hash_functions=100, num_bands=25)

    # Passenger patterns (set of zones they visit frequently)
    passengers = {
        'passenger_A': {'zone_161', 'zone_237', 'zone_170', 'zone_162'},  # Midtown worker
        'passenger_B': {'zone_161', 'zone_237', 'zone_170'},              # Similar pattern
        'passenger_C': {'zone_138', 'zone_132'},                          # Airport commuter
        'passenger_D': {'zone_79', 'zone_80', 'zone_81', 'zone_100'},    # East side resident
        'passenger_E': {'zone_161', 'zone_162', 'zone_237', 'zone_236'}  # Another midtown worker
    }

    for passenger_id, zones in passengers.items():
        lsh2.add_item(passenger_id, zones)

    print("\nSimilar passengers to passenger_A:")
    similar_passengers = lsh2.find_similar_items('passenger_A', similarity_threshold=0.5)
    for pass_id, similarity in similar_passengers:
        print(f"  {pass_id}: {similarity:.2f} similarity")

    # Statistics
    stats = lsh2.get_statistics()
    print(f"\nLSH Statistics:")
    print(f"  Items indexed: {stats['num_items']}")
    print(f"  Total comparisons: {stats['num_comparisons']}")
    print(f"  Buckets used: {stats['total_buckets']}")

    print(f"\n{lsh2}")
