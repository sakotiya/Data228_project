#!/usr/bin/env python3
"""
Local Bloom Filter Deduplication Script
Processes 2025 streaming data using historical bloom filter
"""

import os
import gc
import time
import pickle
import glob

import numpy as np
import pandas as pd

from pybloom_live import BloomFilter


# -----------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------
BLOOM_PATH = "/Users/gouravdhama/Documents/bubu/big_data/git/bloom_all_years.pkl"  # Historical Bloom filter
STAGING_DIR = "/Users/gouravdhama/Documents/bubu/big_data/git/2025_files/"  # STREAMING DATA LOCATION
OUTPUT_DIR = "/Users/gouravdhama/Documents/bubu/big_data/git/dedup/2025_stream_dedup"  # Output location
UPDATED_BLOOM_PATH = "/Users/gouravdhama/Documents/bubu/big_data/git/bloom_all_years_plus2025.pkl"  # Updated filter

# Optional: subset of columns to keep/write; set to None to keep all
COLUMNS_TO_KEEP = None


def build_trip_id_vectorized(df: pd.DataFrame) -> pd.Series:
    """
    Build trip_id from pickup/dropoff datetime and PU/DO location IDs,
    consistent with the Bloom filter creation logic.
    """
    cols = df.columns
    pickup = next(
        (c for c in ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"] if c in cols),
        None,
    )
    dropoff = next(
        (
            c
            for c in [
                "tpep_dropoff_datetime",
                "lpep_dropoff_datetime",
                "dropoff_datetime",
                "dropOff_datetime",
            ]
            if c in cols
        ),
        None,
    )
    puloc = next((c for c in ["PULocationID", "PUlocationID"] if c in cols), None)
    doloc = next((c for c in ["DOLocationID", "DOlocationID"] if c in cols), None)

    parts = []
    if pickup:
        parts.append(df[pickup].astype(str))
    if dropoff:
        parts.append(df[dropoff].astype(str))
    if puloc:
        parts.append(df[puloc].astype(str))
    if doloc:
        parts.append(df[doloc].astype(str))

    return pd.Series(["_".join(x) for x in zip(*parts)], index=df.index)


def main():
    # -------------------------------------------------------------------
    # LOAD BLOOM FILTER FROM LOCAL FILE
    # -------------------------------------------------------------------
    print(f"📥 Loading Bloom filter from {BLOOM_PATH} ...")

    with open(BLOOM_PATH, "rb") as f:
        bloom = pickle.load(f)

    print("✅ Bloom filter loaded!")
    print(f"   Capacity: {bloom.capacity:,}")
    print(f"   Existing elements: {bloom.count:,}")
    print(f"   Fill ratio: {bloom.count / bloom.capacity:.2%}\n")

    # -------------------------------------------------------------------
    # DISCOVER 2025 FILES FROM LOCAL DIRECTORY
    # -------------------------------------------------------------------
    print(f"📂 Scanning {STAGING_DIR} ...")
    files = sorted(glob.glob(os.path.join(STAGING_DIR, "*.parquet")))
    print(f"✅ Found {len(files)} 2025 files.\n")

    if not files:
        print("⚠️ No parquet files found for 2025; exiting.")
        return

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # -------------------------------------------------------------------
    # STREAM THROUGH 2025 FILES (BATCHED) AND WRITE EACH BATCH LOCALLY
    # -------------------------------------------------------------------
    total_rows, unique_rows, dup_rows = 0, 0, 0
    batch_no = 0

    start = time.time()
    for fpath in files:
        batch_no += 1
        fname = os.path.basename(fpath)
        print(f"\n📦 [{batch_no}/{len(files)}] Processing {fname} ...")

        # Read this file into pandas (one file is one 'streaming batch')
        if COLUMNS_TO_KEEP is not None:
            df = pd.read_parquet(fpath, columns=COLUMNS_TO_KEEP)
        else:
            df = pd.read_parquet(fpath)

        if df.empty:
            print("   ⚠️ File empty; skipping.")
            continue

        trip_ids = build_trip_id_vectorized(df)

        # Membership test (True if already in Bloom → duplicate)
        seen_mask = np.fromiter((tid in bloom for tid in trip_ids), bool, len(trip_ids))
        keep_mask = ~seen_mask
        unique_df = df.loc[keep_mask].copy()

        # Add new unique trip IDs to Bloom filter (protect against capacity overflow)
        if bloom.count < bloom.capacity:
            for tid in trip_ids[keep_mask]:
                try:
                    bloom.add(tid)
                except IndexError:
                    print("   ⚠️ Bloom filter reached capacity; stopping further additions.")
                    break
        else:
            print("   ⚠️ Bloom filter already at capacity; skipping additions for this batch.")

        batch_rows = len(df)
        batch_unique = len(unique_df)
        batch_dup = int(seen_mask.sum())

        total_rows += batch_rows
        unique_rows += batch_unique
        dup_rows += batch_dup

        print(f"   Rows: {batch_rows:,} | Unique: {batch_unique:,} | Dups: {batch_dup:,}")

        # Write unique batch to local parquet using pandas
        if not unique_df.empty:
            batch_output = os.path.join(OUTPUT_DIR, f"batch_{batch_no:02d}.parquet")
            unique_df.to_parquet(batch_output, compression='snappy', index=False)
            print(f"   💾 Batch {batch_no} saved to {batch_output}")

        del df, trip_ids, unique_df, seen_mask, keep_mask
        gc.collect()

    print("\n" + "="*70)
    print("✅ STREAMING DEDUP COMPLETE")
    print("="*70)
    print(f"   Total processed: {total_rows:,}")
    print(f"   Unique kept:     {unique_rows:,}")
    print(f"   Duplicates:      {dup_rows:,} ({dup_rows/total_rows*100:.2f}%)")
    print(f"   Time elapsed:    {time.time()-start:.2f}s")
    print(f"   Output location: {OUTPUT_DIR}")

    # -------------------------------------------------------------------
    # SAVE UPDATED BLOOM FILTER (now includes 2025 trips)
    # -------------------------------------------------------------------
    print(f"\n💾 Saving updated Bloom filter to {UPDATED_BLOOM_PATH} ...")
    with open(UPDATED_BLOOM_PATH, "wb") as f:
        pickle.dump(bloom, f)
    print(f"✅ Updated Bloom filter saved to {UPDATED_BLOOM_PATH}")
    print(f"   New element count: {bloom.count:,}")
    print(f"   New fill ratio: {bloom.count / bloom.capacity:.2%}")
    print("\n🏁 Processing complete!")


if __name__ == "__main__":
    main()

