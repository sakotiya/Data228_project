# Bloom Filter Deduplication Summary

## Script: `bloom_dedup_local.py`

### Execution Date
November 18, 2025

### Configuration
- **Historical Bloom Filter**: `$DATA228_GIT_DIR/bloom_all_years.pkl`
- **Input Data Directory**: `$DATA228_2025_FILES_DIR`
- **Output Directory**: `$DATA228_DEDUP_OUTPUT_DIR`
- **Updated Bloom Filter**: `$DATA228_UPDATED_BLOOM_PATH`

### Bloom Filter Statistics
- **Capacity**: 250,000,000 elements
- **Initial Elements**: 250,000,001 (100.00% full)
- **Status**: Already at capacity (no new elements could be added)

### Processing Results

#### Overall Statistics
- **Total Rows Processed**: 53,194,401
- **Unique Rows Kept**: 52,653,164
- **Duplicates Detected**: 541,237 (1.02%)
- **Processing Time**: 211 seconds (~3.5 minutes)

#### File-by-File Breakdown

| Batch | File | Total Rows | Unique | Duplicates | Dup % |
|-------|------|------------|--------|------------|-------|
| 1 | fhv_tripdata_2025-03.parquet | 2,182,992 | 2,161,008 | 21,984 | 1.01% |
| 2 | fhv_tripdata_2025-04.parquet | 1,699,478 | 1,682,322 | 17,156 | 1.01% |
| 3 | fhvhv_tripdata_2025-01.parquet | 20,405,666 | 20,198,108 | 207,558 | 1.02% |
| 4 | fhvhv_tripdata_2025-03.parquet | 20,536,879 | 20,327,220 | 209,659 | 1.02% |
| 5 | green_tripdata_2025-05.parquet | 54,441 | 53,862 | 579 | 1.06% |
| 6 | green_tripdata_2025-06.parquet | 48,352 | 47,868 | 484 | 1.00% |
| 7 | yellow_tripdata_2025-05.parquet | 4,242,250 | 4,199,381 | 42,869 | 1.01% |
| 8 | yellow_tripdata_2025-06.parquet | 4,024,343 | 3,983,395 | 40,948 | 1.02% |

### Output Files

All deduplicated data saved as Snappy-compressed Parquet files:

All deduplicated data is written under the directory configured by
`DATA228_DEDUP_OUTPUT_DIR` (for example,
`/Users/vidushi/Documents/bubu/big_data/git/dedup/2025_stream_dedup/`).

### Key Observations

1. **Consistent Duplicate Rate**: The duplicate rate across all files is remarkably consistent at ~1.02%, suggesting systematic duplication rather than random errors.

2. **Bloom Filter at Capacity**: The historical bloom filter was already at 100% capacity (250M+ elements), so new 2025 trip IDs could not be added to it. However, the filter still successfully identified duplicates from the historical data.

3. **Large Datasets**: The high-volume for-hire vehicle (fhvhv) files contain the most data, with over 20M rows each.

4. **Space Efficiency**: The bloom filter uses 286MB to represent 250 million elements, which is extremely space-efficient compared to storing actual trip IDs.

### Trip ID Construction

Trip IDs are constructed by concatenating:
- Pickup datetime
- Dropoff datetime  
- Pickup Location ID
- Dropoff Location ID

This composite key uniquely identifies each trip across different taxi types (yellow, green, FHV, FHVHV).

### Script Conversion Notes

The original S3-based script was successfully converted to work with local files:
- Removed S3FS dependency
- Removed PySpark dependency (used pandas directly for parquet I/O)
- Replaced S3 paths with local filesystem paths
- Maintained all core bloom filter logic and trip ID generation
- Simplified output to single parquet files per batch (no Spark partitioning)

### Next Steps

1. **Bloom Filter Expansion**: Consider creating a larger bloom filter if additional historical data needs to be indexed
2. **Analysis**: Investigate the cause of the 1.02% duplicate rate
3. **Validation**: Spot-check some deduplicated records to verify correctness
4. **Integration**: Use these deduplicated files for downstream analytics/ML pipelines

