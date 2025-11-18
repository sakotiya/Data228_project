# Ground Truth Spark Jobs

Independent Spark jobs for computing ground truth metrics for streaming algorithm validation.

## Jobs Overview

| Job | Algorithm | Output Path | Memory Intensive |
|-----|-----------|-------------|------------------|
| `ground_truth_basic_stats.py` | Reservoir Sampling | `/basic_stats/` | Low |
| `ground_truth_cardinality.py` | HyperLogLog | `/cardinality/` | Low |
| `ground_truth_frequency_moments.py` | Frequency Moments | `/frequency_moments/` | Medium |
| `ground_truth_dgim.py` | DGIM | `/dgim_metrics/` | Low |
| `ground_truth_lsh.py` | LSH | `/lsh_routes/` | Medium |
| `ground_truth_duplicates.py` | Bloom Filter | `/duplicates/` | **High** |
| `ground_truth_hourly.py` | Hourly Breakdown | `/hourly_stats/` | **High** |

## Running Locally

Set AWS credentials:
```bash
export AWS_PROFILE=data228
export AWS_REGION=us-east-1
```

Run individual jobs:
```bash
# Quick jobs (run locally)
spark-submit --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.683 ground_truth_basic_stats.py

spark-submit --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.683 ground_truth_cardinality.py

spark-submit --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.683 ground_truth_frequency_moments.py

spark-submit --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.683 ground_truth_dgim.py

spark-submit --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.683 ground_truth_lsh.py
```
spark-submit --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.683 ground_truth_duplicates.py
## Running on EMR (Recommended for Heavy Jobs)

Upload scripts:
```bash
scp ground_truth_*.py hadoop@<emr-master-ip>:~/
```

Run on EMR:
```bash
ssh hadoop@<emr-master-ip>

# Heavy jobs - run on EMR
spark-submit ground_truth_duplicates.py
spark-submit ground_truth_hourly.py
```

## Output Structure

All jobs save to: `s3a://data228-bigdata-nyc/ground-truth-enhanced/`

```
ground-truth-enhanced/
├── basic_stats/
│   └── part-00000 (JSON)
├── cardinality/
│   └── part-00000 (JSON)
├── frequency_moments/
│   └── part-00000 (JSON)
├── dgim_metrics/
│   └── part-00000 (JSON)
├── lsh_routes/
│   └── part-00000 (JSON)
├── duplicates/          # Run on EMR
│   └── part-00000 (JSON)
└── hourly_stats/        # Run on EMR
    └── part-00000 (JSON)
```

## Quick Start

**Run all light jobs locally:**
```bash
cd spark-jobs
export AWS_PROFILE=data228
export AWS_REGION=us-east-1

# Run each in sequence (or in parallel in separate terminals)
for job in basic_stats cardinality frequency_moments dgim lsh; do
  echo "Running $job..."
  spark-submit --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.683 ground_truth_${job}.py
done
```

**Then run heavy jobs on EMR:**
```bash
# On EMR
spark-submit ground_truth_duplicates.py
spark-submit ground_truth_hourly.py
```

## Debugging

Each job includes debug output to help diagnose data issues:
- **Cardinality**: Shows non-null vendor counts, column names
- **DGIM**: Shows non-null fare counts, max fare
- **All**: Shows record counts per taxi type

Check output logs for debug info with `ℹ️` prefix.
