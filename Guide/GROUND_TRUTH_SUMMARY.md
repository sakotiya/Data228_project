## Ground Truth Layer – Summary

This document summarizes the **ground truth layer** used to evaluate all streaming algorithms in the NYC Taxi Big Data project.

Ground truth here means: **batch, high‑fidelity metrics computed over the full cleaned historical dataset**, stored in S3 and local JSONs, which we treat as the **gold standard** when measuring the accuracy of Bloom, DGIM, LSH and other streaming approximations.

---

## 1. Components of the Ground Truth Layer

The ground truth layer is built from three main pieces:

- **Spark jobs in `spark-jobs_groundTruth/`**  
  Independent Spark applications that compute exact / near‑exact metrics over large S3 datasets.

- **Pandas baseline notebook `validation/ground_truth_baseline.ipynb`**  
  A local, notebook‑friendly implementation of a comprehensive baseline job, used for exploratory analysis and verification.

- **Validation artifacts in `validation/validation_results.json`**  
  JSON summaries that compare ground truth with streaming / algorithm outputs.

Together, these form the reference against which we evaluate:

- Duplicate counts (Bloom filter)
- Sliding‑window counts (DGIM)
- Similarity search quality (LSH)
- Aggregate statistics (hourly/daily, fare distributions, spatial demand)

---

## 2. Spark Ground Truth Jobs (Batch on S3)

The Spark jobs are documented in detail in `spark-jobs_groundTruth/GROUND_TRUTH_JOBS.md`. At a high level:

- **`ground_truth_basic_stats.py`** – Baseline descriptive statistics per taxi type:
  - Total trips, basic distributions, sanity checks.

- **`ground_truth_cardinality.py`** – Distinct counts using HyperLogLog:
  - Reference for any streaming cardinality estimators.

- **`ground_truth_frequency_moments.py`** – Frequency moments (e.g., \(F_0, F_1, F_2\)):
  - Used to understand heavy hitters and tail behavior.

- **`ground_truth_dgim.py`** – Exact windowed counts for DGIM evaluation:
  - Provides the “true” counts in time windows that DGIM tries to approximate.

- **`ground_truth_lsh.py`** – Route / behavior similarity ground truth:
  - Computes exact neighbors / similarities, used as a benchmark for LSH.

- **`ground_truth_duplicates.py`** (heavy, EMR) – Exact duplicate detection:
  - Full‑dataset duplicate counts and IDs; benchmark for Bloom‑filter streaming dedup.

- **`ground_truth_hourly.py`** (heavy, EMR) – Hourly aggregates:
  - Volume and key KPIs per hour; used to verify that streaming pipelines match batch behavior over time.

**Output location (S3)**  
All Spark jobs write under:

- `s3a://data228-bigdata-nyc/ground-truth-enhanced/`
  - `basic_stats/`
  - `cardinality/`
  - `frequency_moments/`
  - `dgim_metrics/`
  - `lsh_routes/`
  - `duplicates/`
  - `hourly_stats/`

These JSON outputs are small, easy to inspect, and are treated as the **canonical answers** for their respective metrics.

---

## 3. Ground Truth Baseline Notebook (Local, Pandas)

File: `validation/ground_truth_baseline.ipynb`

This notebook is a **pandas version** of the original Spark `ground_truth_baseline.py` job. It is designed for:

- Running on a developer laptop.
- Inspecting results interactively.
- Debugging schemas and metrics before running the heavy Spark/EMR jobs.

### 3.1 Inputs

- Cleaned parquet files in a local directory (originally:
  - `CLEANED_DATA_DIR = "/Users/.../cleaned_data"` – configurable per developer.

The notebook automatically detects:

- Pickup & dropoff timestamps
- Fare amounts
- Trip distances
- `PULocationID` / `DOLocationID`
- Passenger counts (when present)

### 3.2 Metrics Computed

For each cleaned parquet file:

- **Temporal patterns**
  - Hourly and daily aggregates (trip counts).

- **Fare distribution**
  - Mean, median, standard deviation.
  - Percentiles and fare buckets (e.g., \$0–10, \$10–20, …).

- **Spatial analytics**
  - Top pickup zones and dropoff zones.
  - Top origin–destination (OD) pairs.

- **Trip duration statistics**
  - Duration in minutes; distribution summary.

- **Overall summary**
  - Total trips.
  - Total revenue (where fares exist).
  - Total distance.
  - Average fare and average distance.
  - Number of unique zones.

### 3.3 Outputs

The notebook writes JSONs to a local `ground_truth_results/` directory:

- Per‑file summary JSON:
  - `ground_truth_results/summary/*_ground_truth.json`

- Aggregations by taxi type (`fhv`, `fhvhv`, `green`, `yellow`):
  - `ground_truth_results/hourly_stats/{type}_aggregated.json`
  - `ground_truth_results/summary/{type}_overall_summary.json`

- Master index:
  - `ground_truth_results/MASTER_INDEX.json` describing:
    - Which files were processed.
    - Which metrics each file has.
    - Where each JSON is stored.

### 3.4 Example Baseline Numbers (From Last Run)

- **FHV (24 files)**:
  - `~49.6M` trips (no fare fields in schema → no revenue stats).

- **FHVHV (22 files)**:
  - `~441.8M` trips.

- **GREEN (26 files)**:
  - `~7.93M` trips.  
  - Total revenue ≈ **\$107M**, average fare ≈ **\$17.19**.

- **YELLOW (28 files)**:
  - `~161.3M` trips.  
  - Total revenue ≈ **\$2.19B**, average fare ≈ **\$18.19**.

These values serve as a **human‑readable snapshot** of the global ground truth for the NYC taxi system.

---

## 4. Validation Results JSON

File: `validation/validation_results.json`

This JSON file aggregates **comparisons between ground truth and streaming / algorithm outputs**. Typical content includes:

- Metrics per algorithm (Bloom, DGIM, LSH, etc.):
  - Ground truth value.
  - Streaming estimate.
  - Absolute error and relative error.

- Per‑file or per‑window breakdowns:
  - For DGIM: true vs estimated counts over windows.
  - For Bloom: true duplicates vs detected duplicates.
  - For LSH: true neighbor sets vs approximate neighbor sets.

Conceptually, this file answers:

> “Given our ground truth jobs and baseline notebook, **how well do the streaming algorithms match reality**, and where do they differ?”

You can cite this in the report as the **main quantitative evidence** that your streaming techniques are accurate enough for production‑like usage.

---

## 5. How Ground Truth Is Used in the Project

Ground truth underpins several aspects of the NYC Big Data project:

- **Algorithm evaluation**
  - Bloom filter: compare detected duplicate counts vs `duplicates/` outputs.
  - DGIM: compare sliding‑window counts vs `dgim_metrics/` and baseline hourly/daily aggregates.
  - LSH: compare approximate neighbors vs `lsh_routes/` exact neighbors.

- **Parameter tuning**
  - Adjust Bloom capacity / false‑positive rate to balance memory and accuracy.
  - Tune DGIM window sizes and bucket parameters.
  - Tune LSH parameters (bands, rows, similarity thresholds).

- **Reporting and presentation**
  - Use ground truth numbers and plots to:
    - Show error bars / accuracy curves.
    - Justify why approximations are acceptable for real‑time deployment.

In short, **batch ground truth makes the entire streaming story credible**, because every approximation is backed by a clear comparison to a gold standard.

---

## 6. How to Talk About Ground Truth in Report & Slides

When presenting or writing the report, you can summarize as:

- “We implemented a dedicated **ground truth layer** using Spark jobs on S3 and a local pandas baseline notebook. This layer computes comprehensive batch metrics (counts, duplicates, similarity, temporal and spatial stats) over the full cleaned dataset.”
- “All streaming algorithms (Bloom, DGIM, LSH) are evaluated against this ground truth. We store results in JSON on S3 and local `validation_results.json`, and report error rates and trade‑offs.”
- “This separation between *ground truth* and *streaming approximations* follows best practices in big data systems: **batch for correctness, streaming for freshness**.”

You can reference this file (`Guide/GROUND_TRUTH_SUMMARY.md`) directly when mapping to rubric items about **evaluation methodology, technical difficulty, and experimental rigor**.


