## NYC Taxi Big Data Streaming Project

End‑to‑end big data pipeline on **NYC Taxi & FHV/FHVHV trips**, combining:

- **Historical batch processing (through 2024)** on S3 + Redshift
- **Streaming (2025+) via Kafka + streaming algorithms** (Bloom Filter, DGIM, LSH)
- **Ground‑truth Spark jobs** for evaluation
- **Privacy and Responsible AI considerations**

This README gives a concise overview you can reuse in your **report** and **slides**, and points to the **technical design graphs** and **data dictionary** artifacts.

---

## 1. High‑Level Flow

### 1.1 Historical (Batch) Phase – up to 2024

- **Ingest NYC taxi data** (yellow, green, FHV, FHVHV) from public sources.
- **Clean & normalize** using notebooks in `ETL/` and `validation/`, guided by the data dictionary:
  - Fix types, remove impossible values, standardize column names.
- **Write cleaned parquet** to **AWS S3**.
- **Expose S3 data via Redshift staging**:
  - Redshift external tables on top of S3 cleaned data.
  - Used to build **reference structures**:
    - Bloom filters for duplicate detection.
    - LSH indices / similarity baselines.
    - Ground‑truth aggregates and statistics.

### 1.2 Streaming (Online) Phase – 2025+

- **Kafka** simulates 2025+ trip events as a continuous stream.
- Streaming consumers apply:
  - **Bloom Filter** – flag likely duplicates vs. historical data.
  - **DGIM** – approximate recent‑window counts (e.g., recent surge detection).
  - **LSH** – approximate similarity search (e.g., similar customer/route behavior).
- Enriched streaming outputs are written back to **S3 prod paths**, and surfaced via:
  - **Redshift prod tables** on top of prod S3.
  - **QuickSight dashboards** for BI and monitoring.

---

## 2. Tech Stack & System Design

### 2.1 Core Technologies

- **Storage & Warehouse**
  - AWS **S3** for raw, cleaned, and prod datasets.
  - **Redshift** as the warehouse / query engine on top of S3.

- **Compute & Processing**
  - **Apache Spark** (batch) – ground truth jobs in `spark-jobs_groundTruth/`.
  - **Python** streaming consumers – algorithms in `algorithms/`.

- **Streaming**
  - **Kafka** topics for trip events (2025+).
  - Algorithm services consume from Kafka and write enriched results to S3.

- **Algorithms**
  - **Bloom Filter** (`algorithms/bloom_filter/`) – duplicate detection.
  - **DGIM** (`algorithms/DGIM/`) – sliding‑window counting.
  - **LSH** (`algorithms/LSH/`) – locality‑sensitive hashing for similarity.

- **Privacy & Governance**
  - Privacy demos in `privacy/` explore **k‑Anonymity‑style generalization** and simple noise addition.
  - Data dictionary + schemas help avoid direct PII and keep transformations auditable.

### 2.2 Technical Design Diagrams

High‑level and component‑level architecture diagrams are maintained under:

- `graphs/tech_design/` – **system and dataflow schematics** (architecture, ingestion → S3 → Redshift → Kafka → prod, etc.).
- `graphs/bloomFilter_graph/` – Bloom filter architecture, streaming timeline, and use‑case diagrams.
  - `bloom_analysis_plots/`
  - `bloom_streaming_plots/`
  - `bloom_streaming_timeline_plots/`
  - `bloom_use_cases_plots/`

Use these figures directly in your slides/report when explaining:

- Overall **Lambda‑style architecture** (batch + streaming).
- How streaming algorithms plug into Kafka and S3/Redshift.
- Example analytical insights (duplicate rates, efficiency curves, etc.).

---

## 3. Repository Layout (Key Folders)

- **`algorithms/`** – core streaming algorithms and simulators
  - `bloom_filter/` – Bloom filter creation, local dedup, and plotting.
  - `DGIM/` – DGIM implementation and simulators.
  - `LSH/` – LSH implementation and simulators; includes similarity/efficiency graphs (see `graphs/lsh_graph/`).

- **`ETL/`** – ingestion, cleaning, and Lambda‑style processing
  - `analyze_and_clean_Local.ipynb` – local data cleaning and EDA.
  - `lambda/analyze_and_clean_lambda.ipynb` – S3‑integrated cleaning pipeline.

- **`spark-jobs_groundTruth/`** – Spark jobs to compute **ground truth**:
  - `ground_truth_basic_stats.py`, `ground_truth_cardinality.py`, `ground_truth_duplicates.py`, …
  - `GROUND_TRUTH_JOBS.md` explains how to run and what each job produces.

- **`privacy/`** – privacy techniques and demos
  - `privacy_demo.py`, `privacy_techniques.py`.

- **`graphs/`** – all figures and plots used in the report/slides
  - `bloomFilter_graph/`, `dgim_graph/`, `lsh_graph/`, and `tech_design/` (system diagrams).

- **`Guide/`** – written documentation
  - `Design_Document.md` – architecture and design choices.
  - `Implementation_Guide.md` – more detailed runbook.
  - `INSIGHTS_GUIDE.md` – summary of insights from experiments.
  - `dedup_summary.md` – focus on duplicate detection.

- **`validation/`** – data validation notebooks and results
  - `data_validation.ipynb`, `ground_truth_baseline.ipynb`, `validation_results.json`.

---

## 4. Data Dictionary (Schema & Mapping)

Authoritative schemas and mappings are in the **`data_dictionary/`** folder.

### 4.1 Canonical Schema & Availability

- **Master summary of fields**  
  - [`data_dictionary/data_dictionary_Master_Summary.csv`](data_dictionary/data_dictionary_Master_Summary.csv)  
    Overview of all canonical fields, descriptions, and high‑level usage.

- **Canonical columns definition**  
  - [`data_dictionary/data_dictionary_canonical_columns.csv`](data_dictionary/data_dictionary_canonical_columns.csv)  
    Canonical column names, types, and semantic meanings used in the project.

- **Availability by taxi type**  
  - [`data_dictionary/data_dictionary_canonical_availability_by_type_.csv`](data_dictionary/data_dictionary_canonical_availability_by_type_.csv)  
    Matrix of which canonical fields are present in **yellow / green / FHV / FHVHV** datasets.

### 4.2 Mapping & Validation Rules

- **Source → canonical mapping rules**  
  - [`data_dictionary/data_dictionary_Data_Mapping.csv`](data_dictionary/data_dictionary_Data_Mapping.csv)  
    How raw source columns map into the canonical schema (including renames and transforms).

- **Cleaning & validation checks**  
  - [`data_dictionary/data_dictionary_Cleanng_validation.csv`](data_dictionary/data_dictionary_Cleanng_validation.csv)  
    Validation logic and cleaning rules (range checks, allowed values, null‑handling, etc.).

> These CSVs are Git‑friendly (diffable) versions of the original Excel files and are meant to be cited in the report when you describe **schema design**, **ETL rules**, and **data quality**.

---

## 5. How to Run (High‑Level)

> See `Guide/Implementation_Guide.md` for full, step‑by‑step instructions. Below is a quick roadmap.

1. **Set up environment**
   - Python 3.x with required packages (e.g., `pandas`, `pyarrow`, `pyspark`, `boto3`, Kafka client libs).
   - Access to **AWS S3** and (optionally) **Redshift** and **Kafka**.

2. **ETL – Clean historical data**
   - Use `ETL/analyze_and_clean_Local.ipynb` or Lambda notebook to:
     - Read raw NYC taxi datasets.
     - Apply mappings and validation from the **data dictionary**.
     - Write cleaned parquet files to S3.

3. **Compute ground truth with Spark**
   - From `spark-jobs_groundTruth/`, run relevant scripts (see `GROUND_TRUTH_JOBS.md`).
   - Outputs go to designated S3 paths and are used as benchmarks for streaming algorithms.

4. **Run algorithms (offline/streaming)**
   - **Offline**: run simulators in `algorithms/` to understand behavior and generate plots in `graphs/`.
   - **Streaming**: connect Kafka consumers that:
     - Read trip events.
     - Query Bloom/DGIM/LSH structures built from historical data.
     - Write enriched results to S3 prod paths (then to Redshift prod tables).

5. **Visualize**
   - Use **QuickSight** on top of Redshift prod tables for dashboards.
   - Use scripts in `graphs/` to reproduce the figures for the report and slides.

---

## 6. Algorithms & Course Techniques Used

- **Streaming algorithms**
  - Bloom Filter (duplicate detection).
  - DGIM (approximate counts over sliding windows).
  - Frequency moments / cardinality (via Spark ground truth jobs).
  - Locality Sensitive Hashing (LSH) for similarity.

- **Stream processing frameworks**
  - Spark for batch / ground truth.
  - Kafka for event streaming (2025+ trips).

- **Privacy & Responsible AI**
  - Privacy demos in `privacy/` (generalization, noise).
  - Use of data dictionary to keep transformations explicit and avoid direct PII.

- **Additional techniques (for rubric)**
  - Fairness & Explainability: you can analyze **flag rates** (duplicates/anomalies) by taxi type/borough and log reasons for flags.
  - Data Poisoning: you can add sanity checks and spike detection on Kafka streams to discuss robustness.

Use this section as a checklist in your **report** and **presentation** when mapping to course topics (streaming algorithms, LSH, privacy, Responsible AI, etc.).

---

## 7. For Graders & Reviewers

- **Code walkthrough**: follow Sections 1–3 plus `Guide/Design_Document.md`.
- **Data dictionary**: Section 4 and the CSV links provide schema, mappings, and validation logic.
- **Tech design / flow diagrams**: see Section 2.2 and the `graphs/tech_design/` and `graphs/bloomFilter_graph/` folders.
- **Streaming algorithms & frameworks**: Section 6 plus the `algorithms/` and `spark-jobs_groundTruth/` code.
- **Privacy / Responsible AI / other course topics**: `privacy/` directory and the notes in Section 6.

This README is meant to be the **single landing page** for understanding the project structure, technical design, and how it connects to the course rubric.
