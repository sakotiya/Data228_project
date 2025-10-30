# Design Document: StreamTide - NYC Taxi Big Data Analytics System

## 1. System Overview

StreamTide is a real-time analytics system for NYC taxi demand modeling and dynamic fare optimization. The system processes historical taxi trip records from years 2020 and 2025 using streaming algorithms (Reservoir Sampling and Bloom Filters) to detect demand surges, estimate trip distributions, and provide insights into fare dynamics.

**Data Source:** NYC Taxi & Limousine Commission (TLC) - 2020 & 2025 records in Parquet format (~200M+ records)
**Cloud Platform:** AWS (Amazon Web Services)

## 2. System Architecture

```mermaid
graph TB
    subgraph "AWS Data Sources"
        S3_RAW[AWS S3 Raw Data<br/>s3://nyc-tlc/trip-data/<br/>2020 & 2025 Parquet]
        S3_GEO[S3 Zone Files<br/>Geographic Data]
    end

    subgraph "AWS Storage Layer"
        S3_PROC[S3 Processed Data<br/>Validated Parquet]
        EMR_HDFS[EMR HDFS<br/>Distributed Storage]
    end

    subgraph "AWS Processing - Batch"
        EMR_MR[EMR MapReduce<br/>Ground Truth Baseline]
        EMR_SPARK[EMR Spark<br/>Parquet Processing]
    end

    subgraph "AWS Streaming Layer"
        MSK[Amazon MSK<br/>Managed Kafka]
        PRODUCER[Kafka Producer<br/>on EMR]
        CONSUMER[Kafka Consumer<br/>on EMR]
    end

    subgraph "Algorithm Layer - EMR"
        RS[Reservoir Sampling<br/>1K-1M samples]
        BF[Bloom Filter<br/>Duplicate Detection]
        VAL[Real-time Validator<br/>Accuracy Monitor]
    end

    subgraph "AWS Analytics & Visualization"
        CW[CloudWatch Metrics<br/>System Monitoring]
        QUICKSIGHT[QuickSight Dashboard<br/>Real-time Analytics]
        S3_RESULTS[S3 Results<br/>Performance Metrics]
    end

    S3_RAW --> S3_PROC
    S3_GEO --> S3_PROC
    S3_PROC --> EMR_HDFS

    EMR_HDFS --> EMR_MR
    EMR_HDFS --> EMR_SPARK
    EMR_HDFS --> PRODUCER

    PRODUCER --> MSK
    MSK --> CONSUMER
    CONSUMER --> RS
    CONSUMER --> BF

    RS --> VAL
    BF --> VAL
    EMR_MR --> VAL

    VAL --> S3_RESULTS
    VAL --> CW
    S3_RESULTS --> QUICKSIGHT
    CW --> QUICKSIGHT
```

## 3. Data Pipeline Design

### 3.1 Data Flow Stages

| Stage | Component | Input | Output | Purpose |
|-------|-----------|-------|--------|---------|
| **Ingestion** | AWS CLI / SDK | S3 nyc-tlc bucket (2020, 2025) | Raw Parquet files | Acquire ~200M records |
| **Validation** | EMR Spark job | Raw Parquet from S3 | Validated dataset | Schema check, integrity |
| **Storage** | S3 + EMR HDFS | Validated data | S3 processed bucket | Fault-tolerant storage |
| **Baseline** | EMR MapReduce | S3/HDFS data | Ground truth metrics | Statistical baselines |
| **Streaming** | MSK producer on EMR | S3/HDFS data | Kafka topics | Real-time simulation |
| **Processing** | EMR Spark + Algorithms | MSK stream | Analytics results | Demand/fare analysis |
| **Visualization** | QuickSight + CloudWatch | S3 results, CW metrics | Interactive dashboards | Monitoring & insights |

### 3.2 Data Schema Structure

| Field Category | Fields | Data Type | Purpose |
|----------------|--------|-----------|---------|
| **Trip Identity** | trip_id, vendor_id | String | Unique identification |
| **Temporal** | pickup_datetime, dropoff_datetime | Timestamp | Time-series analysis |
| **Spatial** | pickup_location_id, dropoff_location_id | Integer | Zone-based demand |
| **Fare** | fare_amount, tip_amount, total_amount | Decimal | Pricing analysis |
| **Metrics** | trip_distance, passenger_count | Float/Integer | Trip characteristics |

## 4. AWS Infrastructure Design

### 4.1 AWS EMR Cluster Architecture

```mermaid
graph TB
    subgraph "AWS EMR Cluster"
        MASTER[Master Node<br/>m5.xlarge<br/>NameNode + ResourceManager]

        subgraph "Core Nodes"
            CORE1[Core Node 1<br/>m5.2xlarge<br/>DataNode + NodeManager]
            CORE2[Core Node 2<br/>m5.2xlarge<br/>DataNode + NodeManager]
            CORE3[Core Node 3<br/>m5.2xlarge<br/>DataNode + NodeManager]
        end

        subgraph "Task Nodes - Auto-scaling"
            TASK1[Task Node 1<br/>m5.xlarge<br/>NodeManager]
            TASK2[Task Node N<br/>m5.xlarge<br/>NodeManager]
        end
    end

    subgraph "AWS Storage"
        S3[S3 Buckets<br/>Primary Storage]
    end

    MASTER -.->|Coordinate| CORE1
    MASTER -.->|Coordinate| CORE2
    MASTER -.->|Coordinate| CORE3
    MASTER -.->|Coordinate| TASK1
    MASTER -.->|Coordinate| TASK2

    CORE1 <-.->|Data| S3
    CORE2 <-.->|Data| S3
    CORE3 <-.->|Data| S3
```

**EMR Cluster Specifications:**

| Parameter | Configuration | Rationale |
|-----------|--------------|-----------|
| **Master Node** | 1x m5.xlarge (4 vCPU, 16 GB) | Cluster coordination, lightweight |
| **Core Nodes** | 3x m5.2xlarge (8 vCPU, 32 GB) | HDFS storage + processing |
| **Task Nodes** | 2-10x m5.xlarge (auto-scale) | Flexible compute for peak loads |
| **EMR Version** | 6.10+ or 7.x | Latest Spark 3.x, Kafka support |
| **HDFS Replication** | 3x | Fault tolerance within EMR |
| **S3 Storage** | 2-5 TB (2020+2025 data) | Primary durable storage |

### 4.2 EMR Spark Configuration

| Component | Configuration | Purpose |
|-----------|--------------|---------|
| **Execution Mode** | Cluster mode on EMR | Distributed processing |
| **Executor Memory** | 8-12 GB per executor | Handle 200M+ record processing |
| **Executor Cores** | 4 cores per executor | Parallel task execution |
| **Driver Memory** | 8 GB | Manage job coordination |
| **Partitioning** | 200-400 partitions | Optimize parallelism |
| **Optimizer** | Catalyst + Tungsten | Query optimization, code generation |
| **S3 Integration** | S3A connector with committers | Efficient S3 read/write |

### 4.3 AWS S3 Bucket Structure

| Bucket/Prefix | Purpose | Data Format | Lifecycle |
|---------------|---------|-------------|-----------|
| **s3://nyc-taxi-raw/** | Raw TLC data download | Parquet (2020, 2025) | Retain 90 days |
| **s3://nyc-taxi-processed/** | Validated, cleaned data | Parquet (partitioned) | Retain indefinitely |
| **s3://nyc-taxi-results/** | Ground truth, algorithm output | Parquet, JSON | Retain 180 days |
| **s3://nyc-taxi-logs/** | EMR logs, application logs | Text, JSON | Retain 30 days |
| **s3://nyc-taxi-checkpoints/** | Spark streaming checkpoints | Binary | Retain 7 days |

## 5. AWS Streaming Architecture Design

### 5.1 Amazon MSK (Managed Streaming for Kafka) Setup

```mermaid
graph TB
    subgraph "Amazon MSK Cluster"
        subgraph "Availability Zone 1"
            B1[MSK Broker 1<br/>kafka.m5.large]
        end
        subgraph "Availability Zone 2"
            B2[MSK Broker 2<br/>kafka.m5.large]
        end
        subgraph "Availability Zone 3"
            B3[MSK Broker 3<br/>kafka.m5.large]
        end
    end

    subgraph "Topics - Year & Borough Partitioned"
        T1[taxi-trips-2020]
        T2[taxi-trips-2025]
        T3[taxi-trips-manhattan]
        T4[taxi-trips-brooklyn]
    end

    subgraph "EMR Producers"
        P1[EMR Producer Task<br/>Historical Replay from S3]
    end

    subgraph "EMR Consumer Groups"
        CG1[Consumer Group 1<br/>Reservoir Sampling]
        CG2[Consumer Group 2<br/>Bloom Filter]
        CG3[Consumer Group 3<br/>Validator]
    end

    P1 --> T1
    P1 --> T2
    P1 --> T3
    P1 --> T4

    T1 --> CG1
    T2 --> CG1
    T3 --> CG2
    T4 --> CG2
    T1 --> CG3
```

**Amazon MSK Configuration:**

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| **Broker Type** | kafka.m5.large (2 vCPU, 8 GB) | Cost-effective for 200M records |
| **Brokers** | 3 brokers (multi-AZ) | High availability, fault tolerance |
| **Partitions** | 12 per topic | Parallel consumption, scalability |
| **Replication** | 3x (across AZs) | Data durability |
| **Retention** | 7 days | Allow replay for validation |
| **Compression** | Snappy | Reduce network/storage overhead |
| **Authentication** | IAM + TLS | Secure access from EMR |

### 5.2 AWS Stream Processing Flow

```mermaid
sequenceDiagram
    participant S3 as S3 Storage<br/>(2020, 2025 data)
    participant Producer as EMR Producer
    participant MSK as Amazon MSK
    participant Spark as EMR Spark Streaming
    participant RS as Reservoir Sampling
    participant BF as Bloom Filter
    participant S3_Out as S3 Results
    participant QuickSight as QuickSight Dashboard

    S3->>Producer: Read Parquet (2020, 2025)
    Producer->>MSK: Publish trip events
    MSK->>Spark: Stream consumption

    par Parallel Processing
        Spark->>RS: Sample representative trips
        Spark->>BF: Check duplicates
    end

    RS->>S3_Out: Write demand metrics
    BF->>S3_Out: Write duplicate stats
    S3_Out->>QuickSight: Load visualizations

    Note over RS,BF: Continuous real-time processing
```

### 5.3 AWS Data Year Filtering Strategy

| Year | Records (Approx) | S3 Prefix Pattern | Use Case |
|------|------------------|-------------------|----------|
| **2020** | ~100M trips | s3://nyc-tlc/trip-data/yellow_tripdata_2020-*.parquet | Pre-pandemic + pandemic impact |
| **2025** | ~100M trips | s3://nyc-tlc/trip-data/yellow_tripdata_2025-*.parquet | Current year patterns |
| **Combined** | ~200M trips | Both prefixes | Temporal comparison analysis |

## 6. Algorithm Implementation Design

### 6.1 Reservoir Sampling Design

**Purpose:** Memory-efficient random sampling to maintain representative subset of streaming taxi trips

```mermaid
flowchart TD
    A[Stream Event Arrives] --> B{Reservoir Full?}
    B -->|No k < n| C[Add to Reservoir]
    B -->|Yes k = n| D[Generate Random j]
    D --> E{j < n ?}
    E -->|Yes| F[Replace reservoir j with new item]
    E -->|No| G[Skip item]
    C --> H[Update Statistics]
    F --> H
    G --> H
    H --> I[Emit Sample Metrics]
```

**Configuration Matrix:**

| Sample Size | Memory Usage | Accuracy | Use Case |
|-------------|--------------|----------|----------|
| 1,000 | ~100 KB | ±5% | Quick prototyping |
| 10,000 | ~1 MB | ±2% | Development testing |
| 100,000 | ~10 MB | ±0.5% | Production monitoring |
| 1,000,000 | ~100 MB | ±0.1% | High-precision analysis |

**Stratification Strategy:**

| Dimension | Strata | Purpose |
|-----------|--------|---------|
| **Temporal** | Hourly buckets (0-23) | Capture peak/off-peak patterns |
| **Spatial** | Borough-based (5 zones) | Geographic demand distribution |
| **Fare Tiers** | Low/Medium/High | Price sensitivity analysis |

### 6.2 Bloom Filter Design

**Purpose:** Fast duplicate detection across billions of trip records with minimal memory

```mermaid
flowchart LR
    A[Trip ID] --> B[Hash Function 1]
    A --> C[Hash Function 2]
    A --> D[Hash Function k]

    B --> E[Bit Array Position 1]
    C --> F[Bit Array Position 2]
    D --> G[Bit Array Position k]

    E --> H{All Bits = 1?}
    F --> H
    G --> H

    H -->|Yes| I[Possibly Duplicate]
    H -->|No| J[Definitely Unique<br/>Set Bits to 1]
```

**Configuration Parameters:**

| Bit Array Size | Hash Functions | Expected Items | False Positive Rate | Memory |
|----------------|----------------|----------------|---------------------|--------|
| 1 MB | 3 | 100K | ~1% | 1 MB |
| 10 MB | 5 | 1M | ~0.1% | 10 MB |
| 100 MB | 7 | 10M | ~0.01% | 100 MB |
| 1 GB | 10 | 100M | ~0.001% | 1 GB |

**Hash Function Strategy:**

| Hash Type | Characteristics | Use Case |
|-----------|----------------|----------|
| **MurmurHash3** | Fast, good distribution | Primary hashing |
| **CityHash** | Optimized for strings | Trip ID hashing |
| **SHA-256** | High quality, slower | Critical deduplication |

## 7. Ground Truth Baseline Design

### 7.1 EMR MapReduce Ground Truth Pipeline

```mermaid
graph LR
    subgraph "S3 Input"
        S3_IN[S3 Processed Data<br/>2020 + 2025 Parquet]
    end

    subgraph "EMR Map Phase"
        M1[Mapper 1<br/>Parse Parquet]
        M2[Mapper 2<br/>Parse Parquet]
        M3[Mapper N<br/>Parse Parquet]
    end

    subgraph "Shuffle & Sort"
        SS[Partition by Key<br/>Year/Time/Zone/Fare]
    end

    subgraph "EMR Reduce Phase"
        R1[Reducer 1<br/>Compute Stats]
        R2[Reducer 2<br/>Compute Stats]
        R3[Reducer N<br/>Compute Stats]
    end

    subgraph "S3 Output"
        GT[S3 Ground Truth<br/>Statistics by Year]
    end

    S3_IN --> M1
    S3_IN --> M2
    S3_IN --> M3
    M1 --> SS
    M2 --> SS
    M3 --> SS
    SS --> R1
    SS --> R2
    SS --> R3
    R1 --> GT
    R2 --> GT
    R3 --> GT
```

### 7.2 Ground Truth Metrics (Year-wise Comparison)

| Metric Category | Measurements | 2020 Baseline | 2025 Baseline |
|-----------------|--------------|---------------|---------------|
| **Trip Duration** | Mean, median, std dev, percentiles | Pandemic impact analysis | Current patterns |
| **Fare Distribution** | Mean, median, mode, variance | 2020 pricing baseline | 2025 pricing trends |
| **Temporal Patterns** | Hourly/daily/weekly aggregates | COVID-era demand | Post-pandemic demand |
| **Spatial Analytics** | Zone-wise demand, borough distribution | 2020 geographic patterns | 2025 hotspots |
| **Duplicate Count** | Exact duplicate trip_id count | 2020 data quality | 2025 data quality |
| **Year-over-Year** | Growth rates, pattern shifts | Comparative analysis | Trend identification |

## 8. Performance Evaluation Design

### 8.1 Accuracy Validation Framework (AWS)

```mermaid
graph TB
    GT[EMR MapReduce<br/>Ground Truth S3] --> COMP[EMR Comparison Job]
    RS[Reservoir Sample<br/>from MSK] --> COMP
    BF[Bloom Filter<br/>from MSK] --> COMP

    COMP --> MAE[Mean Absolute Error]
    COMP --> RMSE[Root Mean Squared Error]
    COMP --> FPR[False Positive Rate]
    COMP --> CI[95% Confidence Intervals]

    MAE --> S3_REPORT[S3 Performance Report]
    RMSE --> S3_REPORT
    FPR --> S3_REPORT
    CI --> S3_REPORT

    S3_REPORT --> QS[QuickSight Dashboards]
    S3_REPORT --> CW[CloudWatch Metrics]
```

**Error Metrics:**

| Metric | Formula | Acceptable Threshold | Purpose |
|--------|---------|---------------------|---------|
| **MAE** | Σ\|predicted - actual\| / n | < 5% | Average deviation |
| **RMSE** | √(Σ(predicted - actual)² / n) | < 7% | Penalize large errors |
| **False Positive Rate** | FP / (FP + TN) | < 1% | Bloom filter accuracy |
| **Sample Bias** | \|sample_mean - population_mean\| | < 2% | Sampling quality |

### 8.2 AWS Performance Testing Matrix

| Test Category | Metrics | AWS Measurement Method |
|---------------|---------|------------------------|
| **Memory Efficiency** | Peak usage, allocation patterns | EMR JVM profiling, CloudWatch |
| **Throughput** | Records/second, events/minute | MSK metrics, Spark UI, CloudWatch |
| **Latency** | End-to-end processing time, p99 latency | CloudWatch Insights, X-Ray tracing |
| **Scalability** | Performance at 1M, 10M, 50M, 100M, 200M records | EMR auto-scaling testing |
| **Resource Utilization** | CPU, memory, network, disk I/O | CloudWatch, EMR metrics |
| **Cost Efficiency** | Processing cost per million records | AWS Cost Explorer |

### 8.3 AWS Bottleneck Analysis & Optimization

```mermaid
graph TD
    A[CloudWatch Performance Test] --> B{Identify Bottleneck}
    B -->|Low throughput| C[CPU Bottleneck]
    B -->|High latency| D[Network Bottleneck]
    B -->|Memory pressure| E[Memory Bottleneck]
    B -->|Slow S3 reads| F[I/O Bottleneck]

    C --> G[Scale: Add EMR task nodes]
    D --> H[Optimize: MSK compression, batching]
    E --> I[Scale: Larger instance types, tune GC]
    F --> J[Optimize: S3A committers, parallel reads]
```

## 9. AWS Real-time Analytics Dashboard Design

### 9.1 QuickSight Dashboard Components

| Panel | Visualizations | Metrics Displayed | Data Source | Refresh |
|-------|---------------|-------------------|-------------|---------|
| **EMR Health** | Gauge charts, time series | CPU, memory, cluster status | CloudWatch | 1 minute |
| **MSK Performance** | Line charts | Throughput, consumer lag | MSK metrics | 1 minute |
| **Algorithm Accuracy** | Line charts, error bars | MAE, RMSE vs ground truth (2020 vs 2025) | S3 results | 5 minutes |
| **Geographic View** | Interactive NYC map, heatmap | Zone demand (2020 vs 2025) | S3 Athena query | 5 minutes |
| **Temporal Comparison** | Dual time-series | 2020 vs 2025 hourly demand | S3 Athena query | 5 minutes |
| **Fare Analytics** | Distribution plots, box plots | Fare trends (2020 vs 2025) | S3 Athena query | 5 minutes |
| **Duplicate Detection** | Counter widgets, pie charts | Bloom filter stats by year | S3 results | 5 minutes |

### 9.2 AWS Dashboard Architecture

```mermaid
graph LR
    subgraph "AWS Data Sources"
        EMR[EMR Metrics]
        MSK[MSK Metrics]
        S3_RES[S3 Results<br/>Parquet]
    end

    subgraph "AWS Analytics Services"
        CW[CloudWatch<br/>Metrics & Logs]
        ATHENA[Athena<br/>SQL on S3]
        GLUE[Glue Catalog<br/>Schema Registry]
    end

    subgraph "Visualization"
        QS[QuickSight<br/>Interactive Dashboards]
    end

    subgraph "Alerts"
        SNS[SNS Topics<br/>Threshold Alerts]
        LAMBDA[Lambda<br/>Custom Actions]
    end

    EMR --> CW
    MSK --> CW
    S3_RES --> GLUE
    GLUE --> ATHENA

    CW --> QS
    ATHENA --> QS
    CW --> SNS
    SNS --> LAMBDA
```

## 10. Data Validation Strategy

### 10.1 Multi-source Validation

| Validation Type | Check | Action on Failure |
|-----------------|-------|-------------------|
| **Schema Consistency** | Column names, data types match across TLC/S3/Azure | Log inconsistency, use TLC as primary |
| **Temporal Alignment** | Timestamp ranges match expected periods | Filter out-of-range records |
| **Referential Integrity** | Location IDs exist in zone lookup | Drop invalid trips, log count |
| **Data Completeness** | Required fields non-null | Impute or drop based on % missing |
| **Range Validation** | Fare > 0, distance > 0, passengers > 0 | Flag anomalies, apply filters |

### 10.2 AWS Data Cleaning Pipeline

```mermaid
flowchart TD
    A[S3 Raw Parquet<br/>2020 & 2025] --> B[EMR Spark Validation Job]
    B --> C{Valid Schema?}
    C -->|No| D[Log to CloudWatch & Skip]
    C -->|Yes| E[Null Check]
    E --> F{Critical Nulls?}
    F -->|Yes| D
    F -->|No| G[Range Validation]
    G --> H{In Range?}
    H -->|No| I[Flag Anomaly<br/>CloudWatch Metric]
    H -->|Yes| J[Year Filter<br/>2020 or 2025 only]
    I --> J
    J --> K{Duplicate?}
    K -->|Yes| L[Mark & Track in S3]
    K -->|No| M[S3 Processed Bucket<br/>Partitioned by Year]
    L --> M
```


### 11.2 AWS Component Dependencies

```mermaid
graph TD
    A[AWS Account Setup] --> B[S3 Bucket Creation]
    B --> C[Download 2020 & 2025 Data]
    C --> D[EMR Cluster Launch]
    D --> E[Data Validation Job]
    E --> F[MapReduce Baseline]
    E --> G[MSK Cluster Setup]
    G --> H[Spark Streaming Jobs]
    F --> I[Algorithm Implementation]
    H --> I
    I --> J[Performance Evaluation]
    I --> K[QuickSight Dashboard]
    J --> L[Final Report]
    K --> L
```
\