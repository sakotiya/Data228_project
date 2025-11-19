# StreamTide Implementation Guide
## Complete Step-by-Step Setup & Code

---

## Table of Contents
1. [AWS Account Setup](#phase-1-aws-account-setup)
2. [S3 Data Ingestion](#phase-2-s3-data-ingestion)
3. [EMR Cluster Setup](#phase-3-emr-cluster-setup)
4. [Data Validation Pipeline](#phase-4-data-validation-pipeline)
5. [Ground Truth Baseline (MapReduce)](#phase-5-ground-truth-baseline)
6. [Amazon MSK Setup](#phase-6-amazon-msk-setup)
7. [Streaming Algorithms](#phase-7-streaming-algorithms)
8. [Performance Evaluation](#phase-8-performance-evaluation)
9. [QuickSight Dashboards](#phase-9-quicksight-dashboards)
10. [Teardown & Cost Management](#phase-10-teardown)

---

## Phase 1: AWS Account Setup

### 1.1 Create AWS Account (if needed)

**Option A: AWS Educate Account (Recommended for Students)**
```bash
# Visit: https://aws.amazon.com/education/awseducate/
# Apply with your university email (@sjsu.edu)
# You'll receive $100+ in credits
```

**Option B: Regular AWS Account**
- Sign up at: https://aws.amazon.com
- Requires credit card (but we'll use free tier)

### 1.2 Install AWS CLI

```bash
# macOS
brew install awscli

# Verify installation
aws --version

# Configure AWS credentials
aws configure
# Enter:
#   AWS Access Key ID: [from IAM console]
#   AWS Secret Access Key: [from IAM console]
#   Default region: us-east-1
#   Default output format: json
```

### 1.3 Create IAM User with Required Permissions

**Via AWS Console:**
1. Go to IAM → Users → Add User
2. Username: `streamtide-admin`
3. Access type: ✅ Programmatic access, ✅ AWS Management Console access
4. Attach policies:
   - `AmazonS3FullAccess`
   - `AmazonEMRFullAccessPolicy_v2`
   - `AmazonMSKFullAccess`
   - `QuickSightFullAccess`
   - `CloudWatchFullAccess`

**Via CLI:**
```bash
# Create IAM user
aws iam create-user --user-name streamtide-admin

# Attach policies
aws iam attach-user-policy \
  --user-name streamtide-admin \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-user-policy \
  --user-name streamtide-admin \
  --policy-arn arn:aws:iam::aws:policy/AmazonElasticMapReduceFullAccess

# Create access key
aws iam create-access-key --user-name streamtide-admin
```

---

## Phase 2: S3 Data Ingestion

### 2.1 Create S3 Buckets

```bash
# Set variables
export AWS_REGION=us-east-1
export BUCKET_PREFIX=streamtide-nyc-taxi-$(date +%s)

# Create buckets
aws s3 mb s3://${BUCKET_PREFIX}-raw --region ${AWS_REGION}
aws s3 mb s3://${BUCKET_PREFIX}-processed --region ${AWS_REGION}
aws s3 mb s3://${BUCKET_PREFIX}-results --region ${AWS_REGION}
aws s3 mb s3://${BUCKET_PREFIX}-logs --region ${AWS_REGION}
aws s3 mb s3://${BUCKET_PREFIX}-checkpoints --region ${AWS_REGION}

# Enable versioning on critical buckets
aws s3api put-bucket-versioning \
  --bucket ${BUCKET_PREFIX}-processed \
  --versioning-configuration Status=Enabled

echo "Buckets created. Save this prefix: ${BUCKET_PREFIX}"
```

### 2.2 Download NYC Taxi Data (2020 & 2025)

Create script: `scripts/download_data.sh`

```bash
#!/bin/bash

BUCKET_RAW="s3://${BUCKET_PREFIX}-raw"

# Download 2020 data (12 months)
echo "Downloading 2020 data..."
for month in {01..12}; do
  echo "Downloading 2020-${month}..."
  wget -P /tmp/taxi-data/ \
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-${month}.parquet"
done

# Download 2025 data (available months only - adjust as needed)
echo "Downloading 2025 data..."
for month in {01..03}; do  # Adjust based on current month
  echo "Downloading 2025-${month}..."
  wget -P /tmp/taxi-data/ \
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-${month}.parquet" || echo "2025-${month} not available yet"
done

# Download zone lookup file
echo "Downloading zone lookup..."
wget -P /tmp/taxi-data/ \
  "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"

# Upload to S3
echo "Uploading to S3..."
aws s3 sync /tmp/taxi-data/ ${BUCKET_RAW}/raw-data/ \
  --storage-class STANDARD_IA

# Verify upload
aws s3 ls ${BUCKET_RAW}/raw-data/ --recursive --human-readable

# Upload zone file separately
aws s3 cp /tmp/taxi-data/taxi+_zone_lookup.csv \
  ${BUCKET_RAW}/zone-lookup/taxi_zone_lookup.csv

echo "Data download complete!"
```

Run:
```bash
chmod +x scripts/download_data.sh
./scripts/download_data.sh
```

### 2.3 Quick Data Inspection

```bash
# Install parquet-tools (if not installed)
pip install parquet-tools

# Inspect a sample file
aws s3 cp s3://${BUCKET_PREFIX}-raw/raw-data/yellow_tripdata_2020-01.parquet /tmp/
parquet-tools show /tmp/yellow_tripdata_2020-01.parquet | head -20

# Check row counts
parquet-tools rowcount /tmp/yellow_tripdata_2020-01.parquet
```

---

## Phase 3: EMR Cluster Setup

### 3.1 Option A: AWS Console (Web UI) - Recommended for Beginners

**Step-by-Step Web UI Setup:**

1. **Navigate to EMR Console**
   - Go to AWS Console → Services → EMR
   - Click "Create cluster"

2. **Choose Cluster Configuration**
   - Click "Go to advanced options"
   - EMR Release: Select `emr-6.15.0`
   - Applications: Check ✅ Hadoop, ✅ Spark, ✅ Hive, ✅ Livy
   - Click "Next"

3. **Configure Hardware**
   - **Master Node:**
     - Instance type: `m5.xlarge`
     - Instance count: 1

   - **Core Nodes:**
     - Instance type: `m5.2xlarge`
     - Instance count: 3
     - EBS Storage: 100 GB gp3

   - **Task Nodes (Optional):**
     - Instance type: `m5.xlarge`
     - Instance count: 2
     - Purchasing option: Spot instances
     - Maximum Spot price: $0.10/hour

   - EC2 Subnet: Select any subnet in your default VPC
   - Click "Next"

4. **General Cluster Settings**
   - Cluster name: `StreamTide-EMR-Cluster`
   - S3 folder for logs: `s3://YOUR-BUCKET-logs/emr-logs/`
   - ✅ Check "Termination protection" (uncheck for testing)
   - Tags:
     - Key: `Project`, Value: `StreamTide`
     - Key: `Environment`, Value: `Development`
   - Click "Next"

5. **Security Settings**
   - EC2 key pair: Select existing or create new
   - Permissions:
     - EMR role: `EMR_DefaultRole` (auto-created)
     - EC2 instance profile: `EMR_EC2_DefaultRole` (auto-created)
   - Click "Create cluster"

6. **Wait for Cluster to Start** (~15 minutes)
   - Status will change: Starting → Bootstrapping → Running
   - Once running, note the **Master public DNS**

7. **Enable SSH Access**
   - Select your cluster → Security groups for Master
   - Edit inbound rules → Add rule:
     - Type: SSH
     - Port: 22
     - Source: My IP (or your IP range)
   - Save rules

**Screenshot Reference Points:**
- Software configuration screen shows all applications
- Hardware configuration shows instance types and counts
- General configuration shows logging location
- Security shows SSH key pair selection

### 3.2 Option B: AWS CLI - For Automation

### 3.2.1 Create EMR Cluster Configuration

Create: `config/emr-cluster-config.json`

```json
{
  "Name": "StreamTide-EMR-Cluster",
  "ReleaseLabel": "emr-6.15.0",
  "Applications": [
    {"Name": "Hadoop"},
    {"Name": "Spark"},
    {"Name": "Hive"},
    {"Name": "Livy"}
  ],
  "Instances": {
    "InstanceGroups": [
      {
        "Name": "Master",
        "InstanceRole": "MASTER",
        "InstanceType": "m5.xlarge",
        "InstanceCount": 1
      },
      {
        "Name": "Core",
        "InstanceRole": "CORE",
        "InstanceType": "m5.2xlarge",
        "InstanceCount": 3,
        "EbsConfiguration": {
          "EbsBlockDeviceConfigs": [
            {
              "VolumeSpecification": {
                "VolumeType": "gp3",
                "SizeInGB": 100
              },
              "VolumesPerInstance": 1
            }
          ]
        }
      },
      {
        "Name": "Task",
        "InstanceRole": "TASK",
        "InstanceType": "m5.xlarge",
        "InstanceCount": 2,
        "Market": "SPOT",
        "BidPrice": "0.10"
      }
    ],
    "Ec2KeyName": "YOUR_EC2_KEY_NAME",
    "KeepJobFlowAliveWhenNoSteps": true,
    "TerminationProtected": false,
    "Ec2SubnetId": "subnet-xxxxx"
  },
  "BootstrapActions": [
    {
      "Name": "Install Python Dependencies",
      "ScriptBootstrapAction": {
        "Path": "s3://${BUCKET_PREFIX}-logs/bootstrap/install-dependencies.sh"
      }
    }
  ],
  "Configurations": [
    {
      "Classification": "spark",
      "Properties": {
        "maximizeResourceAllocation": "true"
      }
    },
    {
      "Classification": "spark-defaults",
      "Properties": {
        "spark.executor.memory": "10G",
        "spark.executor.cores": "4",
        "spark.driver.memory": "8G",
        "spark.default.parallelism": "400",
        "spark.sql.shuffle.partitions": "400",
        "spark.hadoop.fs.s3a.committer.name": "magic",
        "spark.hadoop.fs.s3a.committer.magic.enabled": "true"
      }
    }
  ],
  "LogUri": "s3://${BUCKET_PREFIX}-logs/emr-logs/",
  "ServiceRole": "EMR_DefaultRole",
  "JobFlowRole": "EMR_EC2_DefaultRole",
  "VisibleToAllUsers": true,
  "Tags": [
    {"Key": "Project", "Value": "StreamTide"},
    {"Key": "Environment", "Value": "Development"}
  ]
}
```

### 3.2 Create Bootstrap Script

Create: `scripts/bootstrap/install-dependencies.sh`

```bash
#!/bin/bash

set -e

echo "Installing Python dependencies..."

# Install pip packages
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install \
  pandas \
  pyarrow \
  fastparquet \
  kafka-python \
  mmh3 \
  boto3 \
  matplotlib \
  seaborn

echo "Bootstrap complete!"
```

Upload bootstrap script:
```bash
aws s3 cp scripts/bootstrap/install-dependencies.sh \
  s3://${BUCKET_PREFIX}-logs/bootstrap/install-dependencies.sh
```

### 3.3 Launch EMR Cluster via CLI

```bash
# Create EC2 key pair first (if you don't have one)
aws ec2 create-key-pair --key-name streamtide-key \
  --query 'KeyMaterial' --output text > ~/.ssh/streamtide-key.pem
chmod 400 ~/.ssh/streamtide-key.pem

# Get default VPC subnet
export SUBNET_ID=$(aws ec2 describe-subnets \
  --filters "Name=default-for-az,Values=true" \
  --query 'Subnets[0].SubnetId' --output text)

# Replace placeholders in config
sed -i '' "s/YOUR_EC2_KEY_NAME/streamtide-key/g" config/emr-cluster-config.json
sed -i '' "s/subnet-xxxxx/${SUBNET_ID}/g" config/emr-cluster-config.json
sed -i '' "s/\${BUCKET_PREFIX}/${BUCKET_PREFIX}/g" config/emr-cluster-config.json

# Launch cluster
aws emr create-cluster --cli-input-json file://config/emr-cluster-config.json

# Get cluster ID
export CLUSTER_ID=$(aws emr list-clusters --active \
  --query 'Clusters[?Name==`StreamTide-EMR-Cluster`].Id' --output text)

echo "Cluster ID: ${CLUSTER_ID}"

# Wait for cluster to be ready (takes ~10-15 minutes)
aws emr wait cluster-running --cluster-id ${CLUSTER_ID}
echo "Cluster is running!"

# Get master node public DNS
export MASTER_DNS=$(aws emr describe-cluster --cluster-id ${CLUSTER_ID} \
  --query 'Cluster.MasterPublicDnsName' --output text)

echo "Master node: ${MASTER_DNS}"
```

### 3.4 SSH into Master Node

```bash
ssh -i ~/.ssh/streamtide-key.pem hadoop@${MASTER_DNS}

# Once connected, verify installations
spark-submit --version
hadoop version
```

---

## Phase 4: Data Validation Pipeline

### 4.1 Create Validation Spark Job

Create: `spark-jobs/data_validation.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, count, when, isnan, isnull
from pyspark.sql.types import *
import sys

def create_spark_session():
    return SparkSession.builder \
        .appName("StreamTide-DataValidation") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def validate_schema(df):
    """Validate expected schema"""
    required_columns = [
        'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
        'passenger_count', 'trip_distance', 'PULocationID', 'DOLocationID',
        'fare_amount', 'tip_amount', 'total_amount'
    ]

    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    print(f"✓ Schema validation passed. Columns: {len(df.columns)}")
    return True

def data_quality_report(df, year_val):
    """Generate data quality metrics"""

    total_rows = df.count()
    print(f"\n{'='*60}")
    print(f"Data Quality Report - Year {year_val}")
    print(f"{'='*60}")
    print(f"Total Records: {total_rows:,}")

    # Null counts
    null_counts = df.select([
        count(when(isnull(c) | isnan(c), c)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()

    print(f"\nNull/NaN Counts:")
    for col_name, null_count in null_counts.items():
        if null_count > 0:
            pct = (null_count / total_rows) * 100
            print(f"  {col_name}: {null_count:,} ({pct:.2f}%)")

    # Range validation
    print(f"\nRange Validations:")
    invalid_fare = df.filter(col('fare_amount') <= 0).count()
    invalid_distance = df.filter(col('trip_distance') <= 0).count()
    invalid_passengers = df.filter(
        (col('passenger_count') <= 0) | (col('passenger_count') > 8)
    ).count()

    print(f"  Invalid fare (<= 0): {invalid_fare:,}")
    print(f"  Invalid distance (<= 0): {invalid_distance:,}")
    print(f"  Invalid passengers: {invalid_passengers:,}")

    return {
        'total_rows': total_rows,
        'null_counts': null_counts,
        'invalid_fare': invalid_fare,
        'invalid_distance': invalid_distance,
        'invalid_passengers': invalid_passengers
    }

def clean_data(df):
    """Clean and filter data"""

    print("\nCleaning data...")

    # Remove nulls in critical columns
    df_clean = df.dropna(subset=[
        'tpep_pickup_datetime', 'tpep_dropoff_datetime',
        'PULocationID', 'DOLocationID', 'fare_amount'
    ])

    # Filter valid ranges
    df_clean = df_clean.filter(
        (col('fare_amount') > 0) & (col('fare_amount') < 500) &
        (col('trip_distance') > 0) & (col('trip_distance') < 100) &
        (col('passenger_count') > 0) & (col('passenger_count') <= 8) &
        (col('total_amount') > 0) & (col('total_amount') < 1000)
    )

    # Add derived columns
    df_clean = df_clean.withColumn('year', year('tpep_pickup_datetime'))
    df_clean = df_clean.withColumn('month', month('tpep_pickup_datetime'))

    rows_removed = df.count() - df_clean.count()
    print(f"Rows removed: {rows_removed:,}")
    print(f"Clean rows: {df_clean.count():,}")

    return df_clean

def main(input_path, output_path, year_val):
    spark = create_spark_session()

    print(f"\n{'='*60}")
    print(f"Processing Year: {year_val}")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"{'='*60}\n")

    # Read data
    df = spark.read.parquet(input_path)

    # Validate schema
    validate_schema(df)

    # Generate quality report
    report = data_quality_report(df, year_val)

    # Clean data
    df_clean = clean_data(df)

    # Write cleaned data partitioned by year and month
    df_clean.write \
        .partitionBy('year', 'month') \
        .mode('overwrite') \
        .parquet(output_path)

    print(f"\n✓ Data validation complete for {year_val}")
    print(f"✓ Clean data written to: {output_path}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: spark-submit data_validation.py <input_path> <output_path> <year>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    year_val = sys.argv[3]

    main(input_path, output_path, year_val)
```

### 4.2 Upload and Run Validation Job

```bash
# Upload script to S3
aws s3 cp spark-jobs/data_validation.py \
  s3://${BUCKET_PREFIX}-logs/spark-jobs/

# SSH into EMR master node
ssh -i ~/.ssh/streamtide-key.pem hadoop@${MASTER_DNS}

# Run validation for 2020
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 10G \
  --executor-cores 4 \
  --num-executors 6 \
  s3://${BUCKET_PREFIX}-logs/spark-jobs/data_validation.py \
  s3://${BUCKET_PREFIX}-raw/raw-data/yellow_tripdata_2020-*.parquet \
  s3://${BUCKET_PREFIX}-processed/clean-data/year=2020/ \
  2020

# Run validation for 2025
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 10G \
  --executor-cores 4 \
  --num-executors 6 \
  s3://${BUCKET_PREFIX}-logs/spark-jobs/data_validation.py \
  s3://${BUCKET_PREFIX}-raw/raw-data/yellow_tripdata_2025-*.parquet \
  s3://${BUCKET_PREFIX}-processed/clean-data/year=2025/ \
  2025
```

### 4.3 Verify Cleaned Data

```bash
# Check output
aws s3 ls s3://${BUCKET_PREFIX}-processed/clean-data/ --recursive --human-readable

# Sample the data
spark-shell

# In Spark shell:
val df = spark.read.parquet("s3://${BUCKET_PREFIX}-processed/clean-data/")
df.printSchema()
df.groupBy("year").count().show()
```

---

## Phase 5: Ground Truth Baseline (MapReduce)

### 5.1 Create MapReduce Job for Ground Truth

Create: `mapreduce-jobs/ground_truth_mapper.py`

```python
#!/usr/bin/env python3
import sys
import json
from datetime import datetime

def parse_line(line):
    """Parse Parquet-based input (simplified for demo)"""
    try:
        # In real implementation, use PyArrow to read Parquet
        # For now, assuming CSV-like format for demonstration
        fields = line.strip().split(',')

        pickup_dt = datetime.fromisoformat(fields[1])
        fare = float(fields[10])
        distance = float(fields[4])
        duration = (datetime.fromisoformat(fields[2]) - pickup_dt).seconds / 60.0

        return {
            'hour': pickup_dt.hour,
            'day_of_week': pickup_dt.weekday(),
            'fare': fare,
            'distance': distance,
            'duration': duration,
            'pu_location': int(fields[7]),
            'do_location': int(fields[8])
        }
    except:
        return None

for line in sys.stdin:
    data = parse_line(line)
    if data:
        # Emit multiple key-value pairs for different aggregations

        # Temporal patterns
        print(f"hour:{data['hour']}\t{json.dumps({'fare': data['fare'], 'distance': data['distance']})}")

        # Fare distribution
        print(f"fare_bucket:{int(data['fare']//10)}\t{data['fare']}")

        # Location patterns
        print(f"location:{data['pu_location']}\t1")

        # Duration stats
        print(f"duration_stats\t{data['duration']}")
```

Create: `mapreduce-jobs/ground_truth_reducer.py`

```python
#!/usr/bin/env python3
import sys
import json
from collections import defaultdict
import statistics

current_key = None
values = []

def compute_stats(nums):
    """Compute statistical measures"""
    if not nums:
        return {}

    return {
        'count': len(nums),
        'mean': statistics.mean(nums),
        'median': statistics.median(nums),
        'stdev': statistics.stdev(nums) if len(nums) > 1 else 0,
        'min': min(nums),
        'max': max(nums),
        'p25': statistics.quantiles(nums, n=4)[0] if len(nums) >= 4 else 0,
        'p75': statistics.quantiles(nums, n=4)[2] if len(nums) >= 4 else 0,
        'p95': statistics.quantiles(nums, n=20)[18] if len(nums) >= 20 else 0
    }

for line in sys.stdin:
    key, value = line.strip().split('\t', 1)

    if current_key != key:
        if current_key:
            # Process accumulated values
            if current_key.startswith('hour:'):
                fares = [json.loads(v)['fare'] for v in values]
                stats = compute_stats(fares)
                print(f"{current_key}\t{json.dumps(stats)}")

            elif current_key.startswith('duration_stats'):
                durations = [float(v) for v in values]
                stats = compute_stats(durations)
                print(f"{current_key}\t{json.dumps(stats)}")

            elif current_key.startswith('location:'):
                count = len(values)
                print(f"{current_key}\t{count}")

        current_key = key
        values = []

    values.append(value)

# Don't forget last key
if current_key:
    if current_key.startswith('hour:'):
        fares = [json.loads(v)['fare'] for v in values]
        stats = compute_stats(fares)
        print(f"{current_key}\t{json.dumps(stats)}")
```

### 5.2 Better Approach: Use Spark for Ground Truth

Create: `spark-jobs/ground_truth_baseline.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_spark_session():
    return SparkSession.builder \
        .appName("StreamTide-GroundTruth") \
        .getOrCreate()

def compute_ground_truth(df, year_val, output_path):
    """Compute comprehensive ground truth statistics"""

    print(f"\nComputing ground truth for {year_val}...")

    # Add time-based features
    df = df.withColumn('hour', hour('tpep_pickup_datetime'))
    df = df.withColumn('day_of_week', dayofweek('tpep_pickup_datetime'))
    df = df.withColumn('trip_duration_min',
        (unix_timestamp('tpep_dropoff_datetime') -
         unix_timestamp('tpep_pickup_datetime')) / 60.0)

    # 1. Temporal patterns (hourly aggregates)
    hourly_stats = df.groupBy('hour').agg(
        count('*').alias('trip_count'),
        mean('fare_amount').alias('avg_fare'),
        stddev('fare_amount').alias('std_fare'),
        mean('trip_distance').alias('avg_distance'),
        mean('trip_duration_min').alias('avg_duration'),
        expr('percentile_approx(fare_amount, 0.5)').alias('median_fare'),
        expr('percentile_approx(fare_amount, 0.95)').alias('p95_fare')
    ).orderBy('hour')

    hourly_stats.write.mode('overwrite').json(
        f"{output_path}/hourly_stats/year={year_val}/"
    )

    # 2. Fare distribution
    fare_dist = df.selectExpr(
        'percentile_approx(fare_amount, array(0.25, 0.5, 0.75, 0.90, 0.95, 0.99)) as fare_percentiles',
        'mean(fare_amount) as mean_fare',
        'stddev(fare_amount) as std_fare',
        'min(fare_amount) as min_fare',
        'max(fare_amount) as max_fare'
    )

    fare_dist.write.mode('overwrite').json(
        f"{output_path}/fare_distribution/year={year_val}/"
    )

    # 3. Spatial analytics (zone-based demand)
    zone_demand = df.groupBy('PULocationID').agg(
        count('*').alias('pickup_count'),
        mean('fare_amount').alias('avg_fare_from_zone')
    ).orderBy(desc('pickup_count'))

    zone_demand.write.mode('overwrite').json(
        f"{output_path}/zone_demand/year={year_val}/"
    )

    # 4. Trip duration stats
    duration_stats = df.selectExpr(
        'count(*) as total_trips',
        'mean(trip_duration_min) as mean_duration',
        'stddev(trip_duration_min) as std_duration',
        'percentile_approx(trip_duration_min, array(0.5, 0.75, 0.95)) as duration_percentiles'
    )

    duration_stats.write.mode('overwrite').json(
        f"{output_path}/duration_stats/year={year_val}/"
    )

    # 5. Overall summary
    summary = df.selectExpr(
        'count(*) as total_trips',
        'count(distinct PULocationID) as unique_pickup_zones',
        'count(distinct DOLocationID) as unique_dropoff_zones',
        'sum(fare_amount) as total_revenue',
        'sum(trip_distance) as total_miles',
        'mean(passenger_count) as avg_passengers'
    )

    summary.write.mode('overwrite').json(
        f"{output_path}/summary/year={year_val}/"
    )

    print(f"✓ Ground truth computed and saved to {output_path}")

    # Display sample results
    print("\n" + "="*60)
    print(f"Ground Truth Summary - {year_val}")
    print("="*60)
    hourly_stats.show(24, truncate=False)

def main():
    spark = create_spark_session()

    bucket = "YOUR_BUCKET_PREFIX"  # Replace with actual bucket

    # Process 2020
    df_2020 = spark.read.parquet(f"s3://{bucket}-processed/clean-data/year=2020/")
    compute_ground_truth(df_2020, 2020, f"s3://{bucket}-results/ground-truth")

    # Process 2025
    df_2025 = spark.read.parquet(f"s3://{bucket}-processed/clean-data/year=2025/")
    compute_ground_truth(df_2025, 2025, f"s3://{bucket}-results/ground-truth")

    spark.stop()

if __name__ == "__main__":
    main()
```

### 5.3 Run Ground Truth Job

```bash
# Upload script
aws s3 cp spark-jobs/ground_truth_baseline.py \
  s3://${BUCKET_PREFIX}-logs/spark-jobs/

# Edit script to replace bucket name
sed "s/YOUR_BUCKET_PREFIX/${BUCKET_PREFIX}/g" spark-jobs/ground_truth_baseline.py > /tmp/ground_truth_baseline.py
aws s3 cp /tmp/ground_truth_baseline.py s3://${BUCKET_PREFIX}-logs/spark-jobs/ground_truth_baseline.py

# Run on EMR
ssh -i ~/.ssh/streamtide-key.pem hadoop@${MASTER_DNS}

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 10G \
  --num-executors 6 \
  s3://${BUCKET_PREFIX}-logs/spark-jobs/ground_truth_baseline.py
```

---

## Phase 6: Amazon MSK Setup

### 6.1 Option A: Create MSK Cluster via AWS Console (Web UI)

**Step-by-Step Web UI Setup:**

1. **Navigate to MSK Console**
   - Go to AWS Console → Services → Amazon MSK
   - Click "Create cluster"

2. **Choose Creation Method**
   - Select "Custom create" (not Quick create)
   - Click "Next"

3. **General Cluster Properties**
   - Cluster name: `streamtide-kafka`
   - Kafka version: `3.5.1`
   - Broker type: `kafka.m5.large`

4. **Networking**
   - VPC: Select the **same VPC** as your EMR cluster
   - Availability Zones: Select 3 different AZs
   - Subnets: Pick one subnet per AZ (3 total)
   - Security groups: Use default or create new allowing:
     - Inbound: Port 9098 (SASL/IAM) from EMR security group

5. **Storage**
   - Storage per broker: `100 GB`
   - Storage type: `gp3` (General Purpose SSD)

6. **Configuration**
   - Use default Kafka configuration (or customize if needed)
   - Number of broker nodes: 3

7. **Security**
   - Encryption in transit:
     - ✅ TLS encryption between clients and brokers
   - Access control methods:
     - ✅ IAM role-based authentication
     - ✅ Unauthenticated access (uncheck this)
   - Client authentication:
     - Select: `IAM role-based authentication`

8. **Monitoring**
   - ✅ Enable Amazon CloudWatch metrics (Basic monitoring)
   - Optionally enable enhanced monitoring

9. **Tags**
   - Key: `Project`, Value: `StreamTide`
   - Key: `Environment`, Value: `Development`

10. **Review and Create**
    - Review all settings
    - Click "Create cluster"
    - Wait 15-20 minutes for cluster creation

11. **Get Connection Details**
    - Once cluster status is "Active"
    - Click on cluster name
    - Go to "View client information"
    - Copy "Bootstrap servers" (IAM authentication endpoints)
    - Save this for later use

**Important Security Group Settings:**
- MSK broker security group must allow:
  - Port 9098 (SASL_SSL with IAM) from EMR master/core security groups

- EMR security groups must allow:
  - Outbound to MSK on port 9098

### 6.2 Option B: Create MSK Cluster via CLI

```bash
# Get VPC and subnet info from EMR cluster
export VPC_ID=$(aws emr describe-cluster --cluster-id ${CLUSTER_ID} \
  --query 'Cluster.Ec2InstanceAttributes.Ec2SubnetId' --output text | \
  xargs -I {} aws ec2 describe-subnets --subnet-ids {} \
  --query 'Subnets[0].VpcId' --output text)

# Get 3 subnets in different AZs
export SUBNETS=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=${VPC_ID}" \
  --query 'Subnets[0:3].SubnetId' --output json)

# Create MSK cluster
aws kafka create-cluster \
  --cluster-name streamtide-kafka \
  --broker-node-group-info "{
    \"InstanceType\": \"kafka.m5.large\",
    \"ClientSubnets\": ${SUBNETS},
    \"StorageInfo\": {
      \"EbsStorageInfo\": {
        \"VolumeSize\": 100
      }
    }
  }" \
  --kafka-version "3.5.1" \
  --number-of-broker-nodes 3 \
  --encryption-info "{
    \"EncryptionInTransit\": {
      \"ClientBroker\": \"TLS\",
      \"InCluster\": true
    }
  }" \
  --client-authentication "{
    \"Sasl\": {
      \"Iam\": {
        \"Enabled\": true
      }
    }
  }"

# Get cluster ARN
export MSK_ARN=$(aws kafka list-clusters \
  --query 'ClusterInfoList[?ClusterName==`streamtide-kafka`].ClusterArn' \
  --output text)

echo "MSK Cluster ARN: ${MSK_ARN}"

# Wait for cluster to be active
aws kafka wait cluster-running --cluster-arn ${MSK_ARN}
```

### 6.3 Get Bootstrap Servers (Both Methods)

```bash
# Get bootstrap broker string
export BOOTSTRAP_SERVERS=$(aws kafka get-bootstrap-brokers \
  --cluster-arn ${MSK_ARN} \
  --query 'BootstrapBrokerStringSaslIam' \
  --output text)

echo "Bootstrap Servers: ${BOOTSTRAP_SERVERS}"
```

### 6.4 Create Kafka Topics

SSH into EMR master and create topics:

```bash
ssh -i ~/.ssh/streamtide-key.pem hadoop@${MASTER_DNS}

# Set bootstrap servers variable
export BOOTSTRAP_SERVERS="<paste-from-above>"

# Download Kafka client
wget https://archive.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz
tar -xzf kafka_2.13-3.5.1.tgz
cd kafka_2.13-3.5.1

# Create topics
bin/kafka-topics.sh --create \
  --bootstrap-server ${BOOTSTRAP_SERVERS} \
  --topic taxi-trips-2020 \
  --partitions 12 \
  --replication-factor 3 \
  --command-config /home/hadoop/client.properties

bin/kafka-topics.sh --create \
  --bootstrap-server ${BOOTSTRAP_SERVERS} \
  --topic taxi-trips-2025 \
  --partitions 12 \
  --replication-factor 3 \
  --command-config /home/hadoop/client.properties

# Verify
bin/kafka-topics.sh --list \
  --bootstrap-server ${BOOTSTRAP_SERVERS} \
  --command-config /home/hadoop/client.properties
```

Create `/home/hadoop/client.properties`:
```properties
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

---

## Phase 7: Streaming Algorithms

### 7.1 Kafka Producer (Historical Replay)

Create: `streaming/kafka_producer.py`

```python
from kafka import KafkaProducer
from pyspark.sql import SparkSession
import json
import time
import sys

def create_producer(bootstrap_servers):
    """Create Kafka producer with IAM auth"""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        security_protocol='SASL_SSL',
        sasl_mechanism='AWS_MSK_IAM',
        sasl_oauth_token_provider='software.amazon.msk.auth.iam.IAMClientCallbackHandler',
        compression_type='snappy',
        batch_size=16384,
        linger_ms=10
    )

def replay_data_to_kafka(input_path, topic, bootstrap_servers, rate_per_sec=1000):
    """Read Parquet data and stream to Kafka"""

    spark = SparkSession.builder \
        .appName("KafkaProducer") \
        .getOrCreate()

    producer = create_producer(bootstrap_servers)

    print(f"Reading data from {input_path}")
    df = spark.read.parquet(input_path).limit(100000)  # Limit for demo

    records_sent = 0
    start_time = time.time()

    for row in df.toLocalIterator():
        message = {
            'pickup_datetime': str(row['tpep_pickup_datetime']),
            'dropoff_datetime': str(row['tpep_dropoff_datetime']),
            'passenger_count': row['passenger_count'],
            'trip_distance': float(row['trip_distance']),
            'PULocationID': row['PULocationID'],
            'DOLocationID': row['DOLocationID'],
            'fare_amount': float(row['fare_amount']),
            'tip_amount': float(row['tip_amount']),
            'total_amount': float(row['total_amount']),
            'trip_id': f"{row['VendorID']}_{row['tpep_pickup_datetime']}"
        }

        producer.send(topic, value=message)
        records_sent += 1

        # Rate limiting
        if records_sent % rate_per_sec == 0:
            elapsed = time.time() - start_time
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            start_time = time.time()

        if records_sent % 10000 == 0:
            print(f"Sent {records_sent} records to {topic}")

    producer.flush()
    producer.close()

    print(f"✓ Total records sent: {records_sent}")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python kafka_producer.py <input_path> <topic> <bootstrap_servers>")
        sys.exit(1)

    replay_data_to_kafka(sys.argv[1], sys.argv[2], sys.argv[3])
```

### 7.2 Reservoir Sampling Implementation

Create: `streaming/reservoir_sampling.py`

```python
import random
from collections import defaultdict
import json

class ReservoirSampler:
    """Reservoir Sampling implementation with stratification"""

    def __init__(self, sample_size=10000, strata_keys=None):
        self.sample_size = sample_size
        self.reservoir = []
        self.count = 0
        self.strata_keys = strata_keys or []
        self.strata_samples = defaultdict(list)
        self.strata_counts = defaultdict(int)

    def add(self, item):
        """Add item to reservoir using Algorithm R"""
        self.count += 1

        # First n items fill the reservoir
        if len(self.reservoir) < self.sample_size:
            self.reservoir.append(item)
        else:
            # Random replacement with probability k/n
            j = random.randint(0, self.count - 1)
            if j < self.sample_size:
                self.reservoir[j] = item

        # Stratified sampling (by hour, zone, etc.)
        if self.strata_keys:
            for key in self.strata_keys:
                if key in item:
                    stratum = f"{key}:{item[key]}"
                    self.strata_counts[stratum] += 1

                    # Simple sampling per stratum
                    if len(self.strata_samples[stratum]) < 100:
                        self.strata_samples[stratum].append(item)

    def get_sample(self):
        """Return current reservoir sample"""
        return self.reservoir

    def get_statistics(self):
        """Compute statistics from sample"""
        if not self.reservoir:
            return {}

        fares = [item['fare_amount'] for item in self.reservoir]
        distances = [item['trip_distance'] for item in self.reservoir]

        return {
            'sample_size': len(self.reservoir),
            'total_processed': self.count,
            'avg_fare': sum(fares) / len(fares),
            'avg_distance': sum(distances) / len(distances),
            'min_fare': min(fares),
            'max_fare': max(fares),
            'strata_counts': dict(self.strata_counts)
        }

def main():
    """Demo of reservoir sampling"""
    sampler = ReservoirSampler(sample_size=1000, strata_keys=['hour'])

    # Simulate stream
    for i in range(100000):
        item = {
            'fare_amount': random.uniform(5, 50),
            'trip_distance': random.uniform(0.5, 20),
            'hour': random.randint(0, 23)
        }
        sampler.add(item)

    stats = sampler.get_statistics()
    print(json.dumps(stats, indent=2))

if __name__ == "__main__":
    main()
```

### 7.3 Bloom Filter Implementation

Create: `streaming/bloom_filter.py`

```python
import mmh3
import math
from bitarray import bitarray

class BloomFilter:
    """Bloom Filter for duplicate detection"""

    def __init__(self, expected_items=10000000, false_positive_rate=0.01):
        """
        Initialize Bloom Filter

        Args:
            expected_items: Expected number of items
            false_positive_rate: Desired false positive rate (0.01 = 1%)
        """
        # Calculate optimal size and hash functions
        self.size = self._optimal_size(expected_items, false_positive_rate)
        self.hash_count = self._optimal_hash_count(self.size, expected_items)

        # Create bit array
        self.bit_array = bitarray(self.size)
        self.bit_array.setall(0)

        # Stats
        self.items_added = 0
        self.duplicates_detected = 0

        print(f"Bloom Filter initialized:")
        print(f"  Bit array size: {self.size:,} bits ({self.size/8/1024/1024:.2f} MB)")
        print(f"  Hash functions: {self.hash_count}")
        print(f"  Expected items: {expected_items:,}")
        print(f"  Target FPR: {false_positive_rate*100}%")

    def _optimal_size(self, n, p):
        """Calculate optimal bit array size"""
        m = -(n * math.log(p)) / (math.log(2) ** 2)
        return int(m)

    def _optimal_hash_count(self, m, n):
        """Calculate optimal number of hash functions"""
        k = (m / n) * math.log(2)
        return int(k)

    def _hash(self, item, seed):
        """Generate hash for item with given seed"""
        return mmh3.hash(item, seed) % self.size

    def add(self, item):
        """Add item to Bloom filter"""
        for i in range(self.hash_count):
            index = self._hash(item, i)
            self.bit_array[index] = 1
        self.items_added += 1

    def check(self, item):
        """Check if item might be in set"""
        for i in range(self.hash_count):
            index = self._hash(item, i)
            if self.bit_array[index] == 0:
                return False  # Definitely not in set
        return True  # Might be in set

    def add_and_check(self, item):
        """Check if duplicate, then add"""
        is_duplicate = self.check(item)
        if is_duplicate:
            self.duplicates_detected += 1
        else:
            self.add(item)
        return is_duplicate

    def get_stats(self):
        """Get Bloom filter statistics"""
        bits_set = self.bit_array.count(1)
        fill_ratio = bits_set / self.size

        # Estimate false positive rate
        actual_fpr = (1 - math.exp(-self.hash_count * self.items_added / self.size)) ** self.hash_count

        return {
            'items_added': self.items_added,
            'duplicates_detected': self.duplicates_detected,
            'bits_set': bits_set,
            'fill_ratio': fill_ratio,
            'estimated_fpr': actual_fpr,
            'memory_mb': self.size / 8 / 1024 / 1024
        }

def main():
    """Demo Bloom filter"""
    bf = BloomFilter(expected_items=1000000, false_positive_rate=0.01)

    # Add unique items
    for i in range(100000):
        bf.add(f"trip_{i}")

    # Test duplicates
    duplicates = 0
    for i in range(50000, 150000):  # 50k overlap
        if bf.check(f"trip_{i}"):
            duplicates += 1

    print(f"\nDuplicates detected: {duplicates}")
    print(f"Stats: {bf.get_stats()}")

if __name__ == "__main__":
    main()
```

### 7.4 Flajolet-Martin Algorithm Implementation

Create: `streaming/flajolet_martin.py`

```python
import hashlib
import math

class FlajoletMartin:
    """Flajolet-Martin algorithm for distinct count estimation"""

    def __init__(self, num_hash_groups=32):
        """
        Initialize FM algorithm with multiple hash groups for accuracy

        Args:
            num_hash_groups: Number of independent hash functions (median of estimates)
        """
        self.num_hash_groups = num_hash_groups
        self.max_trailing_zeros = [0] * num_hash_groups
        self.items_processed = 0

    def _hash(self, element, seed):
        """Generate hash value for element with given seed"""
        hash_input = f"{element}_{seed}".encode('utf-8')
        return int(hashlib.md5(hash_input).hexdigest(), 16)

    def _count_trailing_zeros(self, n):
        """Count number of trailing zeros in binary representation"""
        if n == 0:
            return 64  # Max for 64-bit hash

        count = 0
        while (n & 1) == 0:
            count += 1
            n >>= 1
        return count

    def add_element(self, element):
        """Add element to the stream"""
        self.items_processed += 1

        for i in range(self.num_hash_groups):
            hash_val = self._hash(element, i)
            trailing_zeros = self._count_trailing_zeros(hash_val)
            self.max_trailing_zeros[i] = max(self.max_trailing_zeros[i], trailing_zeros)

    def estimate_distinct_count(self):
        """Estimate number of distinct elements using median of estimates"""
        # Calculate estimate for each hash group
        estimates = [2 ** tz for tz in self.max_trailing_zeros]

        # Take median to reduce variance
        estimates.sorted()
        median_estimate = estimates[len(estimates) // 2]

        # Apply correction factor φ ≈ 0.77351
        corrected_estimate = int(median_estimate / 0.77351)

        return corrected_estimate

    def get_statistics(self):
        """Get detailed statistics"""
        return {
            'items_processed': self.items_processed,
            'distinct_estimate': self.estimate_distinct_count(),
            'hash_groups': self.num_hash_groups,
            'max_trailing_zeros': self.max_trailing_zeros
        }

# Demo usage
if __name__ == "__main__":
    fm = FlajoletMartin(num_hash_groups=32)

    # Simulate stream with duplicates
    for i in range(100000):
        # Add zones (265 unique zones, but 100K total trips)
        zone = i % 265
        fm.add_element(f"zone_{zone}")

    stats = fm.get_statistics()
    print(f"Actual distinct zones: 265")
    print(f"FM Estimate: {stats['distinct_estimate']}")
    print(f"Error: {abs(stats['distinct_estimate'] - 265) / 265 * 100:.2f}%")
```

### 7.5 DGIM Algorithm Implementation

Create: `streaming/dgim.py`

```python
from collections import deque
import time

class DGIMBucket:
    """Bucket for DGIM algorithm"""
    def __init__(self, size, timestamp):
        self.size = size
        self.timestamp = timestamp

class DGIM:
    """DGIM algorithm for counting 1s in sliding window"""

    def __init__(self, window_size=3600):
        """
        Initialize DGIM with window size

        Args:
            window_size: Window size in seconds (default 1 hour)
        """
        self.window_size = window_size
        self.buckets = deque()

    def add_bit(self, bit):
        """Add a bit to the stream"""
        current_time = time.time()

        # Remove expired buckets outside the window
        while self.buckets and (current_time - self.buckets[0].timestamp) > self.window_size:
            self.buckets.popleft()

        if bit == 1:
            # Add new bucket of size 1
            self.buckets.append(DGIMBucket(1, current_time))
            self._merge_buckets()

    def _merge_buckets(self):
        """Merge buckets when there are 3+ of same size"""
        # Count buckets by size
        size_counts = {}
        for bucket in self.buckets:
            size_counts[bucket.size] = size_counts.get(bucket.size, 0) + 1

        # Find sizes with 3+ buckets
        for size in sorted(size_counts.keys()):
            if size_counts[size] >= 3:
                # Merge the two oldest buckets of this size
                merged_buckets = []
                buckets_to_merge = []

                for bucket in self.buckets:
                    if bucket.size == size and len(buckets_to_merge) < 2:
                        buckets_to_merge.append(bucket)
                    else:
                        merged_buckets.append(bucket)

                if len(buckets_to_merge) == 2:
                    # Create new bucket with double size, using newer timestamp
                    new_bucket = DGIMBucket(size * 2, buckets_to_merge[1].timestamp)
                    merged_buckets.append(new_bucket)

                self.buckets = deque(sorted(merged_buckets, key=lambda b: b.timestamp))
                break

    def count_ones(self):
        """Estimate number of 1s in the current window"""
        if not self.buckets:
            return 0

        # Sum all bucket sizes
        total = sum(bucket.size for bucket in self.buckets)

        # Subtract half of the oldest bucket size (error adjustment)
        if self.buckets:
            total -= self.buckets[0].size // 2

        return max(0, total)

    def get_statistics(self):
        """Get DGIM statistics"""
        return {
            'window_size': self.window_size,
            'num_buckets': len(self.buckets),
            'estimated_ones': self.count_ones(),
            'bucket_sizes': [b.size for b in self.buckets]
        }

# Demo usage
if __name__ == "__main__":
    dgim = DGIM(window_size=60)  # 1-minute window

    # Simulate binary stream (1 = trip occurred in that second)
    import random
    actual_count = 0

    for i in range(120):  # 2 minutes of data
        bit = random.choice([0, 0, 0, 1])  # 25% trip rate
        if bit == 1:
            if i >= 60:  # Only count last minute
                actual_count += 1

        dgim.add_bit(bit)
        time.sleep(0.01)  # Simulate real-time

    stats = dgim.get_statistics()
    print(f"Actual 1s in window: {actual_count}")
    print(f"DGIM Estimate: {stats['estimated_ones']}")
    print(f"Error: {abs(stats['estimated_ones'] - actual_count)}")
```

### 7.6 Computing Moments Implementation

Create: `streaming/moments.py`

```python
import math

class StreamingMoments:
    """Compute statistical moments in streaming fashion"""

    def __init__(self):
        self.n = 0
        self.S1 = 0.0  # Sum of x
        self.S2 = 0.0  # Sum of x²
        self.S3 = 0.0  # Sum of x³
        self.S4 = 0.0  # Sum of x⁴

    def add_value(self, x):
        """Add a value to the stream"""
        self.n += 1
        self.S1 += x
        self.S2 += x ** 2
        self.S3 += x ** 3
        self.S4 += x ** 4

    def mean(self):
        """First moment: Mean"""
        return self.S1 / self.n if self.n > 0 else 0

    def variance(self):
        """Second moment: Variance"""
        if self.n == 0:
            return 0
        mu = self.mean()
        return (self.S2 / self.n) - mu ** 2

    def std_dev(self):
        """Standard deviation"""
        return math.sqrt(self.variance())

    def skewness(self):
        """Third moment: Skewness (asymmetry)"""
        if self.n == 0:
            return 0

        mu = self.mean()
        sigma = self.std_dev()

        if sigma == 0:
            return 0

        # Calculate third central moment
        m3 = (self.S3 / self.n) - 3 * mu * (self.S2 / self.n) + 2 * mu ** 3
        return m3 / (sigma ** 3)

    def kurtosis(self):
        """Fourth moment: Kurtosis (tail heaviness)"""
        if self.n == 0:
            return 0

        mu = self.mean()
        sigma = self.std_dev()

        if sigma == 0:
            return 0

        # Calculate fourth central moment
        m4 = (self.S4 / self.n) - 4 * mu * (self.S3 / self.n) + \
             6 * mu ** 2 * (self.S2 / self.n) - 3 * mu ** 4

        return m4 / (sigma ** 4)

    def get_all_moments(self):
        """Get all statistical moments"""
        return {
            'count': self.n,
            'mean': self.mean(),
            'variance': self.variance(),
            'std_dev': self.std_dev(),
            'skewness': self.skewness(),
            'kurtosis': self.kurtosis()
        }

# Stratified moments for multiple dimensions
class StratifiedMoments:
    """Track moments for different strata (hour, zone, etc.)"""

    def __init__(self):
        self.strata_moments = {}

    def add_value(self, value, stratum_key):
        """Add value to specific stratum"""
        if stratum_key not in self.strata_moments:
            self.strata_moments[stratum_key] = StreamingMoments()

        self.strata_moments[stratum_key].add_value(value)

    def get_stratum_stats(self, stratum_key):
        """Get statistics for a specific stratum"""
        if stratum_key in self.strata_moments:
            return self.strata_moments[stratum_key].get_all_moments()
        return None

    def get_all_strata_stats(self):
        """Get statistics for all strata"""
        return {
            key: moments.get_all_moments()
            for key, moments in self.strata_moments.items()
        }

# Demo usage
if __name__ == "__main__":
    import random

    # Test basic moments
    moments = StreamingMoments()

    # Simulate fare amounts
    for _ in range(10000):
        fare = random.gauss(25, 10)  # Mean $25, StdDev $10
        moments.add_value(fare)

    stats = moments.get_all_moments()
    print("Fare Statistics:")
    print(f"  Mean: ${stats['mean']:.2f}")
    print(f"  Std Dev: ${stats['std_dev']:.2f}")
    print(f"  Skewness: {stats['skewness']:.3f}")
    print(f"  Kurtosis: {stats['kurtosis']:.3f}")

    # Test stratified moments
    strat_moments = StratifiedMoments()

    for _ in range(10000):
        hour = random.randint(0, 23)
        fare = random.gauss(25 + hour * 0.5, 10)  # Fare increases slightly by hour
        strat_moments.add_value(fare, f"hour_{hour}")

    print("\nPeak Hour (17:00) vs Off-Peak (3:00):")
    peak_stats = strat_moments.get_stratum_stats("hour_17")
    offpeak_stats = strat_moments.get_stratum_stats("hour_3")
    print(f"  Peak Mean: ${peak_stats['mean']:.2f}")
    print(f"  Off-Peak Mean: ${offpeak_stats['mean']:.2f}")
```

### 7.7 Graph Stream Algorithms Implementation

Create: `streaming/graph_streams.py`

```python
from collections import defaultdict
import random

class GraphStreamAnalyzer:
    """Analyze graph properties in streaming fashion"""

    def __init__(self, edge_sample_size=10000):
        """
        Initialize graph stream analyzer

        Args:
            edge_sample_size: Number of edges to keep in sample
        """
        self.edge_sample_size = edge_sample_size
        self.edge_sample = []
        self.degree_count = defaultdict(int)
        self.triangle_estimate = 0
        self.edge_count = 0

        # Adjacency lists (sampled)
        self.adjacency = defaultdict(set)

    def add_edge(self, source, dest):
        """Add edge to the graph stream"""
        self.edge_count += 1

        # Update degree counts
        self.degree_count[source] += 1
        self.degree_count[dest] += 1

        # Reservoir sampling for edges
        edge = (source, dest)
        if len(self.edge_sample) < self.edge_sample_size:
            self.edge_sample.append(edge)
            self.adjacency[source].add(dest)
            self.adjacency[dest].add(source)  # Treat as undirected
        else:
            # Random replacement
            j = random.randint(0, self.edge_count - 1)
            if j < self.edge_sample_size:
                # Remove old edge from adjacency
                old_edge = self.edge_sample[j]
                self.adjacency[old_edge[0]].discard(old_edge[1])
                self.adjacency[old_edge[1]].discard(old_edge[0])

                # Add new edge
                self.edge_sample[j] = edge
                self.adjacency[source].add(dest)
                self.adjacency[dest].add(source)

        # Update triangle count estimation
        self._update_triangles(source, dest)

    def _update_triangles(self, u, v):
        """Estimate triangle count when edge (u,v) is added"""
        # Check for common neighbors in sampled graph
        neighbors_u = self.adjacency.get(u, set())
        neighbors_v = self.adjacency.get(v, set())

        # Each common neighbor forms a triangle with u and v
        common_neighbors = neighbors_u & neighbors_v
        self.triangle_estimate += len(common_neighbors)

    def get_hub_zones(self, top_k=10):
        """Get top-k zones by degree (popularity)"""
        sorted_zones = sorted(
            self.degree_count.items(),
            key=lambda x: x[1],
            reverse=True
        )
        return sorted_zones[:top_k]

    def estimate_clustering_coefficient(self):
        """Estimate global clustering coefficient"""
        if self.triangle_estimate == 0:
            return 0.0

        # Clustering = (3 * triangles) / connected triples
        # Approximation using sampled graph
        total_degree = sum(self.degree_count.values())
        possible_triples = total_degree * (total_degree - 1) / 2

        if possible_triples == 0:
            return 0.0

        return (3 * self.triangle_estimate) / possible_triples

    def get_degree_distribution(self):
        """Get degree distribution statistics"""
        degrees = list(self.degree_count.values())
        if not degrees:
            return {}

        return {
            'min_degree': min(degrees),
            'max_degree': max(degrees),
            'avg_degree': sum(degrees) / len(degrees),
            'num_nodes': len(self.degree_count)
        }

    def get_statistics(self):
        """Get all graph statistics"""
        return {
            'total_edges': self.edge_count,
            'sampled_edges': len(self.edge_sample),
            'triangle_estimate': self.triangle_estimate,
            'clustering_coefficient': self.estimate_clustering_coefficient(),
            'degree_distribution': self.get_degree_distribution(),
            'top_hubs': self.get_hub_zones(10)
        }

# Demo usage
if __name__ == "__main__":
    graph = GraphStreamAnalyzer(edge_sample_size=1000)

    # Simulate trip network (zones as nodes, trips as edges)
    for _ in range(10000):
        # Random trips between 265 zones
        pickup = random.randint(1, 265)
        dropoff = random.randint(1, 265)

        if pickup != dropoff:
            graph.add_edge(pickup, dropoff)

    stats = graph.get_statistics()
    print("Graph Stream Statistics:")
    print(f"  Total edges: {stats['total_edges']:,}")
    print(f"  Triangle estimate: {stats['triangle_estimate']:,}")
    print(f"  Clustering coefficient: {stats['clustering_coefficient']:.4f}")
    print(f"  Avg degree: {stats['degree_distribution']['avg_degree']:.2f}")
    print(f"\nTop 5 Hub Zones:")
    for zone, degree in stats['top_hubs'][:5]:
        print(f"    Zone {zone}: {degree} connections")
```

### 7.8 Integrated Spark Streaming Consumer (Updated)

Create: `streaming/spark_streaming_consumer.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from reservoir_sampling import ReservoirSampler
from bloom_filter import BloomFilter
from flajolet_martin import FlajoletMartin
from dgim import DGIM
from moments import StratifiedMoments
from graph_streams import GraphStreamAnalyzer
import json

def create_spark_session():
    return SparkSession.builder \
        .appName("StreamTide-Complete-Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def process_stream(bootstrap_servers, topic, checkpoint_path, output_path):
    spark = create_spark_session()

    # Define schema for Kafka messages
    schema = StructType([
        StructField("pickup_datetime", StringType()),
        StructField("dropoff_datetime", StringType()),
        StructField("passenger_count", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("fare_amount", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("trip_id", StringType())
    ])

    # Initialize all streaming algorithms
    reservoir = ReservoirSampler(sample_size=100000, strata_keys=['hour'])
    bloom = BloomFilter(expected_items=10000000, false_positive_rate=0.01)
    fm_zones = FlajoletMartin(num_hash_groups=32)
    fm_passengers = FlajoletMartin(num_hash_groups=32)
    dgim = DGIM(window_size=3600)
    moments = StratifiedMoments()
    graph = GraphStreamAnalyzer(edge_sample_size=10000)

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
        .load()

    # Parse JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Add derived fields
    enriched_df = parsed_df \
        .withColumn("hour", hour(col("pickup_datetime"))) \
        .withColumn("processing_time", current_timestamp())

    # Custom streaming processing with foreachBatch
    def process_batch(batch_df, epoch_id):
        """Process each micro-batch with all algorithms"""

        batch_data = batch_df.collect()

        for row in batch_data:
            trip = row.asDict()

            # Reservoir Sampling
            reservoir.add(trip)

            # Bloom Filter (duplicate detection)
            is_duplicate = bloom.add_and_check(trip['trip_id'])

            # Flajolet-Martin (distinct counts)
            fm_zones.add_element(trip['PULocationID'])
            fm_passengers.add_element(trip['passenger_count'])

            # DGIM (trip counts in sliding window)
            dgim.add_bit(1)  # Each trip = 1

            # Computing Moments (fare statistics by hour)
            moments.add_value(trip['fare_amount'], f"hour_{trip['hour']}")

            # Graph Streams (zone network)
            graph.add_edge(trip['PULocationID'], trip['DOLocationID'])

        # Aggregate results for this batch
        results = {
            'epoch_id': epoch_id,
            'batch_size': len(batch_data),

            # Reservoir Sampling stats
            'reservoir_stats': reservoir.get_statistics(),

            # Bloom Filter stats
            'bloom_stats': bloom.get_stats(),

            # Flajolet-Martin estimates
            'distinct_zones_estimate': fm_zones.estimate_distinct_count(),
            'distinct_passengers_estimate': fm_passengers.estimate_distinct_count(),

            # DGIM count
            'trips_in_last_hour': dgim.count_ones(),

            # Moments statistics (sample hours)
            'peak_hour_stats': moments.get_stratum_stats('hour_17'),
            'offpeak_hour_stats': moments.get_stratum_stats('hour_3'),

            # Graph statistics
            'graph_stats': graph.get_statistics()
        }

        # Save results to S3
        results_df = spark.createDataFrame([results])
        results_df.write.mode("append").json(
            f"{output_path}/algorithm-results/epoch-{epoch_id}/"
        )

        print(f"\nEpoch {epoch_id} Results:")
        print(f"  Processed: {results['batch_size']} trips")
        print(f"  Distinct zones (FM): {results['distinct_zones_estimate']}")
        print(f"  Trips in last hour (DGIM): {results['trips_in_last_hour']}")
        print(f"  Bloom filter duplicates: {results['bloom_stats']['duplicates_detected']}")

    # Windowed aggregations for comparison
    windowed_stats = enriched_df \
        .withWatermark("processing_time", "10 minutes") \
        .groupBy(
            window("processing_time", "5 minutes"),
            "hour"
        ).agg(
            count("*").alias("trip_count"),
            mean("fare_amount").alias("avg_fare"),
            mean("trip_distance").alias("avg_distance"),
            expr("percentile_approx(fare_amount, 0.5)").alias("median_fare")
        )

    # Write streaming query with custom processing
    query1 = enriched_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", f"{checkpoint_path}/algorithms/") \
        .trigger(processingTime='30 seconds') \
        .start()

    query2 = windowed_stats.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"{output_path}/windowed-stats/") \
        .option("checkpointLocation", f"{checkpoint_path}/windowed/") \
        .trigger(processingTime='30 seconds') \
        .start()

    query1.awaitTermination()
    query2.awaitTermination()

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 5:
        print("Usage: spark-submit spark_streaming_consumer.py <bootstrap_servers> <topic> <checkpoint> <output>")
        sys.exit(1)

    process_stream(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
```

---

## Phase 8: Performance Evaluation

### 8.1 Accuracy Validation Script

Create: `evaluation/validate_accuracy.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json

def calculate_mae_rmse(ground_truth, sample_stats):
    """Calculate MAE and RMSE between ground truth and sample"""

    mae = abs(ground_truth['mean'] - sample_stats['mean'])
    mse = (ground_truth['mean'] - sample_stats['mean']) ** 2
    rmse = mse ** 0.5

    return {
        'mae': mae,
        'rmse': rmse,
        'mae_pct': (mae / ground_truth['mean']) * 100,
        'rmse_pct': (rmse / ground_truth['mean']) * 100
    }

def main(ground_truth_path, streaming_results_path, output_path):
    spark = SparkSession.builder \
        .appName("AccuracyValidation") \
        .getOrCreate()

    # Load ground truth
    gt = spark.read.json(f"{ground_truth_path}/hourly_stats/")

    # Load streaming results
    streaming = spark.read.parquet(streaming_results_path)

    # Join and compare
    comparison = gt.join(streaming, "hour", "inner")

    # Calculate errors
    error_df = comparison.withColumn(
        'fare_mae', abs(col('avg_fare_gt') - col('avg_fare_stream'))
    ).withColumn(
        'fare_mae_pct', (col('fare_mae') / col('avg_fare_gt')) * 100
    )

    # Aggregate results
    overall_accuracy = error_df.agg(
        mean('fare_mae').alias('mean_mae'),
        mean('fare_mae_pct').alias('mean_mae_pct'),
        max('fare_mae_pct').alias('max_error_pct')
    )

    # Save results
    overall_accuracy.write.mode('overwrite').json(output_path)

    # Display
    print("\n" + "="*60)
    print("Accuracy Validation Results")
    print("="*60)
    overall_accuracy.show(truncate=False)
    error_df.orderBy('hour').show(24)

    spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv[1], sys.argv[2], sys.argv[3])
```

---

## Phase 9: QuickSight Dashboards

### 9.1 Setup Athena for S3 Querying via Web UI

**Step-by-Step Athena Setup:**

1. **Navigate to Athena Console**
   - Go to AWS Console → Services → Athena
   - First time: Set up query result location
   - Click "Settings" → Query result location
   - Enter: `s3://YOUR-BUCKET-logs/athena-results/`
   - Save

2. **Create Database**
   - In Query Editor, run:
   ```sql
   CREATE DATABASE IF NOT EXISTS streamtide;
   ```

3. **Create Tables for Your Data**

**Option A: Via Athena Console (Manual)**

Navigate to Athena → Query Editor, then run:

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS streamtide;

-- Create table for ground truth
CREATE EXTERNAL TABLE streamtide.ground_truth_hourly (
    hour INT,
    trip_count BIGINT,
    avg_fare DOUBLE,
    std_fare DOUBLE,
    avg_distance DOUBLE,
    median_fare DOUBLE
)
PARTITIONED BY (year INT)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET-processed/ground-truth/hourly_stats/';

-- Load partitions
MSCK REPAIR TABLE streamtide.ground_truth_hourly;

-- Load partitions (important!)
MSCK REPAIR TABLE streamtide.ground_truth_hourly;

-- Create table for zone demand
CREATE EXTERNAL TABLE streamtide.zone_demand (
    PULocationID INT,
    pickup_count BIGINT,
    avg_fare_from_zone DOUBLE
)
PARTITIONED BY (year INT)
STORED AS JSON
LOCATION 's3://YOUR-BUCKET-results/ground-truth/zone_demand/';

MSCK REPAIR TABLE streamtide.zone_demand;

-- Create table for streaming results
CREATE EXTERNAL TABLE streamtide.streaming_results (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    hour INT,
    trip_count BIGINT,
    avg_fare DOUBLE,
    avg_distance DOUBLE,
    median_fare DOUBLE
)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET-checkpoints/streaming-output/';
```

4. **Run Test Queries**

```sql
-- Compare 2020 vs 2025 hourly patterns
SELECT
    year,
    hour,
    avg_fare,
    trip_count
FROM streamtide.ground_truth_hourly
WHERE year IN (2020, 2025)
ORDER BY year, hour;

-- Top pickup zones by year
SELECT
    year,
    PULocationID,
    pickup_count,
    avg_fare_from_zone
FROM streamtide.zone_demand
WHERE year IN (2020, 2025)
ORDER BY year, pickup_count DESC
LIMIT 20;

-- Fare distribution comparison
SELECT
    year,
    ROUND(avg_fare, 2) as avg_fare,
    ROUND(std_fare, 2) as std_fare,
    trip_count
FROM streamtide.ground_truth_hourly
WHERE year IN (2020, 2025)
GROUP BY year
ORDER BY year;
```

**Option B: Via AWS Glue Crawler (Automatic)**

1. Go to AWS Glue → Crawlers → Add crawler
2. Crawler name: `streamtide-crawler`
3. Data source: S3 path `s3://YOUR-BUCKET-results/`
4. IAM role: Create new or use existing with S3 access
5. Schedule: Run on demand
6. Output database: `streamtide`
7. Run crawler - it will automatically detect schema

### 9.2 Create QuickSight Dashboard via Web UI

**Step-by-Step QuickSight Setup:**

1. **Sign Up for QuickSight (if first time)**
   - Go to AWS Console → QuickSight
   - Click "Sign up for QuickSight"
   - Edition: Choose "Standard" (free trial available)
   - Authentication: Use IAM
   - QuickSight account name: `streamtide-analytics`
   - Email: Your email
   - Allow access to S3 buckets (select your buckets)
   - Finish setup

2. **Create Dataset from Athena**
   - QuickSight home → Datasets → New dataset
   - Choose: **Athena**
   - Data source name: `StreamTide-Athena`
   - Click "Create data source"
   - Database: Select `streamtide`
   - Tables: Select `ground_truth_hourly`
   - Click "Select"
   - Import to SPICE: Choose "Directly query your data"
   - Click "Visualize"

3. **Add More Datasets (Repeat for each table)**
   - `zone_demand`
   - `streaming_results`
   - `duration_stats`

4. **Create Analysis**
   - QuickSight home → Analyses → New analysis
   - Choose dataset: `ground_truth_hourly`
   - Click "Create analysis"

5. **Build Visualizations**

**Visual 1: Hourly Demand Comparison (2020 vs 2025)**
   - Click "Add" → Add visual
   - Visual type: Line chart
   - X-axis: Drag `hour` field
   - Value: Drag `trip_count` field
   - Color: Drag `year` field
   - Title: "Hourly Trip Demand: 2020 vs 2025"
   - Sort: X-axis ascending

**Visual 2: Average Fare by Hour**
   - Add new visual → Line chart
   - X-axis: `hour`
   - Value: `avg_fare`
   - Color: `year`
   - Title: "Average Fare by Hour"

**Visual 3: KPI - Total Trips**
   - Add new visual → KPI
   - Value: `trip_count` (aggregate: Sum)
   - Filter: year = 2025
   - Title: "Total Trips 2025"

**Visual 4: KPI - Average Fare**
   - Add new visual → KPI
   - Value: `avg_fare` (aggregate: Average)
   - Title: "Avg Fare 2025"

**Visual 5: Zone Heatmap**
   - Switch dataset to `zone_demand`
   - Add visual → Heat map
   - Rows: `PULocationID`
   - Columns: `year`
   - Values: `pickup_count`
   - Title: "Pickup Demand by Zone"

**Visual 6: Top 10 Zones Bar Chart**
   - Add visual → Horizontal bar chart
   - Y-axis: `PULocationID`
   - Value: `pickup_count`
   - Color: `year`
   - Filter: Top 10 by pickup_count
   - Title: "Top 10 Pickup Zones"

**Visual 7: Fare Distribution Box Plot**
   - Add visual → Box plot
   - Y-axis: `avg_fare`
   - Group/Color: `year`
   - Title: "Fare Distribution Comparison"

**Visual 8: Time Series - Streaming vs Ground Truth**
   - Add calculated field: `error_pct = (streaming_avg - ground_truth_avg) / ground_truth_avg * 100`
   - Visual type: Line chart
   - X-axis: `hour`
   - Value: `error_pct`
   - Title: "Streaming Accuracy (% Error)"

6. **Format Dashboard**
   - Click "Format" → Add title: "StreamTide: NYC Taxi Analytics Dashboard"
   - Add text boxes for descriptions
   - Arrange visuals in grid layout
   - Add filters: Year selector (2020/2025), Hour range slider

7. **Add Insights (ML-Powered)**
   - Click "+ Add" → Suggested insights
   - QuickSight will automatically detect:
     - Anomalies in trip counts
     - Trends over time
     - Top/bottom performers by zone
   - Select interesting insights to add

8. **Publish Dashboard**
   - Click "Share" → Publish dashboard
   - Dashboard name: "StreamTide Analytics"
   - Permissions: Add team members if needed
   - Click "Publish"

9. **Share Dashboard**
   - After publishing, click "Share"
   - Share with users: Add emails or IAM users
   - Permission: Viewer or Co-owner
   - Send invitation

---

## Phase 9.3: Key Insights & Analytics Queries

### Business Insights to Extract

This section provides SQL queries and analysis techniques to extract meaningful insights from your data.

#### 1. **Demand Surge Detection**

```sql
-- Identify hours with significant demand changes between 2020 and 2025
SELECT
    h2020.hour,
    h2020.trip_count as trips_2020,
    h2025.trip_count as trips_2025,
    ((h2025.trip_count - h2020.trip_count) * 100.0 / h2020.trip_count) as pct_change,
    CASE
        WHEN ((h2025.trip_count - h2020.trip_count) * 100.0 / h2020.trip_count) > 50 THEN 'High Surge'
        WHEN ((h2025.trip_count - h2020.trip_count) * 100.0 / h2020.trip_count) > 20 THEN 'Moderate Surge'
        WHEN ((h2025.trip_count - h2020.trip_count) * 100.0 / h2020.trip_count) < -20 THEN 'Demand Drop'
        ELSE 'Stable'
    END as demand_category
FROM
    (SELECT hour, trip_count FROM streamtide.ground_truth_hourly WHERE year = 2020) h2020
JOIN
    (SELECT hour, trip_count FROM streamtide.ground_truth_hourly WHERE year = 2025) h2025
ON h2020.hour = h2025.hour
ORDER BY pct_change DESC;
```

**Insight:** Shows which hours experienced recovery/growth post-pandemic

#### 2. **Dynamic Pricing Opportunities**

```sql
-- Calculate suggested surge multipliers based on demand and supply patterns
SELECT
    hour,
    avg_fare as base_fare,
    trip_count,
    CASE
        WHEN trip_count > (SELECT AVG(trip_count) * 1.5 FROM streamtide.ground_truth_hourly WHERE year = 2025)
            THEN ROUND(1 + (trip_count * 1.0 / (SELECT MAX(trip_count) FROM streamtide.ground_truth_hourly WHERE year = 2025)), 2)
        ELSE 1.0
    END as suggested_surge_multiplier,
    avg_fare * suggested_surge_multiplier as surge_fare
FROM streamtide.ground_truth_hourly
WHERE year = 2025
ORDER BY suggested_surge_multiplier DESC;
```

**Insight:** Data-driven surge pricing recommendations based on demand

#### 3. **Geographic Demand Imbalance**

```sql
-- Find zones with high pickup but low dropoff (opportunity for repositioning)
WITH pickup_stats AS (
    SELECT PULocationID as zone, pickup_count as pickups
    FROM streamtide.zone_demand
    WHERE year = 2025
),
dropoff_stats AS (
    SELECT DOLocationID as zone, COUNT(*) as dropoffs
    FROM streamtide.clean_trips
    WHERE year = 2025
    GROUP BY DOLocationID
)
SELECT
    p.zone,
    p.pickups,
    COALESCE(d.dropoffs, 0) as dropoffs,
    (p.pickups - COALESCE(d.dropoffs, 0)) as imbalance,
    CASE
        WHEN p.pickups > COALESCE(d.dropoffs, 0) * 1.5 THEN 'Taxi Source Zone'
        WHEN COALESCE(d.dropoffs, 0) > p.pickups * 1.5 THEN 'Taxi Sink Zone'
        ELSE 'Balanced'
    END as zone_type
FROM pickup_stats p
LEFT JOIN dropoff_stats d ON p.zone = d.zone
ORDER BY imbalance DESC
LIMIT 20;
```

**Insight:** Identifies zones where taxi repositioning would improve efficiency

#### 4. **Revenue Optimization Analysis**

```sql
-- Calculate potential revenue gain with dynamic pricing
SELECT
    h.year,
    SUM(h.trip_count * h.avg_fare) as current_revenue,
    SUM(h.trip_count * h.avg_fare *
        CASE
            WHEN h.trip_count > avg_demand.avg_trips * 1.3 THEN 1.3
            WHEN h.trip_count > avg_demand.avg_trips * 1.1 THEN 1.15
            ELSE 1.0
        END
    ) as potential_revenue_with_surge,
    SUM(h.trip_count * h.avg_fare *
        CASE
            WHEN h.trip_count > avg_demand.avg_trips * 1.3 THEN 1.3
            WHEN h.trip_count > avg_demand.avg_trips * 1.1 THEN 1.15
            ELSE 1.0
        END
    ) - SUM(h.trip_count * h.avg_fare) as additional_revenue
FROM streamtide.ground_truth_hourly h
CROSS JOIN (
    SELECT AVG(trip_count) as avg_trips
    FROM streamtide.ground_truth_hourly
    WHERE year = 2025
) avg_demand
WHERE h.year = 2025
GROUP BY h.year;
```

**Insight:** Quantifies potential revenue increase from surge pricing

#### 5. **Service Quality Metrics**

```sql
-- Analyze trip duration and fare efficiency
SELECT
    year,
    ROUND(AVG(trip_duration_min), 2) as avg_duration_minutes,
    ROUND(AVG(fare_amount / trip_duration_min), 2) as revenue_per_minute,
    ROUND(AVG(fare_amount / trip_distance), 2) as revenue_per_mile,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_duration_min) as median_duration,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY trip_duration_min) as p95_duration
FROM streamtide.duration_stats
GROUP BY year;
```

**Insight:** Service efficiency and customer experience metrics

#### 6. **Peak vs Off-Peak Analysis**

```sql
-- Compare peak and off-peak performance
SELECT
    CASE
        WHEN hour BETWEEN 7 AND 9 THEN 'Morning Rush'
        WHEN hour BETWEEN 17 AND 19 THEN 'Evening Rush'
        WHEN hour BETWEEN 22 AND 5 THEN 'Late Night'
        ELSE 'Off-Peak'
    END as time_period,
    year,
    SUM(trip_count) as total_trips,
    ROUND(AVG(avg_fare), 2) as avg_fare,
    ROUND(AVG(avg_distance), 2) as avg_distance
FROM streamtide.ground_truth_hourly
GROUP BY time_period, year
ORDER BY year, total_trips DESC;
```

**Insight:** Time-of-day demand patterns for resource allocation

#### 7. **Pandemic Impact Recovery Analysis**

```sql
-- Measure pandemic recovery by comparing 2020 to 2025
SELECT
    '2020 (Pandemic)' as period,
    SUM(trip_count) as total_annual_trips,
    ROUND(AVG(avg_fare), 2) as avg_fare,
    ROUND(SUM(trip_count * avg_fare), 2) as estimated_annual_revenue
FROM streamtide.ground_truth_hourly
WHERE year = 2020

UNION ALL

SELECT
    '2025 (Current)' as period,
    SUM(trip_count) as total_annual_trips,
    ROUND(AVG(avg_fare), 2) as avg_fare,
    ROUND(SUM(trip_count * avg_fare), 2) as estimated_annual_revenue
FROM streamtide.ground_truth_hourly
WHERE year = 2025;
```

**Insight:** Overall market recovery and growth trends

#### 8. **Algorithm Performance Validation**

```sql
-- Compare Reservoir Sampling accuracy vs Ground Truth
SELECT
    gt.hour,
    gt.avg_fare as ground_truth_fare,
    rs.avg_fare as sample_fare,
    ABS(gt.avg_fare - rs.avg_fare) as absolute_error,
    ROUND((ABS(gt.avg_fare - rs.avg_fare) / gt.avg_fare) * 100, 2) as error_percentage,
    CASE
        WHEN (ABS(gt.avg_fare - rs.avg_fare) / gt.avg_fare) * 100 < 2 THEN 'Excellent'
        WHEN (ABS(gt.avg_fare - rs.avg_fare) / gt.avg_fare) * 100 < 5 THEN 'Good'
        WHEN (ABS(gt.avg_fare - rs.avg_fare) / gt.avg_fare) * 100 < 10 THEN 'Acceptable'
        ELSE 'Needs Tuning'
    END as accuracy_rating
FROM streamtide.ground_truth_hourly gt
JOIN streamtide.streaming_results rs ON gt.hour = rs.hour
WHERE gt.year = 2025
ORDER BY error_percentage DESC;
```

**Insight:** Validates streaming algorithm accuracy for production use

#### 9. **Bloom Filter Efficiency Report**

```sql
-- Analyze duplicate detection effectiveness
SELECT
    'Total Items Processed' as metric,
    items_added as value
FROM streamtide.bloom_filter_stats

UNION ALL

SELECT
    'Duplicates Detected' as metric,
    duplicates_detected as value
FROM streamtide.bloom_filter_stats

UNION ALL

SELECT
    'False Positive Rate' as metric,
    ROUND(estimated_fpr * 100, 4) as value
FROM streamtide.bloom_filter_stats

UNION ALL

SELECT
    'Memory Usage (MB)' as metric,
    ROUND(memory_mb, 2) as value
FROM streamtide.bloom_filter_stats;
```

**Insight:** Memory efficiency and accuracy of duplicate detection

#### 10. **Neighborhood Demand Heatmap Data**

```sql
-- Prepare data for geographic heatmap visualization
SELECT
    zl.Borough,
    zl.Zone,
    zd.PULocationID,
    zd.pickup_count,
    zd.avg_fare_from_zone,
    zd.year,
    NTILE(5) OVER (PARTITION BY year ORDER BY pickup_count) as demand_quintile
FROM streamtide.zone_demand zd
JOIN streamtide.zone_lookup zl ON zd.PULocationID = zl.LocationID
WHERE year IN (2020, 2025)
ORDER BY year, pickup_count DESC;
```

**Insight:** Geographic visualization of demand distribution by neighborhood

### Visualization Recommendations in QuickSight

| Analysis Type | Best Visualization | Key Metrics |
|---------------|-------------------|-------------|
| **Demand Surge Detection** | Line chart with dual Y-axis | Trip count 2020 vs 2025 by hour |
| **Dynamic Pricing** | Heat map | Hour x Surge multiplier |
| **Geographic Imbalance** | Map with bubble size | Zone x Imbalance count |
| **Revenue Optimization** | KPI cards + bar chart | Current vs potential revenue |
| **Service Quality** | Box plot | Duration distribution by year |
| **Peak Analysis** | Stacked bar chart | Time period x trip count x year |
| **Recovery Analysis** | Donut chart + comparison table | 2020 vs 2025 total trips/revenue |
| **Algorithm Accuracy** | Gauge chart | Error % by hour |
| **Bloom Filter** | Metric cards | FPR, memory usage, duplicates |
| **Neighborhood Heatmap** | Geographic map | Zone shading by demand quintile |

### Export Insights for IEEE Report

```bash
# Export query results to CSV for analysis in Python/R
aws athena start-query-execution \
  --query-string "SELECT * FROM streamtide.ground_truth_hourly" \
  --result-configuration OutputLocation=s3://YOUR-BUCKET-results/exports/ \
  --query-execution-context Database=streamtide

# Download results
aws s3 sync s3://YOUR-BUCKET-results/exports/ ./report-data/
```

---

## Phase 10: Teardown & Cost Management

### 10.1 Option A: Stop/Terminate Resources via AWS Console (Web UI)

**Step-by-Step Cleanup:**

1. **Terminate EMR Cluster**
   - Go to EMR Console
   - Select your cluster: `StreamTide-EMR-Cluster`
   - Click "Terminate"
   - Confirm termination
   - Wait for status: "Terminated"

2. **Delete MSK Cluster**
   - Go to Amazon MSK Console
   - Select cluster: `streamtide-kafka`
   - Actions → Delete cluster
   - Type cluster name to confirm
   - Click "Delete"
   - Wait for deletion (~10 minutes)

3. **Manage S3 Data**
   - Go to S3 Console
   - Select bucket: `YOUR-BUCKET-processed`

   **Option 1: Archive to Glacier (Keep data, reduce cost)**
   - Select all objects
   - Actions → Edit storage class
   - Choose: Glacier Instant Retrieval
   - Save changes

   **Option 2: Delete Temporary Data**
   - Select bucket: `YOUR-BUCKET-logs`
   - Empty bucket (delete all objects)
   - Then delete bucket itself

4. **Stop QuickSight Subscription (if not needed)**
   - Go to QuickSight
   - Admin → Manage QuickSight
   - Account settings → Unsubscribe
   - (Only if you don't plan to use QuickSight further)

5. **Delete Athena Query Results**
   - Go to S3 → `YOUR-BUCKET-logs/athena-results/`
   - Select and delete old query results

6. **Review CloudWatch Logs**
   - Go to CloudWatch → Log groups
   - Delete old EMR and MSK log groups if not needed

### 10.2 Option B: Cleanup via CLI

```bash
# Terminate EMR cluster
aws emr terminate-clusters --cluster-ids ${CLUSTER_ID}

# Delete MSK cluster
aws kafka delete-cluster --cluster-arn ${MSK_ARN}

# Keep S3 data but move to cheaper storage
aws s3 sync s3://${BUCKET_PREFIX}-processed/ \
  s3://${BUCKET_PREFIX}-archive/ \
  --storage-class GLACIER_IR

# Delete temporary buckets (optional)
# aws s3 rb s3://${BUCKET_PREFIX}-logs --force
```

### 10.3 Monitor Costs in AWS Console

**Track Spending in Real-Time:**

1. **Go to AWS Cost Explorer**
   - AWS Console → Billing → Cost Explorer
   - Enable Cost Explorer (if first time)
   - View costs by service (EMR, MSK, S3)

2. **Set Budget Alerts**
   - AWS Console → Billing → Budgets
   - Create budget
   - Budget name: `StreamTide-Project-Budget`
   - Budget amount: $200
   - Alert threshold: 80% ($160)
   - Email notification to your address

3. **Daily Cost Monitoring**
   - Check Cost Explorer daily
   - Filter by tags: `Project=StreamTide`
   - Review top services by cost

### 10.4 Cost Estimation

| Service | Configuration | Estimated Cost (5 weeks) |
|---------|--------------|-------------------------|
| EMR | 1 m5.xlarge + 3 m5.2xlarge (8 hrs/day) | ~$300 |
| MSK | 3 kafka.m5.large (8 hrs/day) | ~$150 |
| S3 | 5 TB storage + data transfer | ~$100 |
| QuickSight | Author subscription | ~$20 |
| **Total** | | **~$570** |

**Cost Savings:**
- Use Spot instances for EMR task nodes: Save 50-70%
- Run clusters only when needed (8 hrs/day): Save 66%
- **Optimized Total: ~$150-200**

---

## Complete Execution Checklist

- [ ] Phase 1: AWS account setup and credentials
- [ ] Phase 2: S3 buckets created and data downloaded
- [ ] Phase 3: EMR cluster launched and verified
- [ ] Phase 4: Data validation jobs completed
- [ ] Phase 5: Ground truth baseline generated
- [ ] Phase 6: MSK cluster created and topics configured
- [ ] Phase 7: Streaming jobs running (producer + consumer)
- [ ] Phase 8: Accuracy validation completed
- [ ] Phase 9: QuickSight dashboards created
- [ ] Phase 10: Documentation and teardown

---

## Troubleshooting

### Common Issues

**1. EMR cluster fails to start**
```bash
# Check logs
aws emr describe-cluster --cluster-id ${CLUSTER_ID} --query 'Cluster.Status'

# View detailed logs in S3
aws s3 ls s3://${BUCKET_PREFIX}-logs/emr-logs/ --recursive
```

**2. Kafka connection issues**
```bash
# Test network connectivity from EMR to MSK
telnet <broker-endpoint> 9098

# Check security groups allow EMR → MSK traffic
```

**3. Out of memory in Spark**
```bash
# Increase executor memory
--executor-memory 12G --driver-memory 10G

# Reduce partition size
--conf spark.sql.shuffle.partitions=200
```

---

**Next Steps:** Run each phase sequentially, verify outputs, and document results for your IEEE report!
