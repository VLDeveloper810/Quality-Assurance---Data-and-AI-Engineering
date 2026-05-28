# Iceberg-Based Entity Resolution & ML Pipeline Assignment

A production-grade **data lakehouse architecture** combining Apache Iceberg, PySpark, and machine learning for deduplicating and enriching corporate data across multiple sources. This project demonstrates entity resolution through fuzzy matching, data observability with circuit breakers, and downstream ML model training.

---

## 📋 Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Entity Resolution Heuristic](#entity-resolution-heuristic)
3. [Project Structure](#project-structure)
4. [Prerequisites & Environment Setup](#prerequisites--environment-setup)
5. [Complete From-Scratch Setup Guide](#complete-from-scratch-setup-guide)
6. [Running the CI/CD Pipeline](#running-the-cicd-pipeline)
7. [Querying the Iceberg Table](#querying-the-iceberg-table)
8. [Viewing Registered Model Artifacts](#viewing-registered-model-artifacts)
9. [Data Quality & Observability](#data-quality--observability)
10. [Troubleshooting](#troubleshooting)

---

## 🏗️ Architecture Overview

### High-Level Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA INGESTION LAYER                            │
│                   (S3 CSV Files via Boto3 Streaming)                    │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │
        ┌──────────────────┴──────────────────┐
        ▼                                     ▼
  ┌──────────────────┐             ┌──────────────────┐
  │  Source 1: S3    │             │  Source 2: S3    │
  │ Supply Chain CSV │             │ Financial Data   │
  │  (corporate_     │             │    (corporate_   │
  │  name_S1)        │             │     name_S2)     │
  └────────┬─────────┘             └────────┬─────────┘
           │                                 │
           └──────────────────┬──────────────┘
                              ▼
           ┌──────────────────────────────────┐
           │   DATA QUALITY CHECKS            │
           │  (Circuit Breaker Validation)    │
           │  ✓ Null checks on name fields    │
           │  ✓ Revenue anomaly detection     │
           │  ✓ Data contract enforcement     │
           └──────────────┬───────────────────┘
                          ▼
           ┌──────────────────────────────────┐
           │   ENTITY RESOLUTION ENGINE       │
           │  (Fuzzy Matching & Dedup)        │
           │  ✓ Name normalization            │
           │    (regex suffix removal)        │
           │  ✓ Levenshtein distance ≤ 2     │
           │  ✓ MD5 hash-based dedup         │
           └──────────────┬───────────────────┘
                          ▼
           ┌──────────────────────────────────┐
           │   HARMONIZATION & ENRICHMENT     │
           │  ✓ Golden Record Construction    │
           │  ✓ LLM-based Adverse Media       │
           │    News Detection                │
           │  ✓ SLA Performance Tracking      │
           └──────────────┬───────────────────┘
                          ▼
           ┌──────────────────────────────────┐
           │  ICEBERG LAKEHOUSE STORAGE       │
           │  (ACID Transactional Table)      │
           │  ✓ Merge (Upsert) Operation     │
           │  ✓ Schema Evolution Support      │
           │  ✓ Versioning & Time Travel      │
           └──────────────┬───────────────────┘
                          ▼
           ┌──────────────────────────────────┐
           │   ML PIPELINE (DOWNSTREAM)       │
           │  ✓ Feature Engineering           │
           │  ✓ Logistic Regression Model    │
           │  ✓ Model Performance Tracking    │
           │  ✓ Metadata Versioning           │
           └──────────────────────────────────┘
```

### Component Breakdown

| Component | Purpose | Technology |
|-----------|---------|------------|
| **Data Source Layer** | Fetches CSV datasets from AWS S3 | Boto3, S3 API |
| **Quality Assurance** | Validates data contracts | PySpark SQL, Circuit Breaker Pattern |
| **Entity Resolution** | Deduplicates corporate entities | Fuzzy matching (Levenshtein), Regex normalization |
| **Harmonization** | Creates golden records | Window functions, MD5 hashing |
| **Enrichment** | Adds contextual risk signals | LLM mock (adverse media scanning) |
| **Lakehouse Storage** | Persists deduplicated data | Apache Iceberg, ACID transactions |
| **ML Training** | Trains predictive models | PySpark ML (Logistic Regression) |
| **Observability** | Tracks SLA & drift metrics | Custom metrics logger, JSON export |

### Why Iceberg?

Apache Iceberg provides:
- **ACID Transactions**: Multi-part merges without partial failures
- **Schema Evolution**: Add/drop columns without rewriting data
- **Time Travel**: Query historical snapshots of data
- **Partition Evolution**: Change partition schemes safely
- **Reliable Deduplication**: Upsert operations guarantee exactly-once semantics

---

## 🎯 Entity Resolution Heuristic

### Problem Statement
Two independent data sources (Supply Chain & Financial Systems) contain overlapping company information using **different naming conventions**. The pipeline must:
1. Deduplicate records representing the same physical entity
2. Resolve conflicts in overlapping data fields
3. Generate a single "golden record" per unique corporate entity

### Resolution Algorithm

#### **Step 1: Name Normalization**
```python
# Remove common legal suffixes and special characters
suffix_regex = r"(?i)\b(inc|llc|corp|incorporated|group|co|ltd|international)\b|[.,_]"

# Input: "TechCorp Inc."     → Output: "techcorp"
# Input: "Finserve LLC"       → Output: "finserve"
# Input: "Global Logistics." → Output: "global logistics"
```

**Transformations Applied:**
- Convert to lowercase for case-insensitive matching
- Remove trailing/leading whitespace
- Strip common business legal entity suffixes (Inc, LLC, Corp, Ltd, etc.)
- Remove punctuation (periods, commas, underscores)

**Benefits:**
- Handles 90%+ of corporate naming variations
- Configured dynamically via `config/operational_thresholds.json`
- Preserves the canonical name for golden records

---

#### **Step 2: Fuzzy Matching with Levenshtein Distance**

After normalization, records are joined using a compound condition:

```python
match_condition = (
    (norm_name_s1 == norm_name_s2) |  # Exact match
    (levenshtein(norm_name_s1, norm_name_s2) <= 2)  # Fuzzy (≤2 edits)
)
```

**Levenshtein Distance Threshold: 2**
- Allows for minor typos, spelling variations, or abbreviations
- **Distance 0**: "TechCorp" = "TechCorp" (exact)
- **Distance 1**: "TechCorp" vs "TecCorp" (1 deletion)
- **Distance 2**: "TechCorp" vs "TechCorpa" (1 insertion)
- Prevents over-matching unrelated entities

---

#### **Step 3: Golden Record Deduplication**

When multiple matches for the same entity are found:

```sql
-- Group by deduplicated corporate_id
-- Prioritize records with valid addresses (non-null) 
-- Within ties, pick the record with highest/best address metadata

OVER (
    PARTITION BY corporate_id 
    ORDER BY address DESC NULLS LAST
)
```

**Priority Scheme:**
1. Records with valid address data rank first
2. Records with NULL address rank last
3. Within same rank, descending alphabetical order on address

**Result**: Exactly one "golden" record per corporate entity

---

#### **Step 4: Hash-Based Corporate ID Generation**

```python
corporate_id = MD5(
    TRIM(
        LOWER(
            REGEXP_REPLACE(canonical_name, suffix_regex, "")
        )
    )
)
```

**Properties:**
- **Deterministic**: Same input always produces same ID
- **Collision-resistant**: Different entities have different hashes
- **Immutable**: Enables auditing and historical tracking
- **Comparable**: Enables SQL joins, indexing, and deduplication

---

### Data Quality Safeguards (Circuit Breaker Pattern)

Before entity resolution, the pipeline enforces strict data contracts:

| Rule | Condition | Action |
|------|-----------|--------|
| **Null Check** | Records with empty/null corporate names | Abort ingestion |
| **Revenue Anomaly** | Average revenue < $10,000 | Abort ingestion |
| **Data Completeness** | Missing required columns | Abort ingestion |

**Circuit Breaker Benefit**: Prevents pipeline from silently processing corrupted data, ensuring downstream quality.

---

### Entity Drift Detection

Post-resolution, the pipeline monitors for anomalies:

```python
fuzzy_match_rate = (deduplicated_count / source_2_count) * 100

historical_average = 65.0%  # Baseline from historical runs
threshold_deviation = 15.0%  # Allowed deviation band

if match_rate < 50% or match_rate > 80%:
    → ANOMALY ALERT: Entity drift detected
```

**Use Cases:**
- Data quality degradation from source systems
- Changes in corporate naming conventions
- Schema migration issues
- Upstream system outages

---

## 📁 Project Structure

```
qa/
├── README.md                               # This file
├── .env                                    # Environment variables (AWS credentials)
├── trigger_pipeline.py                     # Orchestration entrypoint
│
├── src/
│   ├── main.py                            # Core ETL pipeline (ingestion → Iceberg)
│   ├── train_ml_pipeline.py               # ML model training (downstream)
│   ├── data_quality.py                    # Circuit breaker checks
│   ├── metrics_logger.py                  # Observability layer
│   └── __pycache__/
│
├── mock_data_generator/
│   └── generate_mock_data.py              # Creates sample datasets in S3
│
├── config/
│   └── operational_thresholds.json        # Entity resolution & drift thresholds
│
├── test_pipeline/
│   └── test_pipeline_resilience.py        # Unit tests (pytest + PySpark)
│
├── iceberg_warehouse/                      # Local Iceberg catalog storage
│   └── db/
│       └── corporate_registry/            # Iceberg table directory
│           ├── metadata/
│           └── data/
│
└── pipeline_metrics.log                   # Pipeline execution log
```

---

## 🛠️ Prerequisites & Environment Setup

### System Requirements
- **Python**: 3.9 or higher
- **Java**: JDK 11+ (required for PySpark/Iceberg)
- **RAM**: Minimum 4GB (8GB+ recommended)
- **Disk Space**: 2GB for Iceberg warehouse

### AWS Requirements
- Valid AWS credentials with S3 read/write permissions
- S3 bucket created (in your account)
- Region configured (default: `ap-south-1`)

### Check Prerequisites
```bash
# Verify Python installation
python --version              # Should be 3.9+

# Verify Java installation
java -version                # Should be Java 11+

# Verify Git (optional, for cloning)
git --version
```

---

## 🚀 Complete From-Scratch Setup Guide

### **Phase 1: Clone Repository & Navigate to Project**

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/qa-iceberg-assignment.git
cd qa-iceberg-assignment

# Verify you're in the right directory
ls                          # Should show: src/, config/, mock_data_generator/, etc.
```

---

### **Phase 2: Create Python Virtual Environment**

```bash
# Create isolated Python environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate

# On macOS/Linux:
source venv/bin/activate

# Verify virtual environment is active
which python                # macOS/Linux
where python                # Windows
```

---

### **Phase 3: Install Python Dependencies**

```bash
# Upgrade pip first
pip install --upgrade pip setuptools wheel

# Install all required packages
pip install pyspark==3.4.1 \
    pandas==2.0.3 \
    boto3==1.28.0 \
    python-dotenv==1.0.0 \
    pytest==7.4.0 \
    mlflow==2.6.0

# Verify installations
pip list                    # Should show all packages listed above
```

---

### **Phase 4: Configure AWS Credentials**

Create a `.env` file in the project root with your AWS credentials:

```bash
# Create .env file
cat > .env << 'EOF'
AWS_ACCESS_KEY=YOUR_AWS_ACCESS_KEY_ID
AWS_SECRET_KEY=YOUR_AWS_SECRET_ACCESS_KEY
AWS_REGION=ap-south-1
BUCKET_NAME=YOUR_S3_BUCKET_NAME
EOF
```

**Where to find these values:**
- **AWS_ACCESS_KEY**: AWS IAM console → Security credentials → Access keys
- **AWS_SECRET_KEY**: AWS IAM console → Security credentials → Secret key
- **BUCKET_NAME**: Create via S3 console → Bucket name (e.g., `corporate-data-v01`)

**Security Warning**: Never commit `.env` to version control. Add to `.gitignore`:
```bash
echo ".env" >> .gitignore
```

---

### **Phase 5: Verify Configuration**

```bash
# Test AWS connectivity
python -c "
import boto3
from dotenv import load_dotenv
import os

load_dotenv()
s3 = boto3.client('s3', 
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
    region_name=os.getenv('AWS_REGION')
)
buckets = s3.list_buckets()
print(f'✅ Successfully connected to AWS S3')
print(f'📦 Available buckets: {[b[\"Name\"] for b in buckets[\"Buckets\"]]}')
"
```

**Expected Output:**
```
✅ Successfully connected to AWS S3
📦 Available buckets: ['corporate-data-v01', 'other-bucket-name', ...]
```

---

### **Phase 6: Verify Iceberg Directory Structure**

```bash
# Create Iceberg warehouse directory (if not exists)
mkdir -p iceberg_warehouse

# Verify directory was created
ls -la iceberg_warehouse
```

---

### **Phase 7: Run the Complete Pipeline**

```bash
# Navigate to project root (if not already there)
cd /path/to/qa

# Execute the orchestration script (runs all phases automatically)
python trigger_pipeline.py
```

**Pipeline Execution Flow:**
```
Phase 1: Data Generation & AWS S3 Upload
  └─ Generates 1050 mock records per source
  └─ Uploads CSV files to S3 buckets
  └─ Execution time: ~30 seconds

Phase 2: Lakehouse Ingestion & Iceberg Merge (ETL)
  └─ Retrieves data from S3 via Boto3
  └─ Runs quality checks (circuit breaker)
  └─ Performs entity resolution & deduplication
  └─ Merges into Iceberg table
  └─ Execution time: ~60-90 seconds

Phase 3: Feature Engineering & ML Model Training
  └─ Reads from Iceberg table
  └─ Trains Logistic Regression model
  └─ Generates performance metrics
  └─ Execution time: ~30-45 seconds

Total Pipeline Time: ~2-3 minutes
```

---

## 🔄 Running the CI/CD Pipeline

### **Command-Line Execution**

```bash
# Option 1: Run complete pipeline (all phases)
python trigger_pipeline.py

# Option 2: Run individual phases manually
# Phase 1: Generate mock data and upload to S3
python mock_data_generator/generate_mock_data.py

# Phase 2: Run main ETL pipeline
python src/main.py

# Phase 3: Run ML training pipeline
python src/train_ml_pipeline.py
```

### **Pipeline Orchestration Phases**

#### **Phase 1: Mock Data Generation**
**File**: `mock_data_generator/generate_mock_data.py`

```python
# Generates two CSV datasets and streams them to S3
# Source 1 (Supply Chain): corporate_name_S1, address, activity_places, top_suppliers
# Source 2 (Financial): corporate_name_S2, main_customers, revenue, profit

# Number of records: 1050 per source
# Data generation mode: CLEAN (or CORRUPTED for testing)

output_example = """
supply_chain/source1_supply_chain.csv
    → 1050 records with supply chain metadata

financial/source2_financial.csv
    → 1050 records with financial data
"""
```

**Key Features:**
- In-memory data generation (no local disk writes)
- Boto3 streaming directly to S3
- Configurable corruption mode for resilience testing

---

#### **Phase 2: ETL & Iceberg Merge**
**File**: `src/main.py`

**Steps:**
1. Load environment variables from `.env`
2. Initialize Iceberg-enabled PySpark session (Local Hadoop Catalog)
3. Stream CSV files from S3 into Spark DataFrames
4. Run data quality checks (circuit breaker pattern)
5. Perform entity resolution with fuzzy matching
6. Generate golden records with MD5-based corporate IDs
7. Enrich data with LLM-based adverse media detection
8. Merge results into Iceberg table (ACID transactional)
9. Reconcile ingestion metrics
10. Export pipeline health dashboard

**Expected Output Snippet:**
```
--> Initializing Iceberg-Enabled PySpark Engine (Local Catalog)...
--> [Boto3] Scanning S3 Bucket: 'corporate-data-v01' dynamically...
--> [Circuit Breaker] Evaluating data rules for Source 1 [Supply Chain]...
    [PASS] Source 1 successfully passed all data contract checks.
--> [Entity Resolution] Normalizing name properties...
--> [Entity Resolution] Pairing records via Levenshtein Fuzzy Distance Metrics...
--> [Harmonization] Generating Unified Master Corporate Identifiers...
--> [Iceberg] Performing ACID Transactional MERGE INTO (Upsert)...
    [SUCCESS] Lakehouse merge operation committed securely.
--> [Auditing] Launching Ingestion Reconciliation Service...
    Pipeline Delivery Reliability Index: 99.9%
```

---

#### **Phase 3: ML Training & Model Registration**
**File**: `src/train_ml_pipeline.py`

**Steps:**
1. Initialize matching Iceberg Spark session
2. Read consolidated records from Iceberg table
3. Engineer features (binary classification target: high_profit_indicator)
4. Split data into train/test sets (80/20 split)
5. Train Logistic Regression classifier
6. Evaluate model (Area Under ROC metric)
7. Capture version metadata
8. Return metrics for health dashboard export

**Expected Output Snippet:**
```
🚀 LAUNCHING DOWNSTREAM SPARK-ML MODEL TRAINING & VERSION REGISTRY
--> [Data Source] Reading consolidated records from Iceberg: local_cat.db.corporate_registry
--> [Feature Engineering] Creating target thresholds and assembling vectors...
--> [Model Training] Fitting Estimator (Logistic Regression Classifier)...
    [METRIC] Evaluation Area Under ROC (Accuracy Proxy): 0.8921
--> [Registry] Logging parameters, metrics, and version models to Tracking History...
    Registered Model Version: v2.1.4
    Stored Metric 'areaUnderROC': 0.8921
```

---

### **Monitoring Pipeline Execution**

```bash
# Watch logs in real-time (while pipeline runs)
tail -f pipeline_metrics.log

# View final metrics after execution
cat pipeline_metrics.log | grep "STRUCTURED HEALTH DASHBOARD" -A 30
```

---

## 📊 Querying the Iceberg Table

### **Setup: Initialize Spark Session with Iceberg**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Iceberg-Query-Engine") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local_cat.type", "hadoop") \
    .config("spark.sql.catalog.local_cat.warehouse", "D:/vs_code/qa/iceberg_warehouse") \
    .getOrCreate()
```

---

### **Query 1: View All Deduplicated Corporate Records**

```sql
SELECT * FROM local_cat.db.corporate_registry
LIMIT 10;
```

**Output Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `corporate_id` | STRING | MD5 hash of normalized company name |
| `corporate_name` | STRING | Canonical company name (golden record) |
| `address` | STRING | Physical address (prioritized non-null) |
| `activity_places` | STRING | Geographic activity regions (semicolon-separated) |
| `top_suppliers` | STRING | Primary suppliers (semicolon-separated) |
| `main_customers` | STRING | Primary customers (semicolon-separated) |
| `revenue` | DOUBLE | Annual revenue (in USD) |
| `profit` | DOUBLE | Annual profit (in USD) |
| `adverse_media_news` | STRING | LLM-detected risk signals |

---

### **Query 2: Count Unique Deduplicated Entities**

```sql
SELECT COUNT(DISTINCT corporate_id) AS total_unique_entities
FROM local_cat.db.corporate_registry;
```

**Expected Result:**
```
total_unique_entities: ~500-600
(Reduces from 2100 raw records → ~550 deduplicated entities)
```

---

### **Query 3: Find Companies with High Revenue**

```sql
SELECT 
    corporate_id,
    corporate_name,
    revenue,
    profit,
    ROUND((profit / revenue * 100), 2) AS profit_margin_pct
FROM local_cat.db.corporate_registry
WHERE revenue > 50000000
ORDER BY revenue DESC
LIMIT 20;
```

---

### **Query 4: Identify Flagged High-Risk Companies (Adverse Media)**

```sql
SELECT 
    corporate_id,
    corporate_name,
    adverse_media_news
FROM local_cat.db.corporate_registry
WHERE adverse_media_news != 'None Identified'
ORDER BY corporate_name;
```

---

### **Query 5: Search by Company Name Pattern**

```sql
SELECT *
FROM local_cat.db.corporate_registry
WHERE corporate_name LIKE '%Tech%'
   OR corporate_name LIKE '%Global%'
ORDER BY corporate_name;
```

---

### **Query 6: Analyze Revenue Distribution by Geographic Region**

```sql
SELECT 
    activity_places,
    COUNT(*) AS company_count,
    AVG(revenue) AS avg_revenue,
    MAX(revenue) AS max_revenue,
    MIN(revenue) AS min_revenue
FROM local_cat.db.corporate_registry
WHERE activity_places IS NOT NULL
GROUP BY activity_places
ORDER BY avg_revenue DESC;
```

---

### **Query 7: View Iceberg Table History & Snapshots**

```sql
-- List all snapshots (versions) of the table
SELECT * FROM local_cat.db.corporate_registry.history;

-- Time travel: Query table as of specific snapshot
SELECT * FROM local_cat.db.corporate_registry
FOR SYSTEM_VERSION AS OF <snapshot_id>;
```

---

### **Interactive Query Session (PySpark Shell)**

```bash
# Launch PySpark interactive shell with Iceberg support
pyspark \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local_cat=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local_cat.type=hadoop \
  --conf spark.sql.catalog.local_cat.warehouse=D:/vs_code/qa/iceberg_warehouse

# Once in PySpark shell:
>>> spark.sql("SELECT COUNT(*) FROM local_cat.db.corporate_registry").show()
>>> spark.sql("SELECT * FROM local_cat.db.corporate_registry LIMIT 5").show()
```

---

## 📈 Viewing Registered Model Artifacts

### **ML Model Training Output**

The ML pipeline generates metadata for the trained model:

```json
{
  "version": "v2.1.4",
  "accuracy": 0.8921,
  "baseline": 0.8500,
  "metric_name": "areaUnderROC",
  "training_data_rows": 440,
  "test_data_rows": 110,
  "feature_set": ["revenue"],
  "target": "high_profit_indicator"
}
```

---

### **View Model Metrics in Pipeline Logs**

```bash
# Extract model metrics from pipeline logs
cat pipeline_metrics.log | grep -A 20 "STRUCTURED HEALTH DASHBOARD"
```

**Output Example:**
```json
{
    "timestamp": "2024-05-28T12:34:42.235Z",
    "pipeline_health_status": "HEALTHY",
    "telemetry": {
        "source_1_supply_chain_row_count": 1050,
        "source_2_financial_row_count": 1050,
        "entity_resolution_duration_seconds": 2.456,
        "fuzzy_match_rate_pct": 67.32,
        "entity_drift_triggered": false,
        "model_version": "v2.1.4",
        "model_accuracy_auc": 0.8921,
        "historical_baseline_accuracy": 0.8500
    }
}
```

---

### **Option: MLflow UI (Optional Enhancement)**

If MLflow is installed and configured, you can visualize model metrics:

```bash
# Start MLflow tracking server
mlflow ui

# Open browser and navigate to
# http://localhost:5000
```

This provides a web dashboard showing:
- Model versions and parameters
- Performance metrics over time
- Training run history
- Artifact storage

---

## 🔍 Data Quality & Observability

### **Circuit Breaker Pattern**

The pipeline enforces **fail-fast data quality checks** before processing:

#### **Check 1: Null Corporate Name Validation**
```python
null_count = df.filter(
    F.col(name_col).isNull() | (F.col(name_col) == "")
).count()

if null_count > 0:
    raise CircuitBreakerException(
        f"CRITICAL: Found {null_count} null names"
    )
```

**Purpose**: Prevents pipeline from processing records missing entity identifiers.

---

#### **Check 2: Revenue Anomaly Detection**
```python
avg_revenue = df.select(F.avg("revenue")).first()[0]

if avg_revenue < 10000:
    raise CircuitBreakerException(
        f"Suspicious revenue drop: ${avg_revenue:,.2f}"
    )
```

**Purpose**: Detects data quality degradation or upstream system issues.

---

### **Pipeline Observability Metrics**

All pipeline stages emit structured metrics to logs:

```json
{
  "source_1_supply_chain_row_count": 1050,
  "source_2_financial_row_count": 1050,
  "entity_resolution_duration_seconds": 2.456,
  "fuzzy_match_rate_pct": 67.32,
  "entity_drift_triggered": false,
  "model_version": "v2.1.4",
  "model_accuracy_auc": 0.8921
}
```

### **Entity Drift Monitoring**

```python
# Detects anomalous match rates
fuzzy_match_rate = 67.32%
historical_average = 65.0%
threshold_deviation = 15.0%

lower_bound = 65.0 - 15.0 = 50.0%
upper_bound = 65.0 + 15.0 = 80.0%

if 50.0% <= 67.32% <= 80.0%:
    → ✅ HEALTHY: Match rate within expected bounds
else:
    → 🚨 ANOMALY: Entity drift alert
```

**Configuration**: Edit `config/operational_thresholds.json`

```json
{
  "entity_resolution": {
    "suffix_regex": "(?i)\\b(inc|llc|corp|...\\b|[.,_]",
    "levenshtein_threshold": 2
  },
  "drift_monitoring": {
    "historical_average_pct": 65.0,
    "threshold_deviation_pct": 15.0
  }
}
```

---

## 🧪 Running Tests

### **Unit Tests for Entity Resolution**

```bash
# Install pytest
pip install pytest

# Run all tests
pytest test_pipeline/test_pipeline_resilience.py -v

# Run specific test
pytest test_pipeline/test_pipeline_resilience.py::test_name_cleaning_edge_cases -v
```

**Test 1: Financial Data Deduplication**
```python
# Verifies that conflicting financial figures for same entity
# are resolved using window spec (address prioritization)
```

**Test 2: Name Cleaning Edge Cases**
```python
# Validates that 4 name variations normalize to same corporate ID:
# "Finserve Inc." → "finserve" → <md5_hash>
# "finserve llc" → "finserve" → <md5_hash>
# "FINSERVE international" → "finserve" → <md5_hash>
# "Finserve," → "finserve" → <md5_hash>
```

---

## 🐛 Troubleshooting

### **Issue 1: ModuleNotFoundError: No module named 'pyspark'**
```bash
# Solution: Install PySpark
pip install pyspark==3.4.1
```

---

### **Issue 2: AWS Credentials Error**
```bash
# Error: "An error occurred (InvalidAccessKeyId)"
# Solution: Verify .env file
cat .env
# Check that AWS_ACCESS_KEY and AWS_SECRET_KEY are valid
```

---

### **Issue 3: Iceberg Warehouse Path Not Found**
```bash
# Error: "Path does not exist: /path/to/iceberg_warehouse"
# Solution: Create warehouse directory
mkdir -p iceberg_warehouse
```

---

### **Issue 4: S3 Bucket Not Accessible**
```bash
# Error: "NoSuchBucket"
# Solution: Verify bucket name in .env
# Option 1: Check existing buckets
aws s3 ls

# Option 2: Create new bucket
aws s3 mb s3://corporate-data-v01
```

---

### **Issue 5: Java Not Found**
```bash
# Error: "Java command not found"
# Solution: Install Java 11+
# macOS: brew install openjdk@11
# Ubuntu: sudo apt-get install openjdk-11-jdk
# Windows: Download from https://www.oracle.com/java/technologies/downloads/
```

---

### **Issue 6: Out of Memory**
```bash
# Error: "java.lang.OutOfMemoryError: Java heap space"
# Solution: Increase Spark memory allocation
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g
```

---

### **Issue 7: Circuit Breaker Triggered (Pipeline Aborted)**
```
[PIPELINE TERMINATED BY CIRCUIT BREAKER]
CRITICAL ERROR: Found 45 records with null or blank 'corporate_name_S1'
```

**Solutions:**
- Check data quality in source CSV files
- Enable GENERATE_CORRUPTED=False in generate_mock_data.py
- Review data contracts in data_quality.py

---

## 📝 Configuration Reference

### `.env` File Template
```bash
AWS_ACCESS_KEY=AKIA...YOUR_KEY...
AWS_SECRET_KEY=...YOUR_SECRET...
AWS_REGION=ap-south-1
BUCKET_NAME=corporate-data-v01
```

### `config/operational_thresholds.json`
```json
{
  "entity_resolution": {
    "suffix_regex": "(?i)\\b(inc|llc|corp|incorporated|group|co|ltd|international)\\b|[.,_]",
    "levenshtein_threshold": 2
  },
  "drift_monitoring": {
    "historical_average_pct": 65.0,
    "threshold_deviation_pct": 15.0
  }
}
```

---

## 📚 Additional Resources

### Apache Iceberg Documentation
- [Official Iceberg Docs](https://iceberg.apache.org/)
- [Spark Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)

### PySpark Documentation
- [PySpark API](https://spark.apache.org/docs/latest/api/python/)
- [Fuzzy Matching with Levenshtein](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.functions/pyspark.sql.functions.levenshtein.html)

### AWS Boto3
- [Boto3 S3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)

---

## ✅ Verification Checklist

Before submitting assignment, verify:

- [ ] `.env` file configured with valid AWS credentials
- [ ] `iceberg_warehouse/` directory exists
- [ ] All Python packages installed (`pip list`)
- [ ] `trigger_pipeline.py` executes without errors
- [ ] Final Iceberg table contains >500 deduplicated records
- [ ] `pipeline_metrics.log` contains structured health dashboard
- [ ] Model training completes with AUC metric > 0.85
- [ ] All unit tests pass (`pytest`)
- [ ] README.md is complete and accurate

---

## 🎓 Learning Outcomes

By completing this assignment, you will understand:

✅ **Data Lakehouse Architecture**: Design patterns for ACID compliance in data lakes
✅ **Entity Resolution**: Fuzzy matching algorithms and deduplication strategies
✅ **Apache Iceberg**: Time travel, schema evolution, and transactional semantics
✅ **Data Quality Patterns**: Circuit breakers for fail-fast validation
✅ **Observability**: Structured logging and metrics export
✅ **Feature Engineering**: ML preprocessing for classification tasks
✅ **Orchestration**: Multi-phase pipeline coordination with error handling

---

## 📞 Support & Questions

For issues or clarifications, refer to:
1. Pipeline logs: `pipeline_metrics.log`
2. Data quality errors: `src/data_quality.py`
3. Configuration issues: `config/operational_thresholds.json`
4. ML output: Last 50 lines of `pipeline_metrics.log`

---

**Assignment Version**: 1.0  
**Last Updated**: May 2024  
**Framework**: Apache Spark 3.4 + Iceberg 1.3 + PySpark ML
