# Lakehouse Entity Resolution & ML Pipeline

## Overview
This repository implements a Python-based Lakehouse pipeline that:
- generates synthetic source data,
- loads it into an Iceberg table via Spark,
- performs entity resolution using normalized corporate names,
- applies data contract validations,
- trains a Spark ML logistic regression model,
- stores model artifacts with MLflow,
- and exposes pipeline telemetry via structured logs.

The main orchestration entrypoint is `trigger_pipeline.py`.

## Architecture
The pipeline is built as a local end-to-end data workflow with the following stages:

1. **Mock data generation**
   - `mock_data_generator/generate_mock_data.py`
   - Produces two CSV datasets in-memory.
   - Uploads them to the configured S3 bucket using Boto3.

2. **Ingestion + Entity Resolution + Iceberg write**
   - `src/main.py`
   - Reads the CSV objects from S3 using Spark with `s3a://`.
   - Validates both sources using data contract checks.
   - Normalizes corporate names and resolves duplicates.
   - Writes the final deduplicated dataset to Iceberg using a local warehouse.

3. **Model training + MLflow registry**
   - `src/train_ml_pipeline.py`
   - Reads the Iceberg table back into Spark.
   - Trains a logistic regression model to predict high-profit companies.
   - Logs metrics and stores the Spark ML model artifact in `mlruns/`.

4. **Monitoring and telemetry**
   - `src/metrics_logger.py`
   - Captures pipeline metrics, stage timing, and drift signals.
   - Writes logs to console and `pipeline_metrics.log`.

## Entity Resolution Heuristic
The entity resolution logic is implemented in `src/main.py` as follows:

- Normalize names by lowercasing and removing suffixes such as `Inc`, `LLC`, `Corp`, `Group`, `Co`, `Ltd`, `International`, plus punctuation.
- Compare records using either exact normalized match or fuzzy comparison via Spark SQL `levenshtein()`.
- The match threshold is configured in `config/operational_thresholds.json`:
  - `entity_resolution.suffix_regex`
  - `entity_resolution.levenshtein_threshold`
- Matching rows are merged with an outer join, then canonicalized to a single corporate identifier using MD5 of the normalized canonical company name.

## Setup Requirements
### 1. System requirements
- Python 3.9, 3.10, 3.11, or 3.12
- Java JDK 11 or 17
- Internet access for PyPI package installation
- AWS credentials with S3 permissions to read/write the configured bucket

### 2. Python dependencies
Install the repository dependencies from `requirements.txt`:

```bash
python -m pip install -r requirements.txt
```

### 2.1 One-click setup scripts
The repository includes one-click setup helpers:
- `setup.ps1` for Windows PowerShell
- `setup.sh` for Bash-compatible shells

Run the appropriate script from the repo root.

Windows PowerShell:

```powershell
./setup.ps1
```

Linux / macOS / WSL:

```bash
./setup.sh
```

These scripts will:
- create a local virtual environment in `.venv`
- install dependencies from `requirements.txt`
- copy `example.env` to `.env` if needed
- create required local folders `iceberg_warehouse` and `mlruns`

### 3. Environment variables
Create a `.env` file at the repository root by copying `example.env`:

```bash
copy example.env .env
```

Update `.env` with real values:

```env
AWS_ACCESS_KEY=AKIA...YOUR_ACTUAL_ACCESS_KEY_ID...
AWS_SECRET_KEY=...YOUR_ACTUAL_SECRET_ACCESS_KEY...
AWS_REGION=ap-south-1
BUCKET_NAME=your-s3-bucket-name
```

### 4. Local folders
Confirm these folders exist in the repo root:
- `hadoop/` — used for Spark on Windows
- `iceberg_warehouse/` — Iceberg warehouse storage
- `mlruns/` — MLflow artifact storage

The pipeline uses a local Iceberg catalog configured in `config/operational_thresholds.json`.

## Iceberg Metastore Setup
This repository uses a local Iceberg metastore based on the `hadoop` catalog and local warehouse.

The relevant config is in `config/operational_thresholds.json`:
- `pipeline.iceberg_catalog` = `local_cat`
- `pipeline.iceberg_db` = `db`
- `pipeline.iceberg_table` = `corporate_registry`
- `pipeline.warehouse_type` = `hadoop`

No separate external Hive metastore is required for local execution.

### Verify the local Iceberg setup
Before running the full pipeline, make sure Spark can initialize and access the warehouse path.

Example quick check:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master('local[*]')
    .appName('iceberg-check')
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.hadoop:hadoop-aws:3.3.4')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.local_cat', 'org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.local_cat.type', 'hadoop')
    .config('spark.sql.catalog.local_cat.warehouse', 'D:/vs_code/qa/iceberg_warehouse')
    .getOrCreate()

spark.sql('SHOW DATABASES').show()
spark.stop()
```

Replace the warehouse path with your actual repo path if needed.

## Running the Pipeline
From the repository root, execute:

```bash
python trigger_pipeline.py
```

This will run all pipeline stages in sequence:
1. Mock data generation and upload to S3
2. Spark ingestion, entity resolution, and Iceberg merge
3. Spark ML training and MLflow model registry
4. Final Iceberg table inspection and sample output display

### Expected outputs
- Iceberg table path: `local_cat.db.corporate_registry`
- MLflow artifacts under `mlruns/`
- Prometheus-style telemetry logged in `pipeline_metrics.log`

## Querying the Final Iceberg Table
### Option 1: Use the provided inspector script
Run:

```bash
python check_data.py
```

This script reads the Iceberg table and prints row counts plus sample rows.

### Option 2: Query directly from Spark
Use a Python script or interactive shell:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master('local[*]')
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.local_cat', 'org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.local_cat.type', 'hadoop')
    .config('spark.sql.catalog.local_cat.warehouse', 'iceberg_warehouse')
    .getOrCreate()

final_df = spark.sql('SELECT * FROM local_cat.db.corporate_registry')
final_df.show(20, truncate=False)
print(final_df.count())
```

## Viewing the Registered Model Artifact
The MLflow local tracking URI is set to the repository `mlruns/` folder.

### View via MLflow UI
Run:

```bash
mlflow ui --backend-store-uri mlruns
```

Then open the browser at:

```text
http://127.0.0.1:5000
```

Select the experiment `Corporate_Profitability_Analysis` and inspect the latest run.

### View artifact files directly
The model artifact is stored under:

```text
mlruns/<run_id>/artifacts/logistic_regression_model
```

Look for `MLmodel`, `python_env.yaml`, and the saved Spark ML model files.

## Troubleshooting
### Common issues
- `Missing required environment variables`: ensure `.env` exists and contains all keys.
- `S3 access error`: verify AWS keys and `BUCKET_NAME` permissions.
- `Spark package download failure`: ensure network access or predownload dependencies.
- `Iceberg warehouse path missing`: make sure `iceberg_warehouse/` exists and Spark can write there.

### Validation checklist
- [ ] Python environment active
- [ ] `requirements.txt` installed
- [ ] `.env` configured with valid AWS credentials
- [ ] S3 bucket reachable from local machine
- [ ] `hadoop/` folder present for Windows
- [ ] `iceberg_warehouse/` folder present
- [ ] `mlruns/` folder present or writable

## File references
- `trigger_pipeline.py` — main orchestration
- `mock_data_generator/generate_mock_data.py` — synthetic dataset generator
- `src/main.py` — ingestion, entity resolution, Iceberg merge
- `src/train_ml_pipeline.py` — Spark ML training and MLflow tracking
- `src/metrics_logger.py` — pipeline logging and telemetry
- `config/operational_thresholds.json` — pipeline parameters and thresholds
- `check_data.py` — Iceberg table inspection tool

## Notes
- This assignment is designed for local development and prototype execution.
- For production deployment, ensure credentials and logging are managed securely, and move the metastore to a dedicated catalog service if required.
