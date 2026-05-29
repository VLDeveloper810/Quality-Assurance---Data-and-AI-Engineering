import json
import os
import sys

import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window

from data_quality import CircuitBreakerException, run_source_quality_checks
from metrics_logger import PipelineMonitor, logger
from train_ml_pipeline import run_ml_training_pipeline

# loading ENV variables
load_dotenv()
monitor = PipelineMonitor()

AWS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
BUCKET_NAME = os.getenv("BUCKET_NAME")

if not all([AWS_KEY, AWS_SECRET, BUCKET_NAME]):
    logger.error("Missing required environment variables in .env file!")
    sys.exit(1)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

# Dynamically route the Iceberg local warehouse relative to the active root
WAREHOUSE_PATH = os.path.join(PROJECT_ROOT, "iceberg_warehouse")

# Load configuration
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "operational_thresholds.json")
logger.info(f"Loading configuration from: {CONFIG_PATH}")

with open(CONFIG_PATH, "r") as config_file:
    pipeline_config = json.load(config_file)

# Extract config values
suffix_regex = pipeline_config["entity_resolution"]["suffix_regex"]
levenshtein_threshold = pipeline_config["entity_resolution"]["levenshtein_threshold"]
historical_avg = pipeline_config["drift_monitoring"]["historical_average_pct"]
deviation = pipeline_config["drift_monitoring"]["threshold_deviation_pct"]
app_name_ingestion = pipeline_config["pipeline"]["app_name_ingestion"]
iceberg_catalog = pipeline_config["pipeline"]["iceberg_catalog"]
iceberg_db = pipeline_config["pipeline"]["iceberg_db"]
iceberg_table = pipeline_config["pipeline"]["iceberg_table"]
temp_view_name = pipeline_config["pipeline"]["temp_view_name"]
s3a_endpoint_suffix = pipeline_config["spark_config"]["s3a_endpoint_suffix"]
iceberg_package = pipeline_config["spark_config"]["iceberg_package"]
hadoop_aws_package = pipeline_config["spark_config"]["hadoop_aws_package"]
min_avg_revenue = pipeline_config["data_quality"]["min_avg_revenue"]

# Define explicit schemas for both sources (avoids inference overhead)
schema_s1 = StructType([
    StructField("corporate_name_S1", StringType(), True),
    StructField("address", StringType(), True),
    StructField("activity_places", StringType(), True),
    StructField("top_suppliers", StringType(), True),
])

schema_s2 = StructType([
    StructField("corporate_name_S2", StringType(), True),
    StructField("main_customers", StringType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("profit", DoubleType(), True),
])

# Initialize Spark session with configuration
logger.info("Initializing Iceberg-Enabled PySpark Engine (Local Catalog)...")
spark = (
    SparkSession.builder.appName(app_name_ingestion)
    .master("local[*]")
    .config(
        "spark.jars.packages", f"{iceberg_package},{hadoop_aws_package}"
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(f"spark.sql.catalog.{iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{iceberg_catalog}.type", "hadoop")
    .config(f"spark.sql.catalog.{iceberg_catalog}.warehouse", WAREHOUSE_PATH)
    .config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET)
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.{s3a_endpoint_suffix}")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# Initialize S3 client and scan bucket
logger.info(f"Scanning S3 Bucket: '{BUCKET_NAME}'...")
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    region_name=AWS_REGION,
)

try:
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
except Exception as e:
    logger.error(f"AWS S3 API ERROR: Could not fetch object listings. Details: {e}")
    spark.stop()
    sys.exit(1)

# Use boto3 only for discovery, then read directly with Spark (distributed + efficient)
df_s1_raw = None
df_s2_raw = None

logger.info("Reading CSV files directly from S3 using distributed file system...")

if "Contents" in response:
    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.endswith("/") and key.endswith(".csv"):
            s3_path = f"s3a://{BUCKET_NAME}/{key}"
            logger.info(f"Processing S3 object: {s3_path}")
            try:
                # Use Spark's native CSV reader (distributed, memory-efficient)
                df_temp = spark.read.csv(
                    s3_path,
                    header=True,
                    inferSchema=False,
                    mode="FAILFAST"
                )
                
                cols = df_temp.columns
                
                if "corporate_name_S1" in cols:
                    # Re-read with proper schema for Source 1
                    df_s1_raw = spark.read.csv(
                        s3_path,
                        schema=schema_s1,
                        header=True,
                        mode="FAILFAST"
                    )
                    logger.info(f"Routed '{key}' as Source 1 (Supply Chain)")
                    
                elif "corporate_name_S2" in cols:
                    # Re-read with proper schema for Source 2
                    df_s2_raw = spark.read.csv(
                        s3_path,
                        schema=schema_s2,
                        header=True,
                        mode="FAILFAST"
                    )
                    logger.info(f"Routed '{key}' as Source 2 (Financial)")
                    
            except Exception as stream_err:
                logger.error(f"Failed to process S3 object '{key}': {stream_err}")

if df_s1_raw is None or df_s2_raw is None:
    logger.error("Data dependencies missing. Pipeline terminated.")
    spark.stop()
    sys.exit(1)

# Cache DataFrames to avoid repeated scans
df_s1_raw.cache()
df_s2_raw.cache()

# Log metrics info (single count operations)
monitor.metrics["source_1_supply_chain_row_count"] = df_s1_raw.count()
monitor.metrics["source_2_financial_row_count"] = df_s2_raw.count()

logger.info(
    f"[INGEST] Source 1 Count: {monitor.metrics['source_1_supply_chain_row_count']} rows."
)
logger.info(
    f"[INGEST] Source 2 Count: {monitor.metrics['source_2_financial_row_count']} rows."
)

# =====================================================================
# Circuit breaker checks
# =====================================================================
try:
    run_source_quality_checks(df_s1_raw, "Source 1 [Supply Chain]", "corporate_name_S1")
    run_source_quality_checks(
        df_s2_raw, "Source 2 [Financial Data]", "corporate_name_S2"
    )

except CircuitBreakerException as cb_err:
    logger.error(f"Pipeline terminated by circuit breaker: {str(cb_err)}")
    spark.stop()
    sys.exit(1)

logger.info("Data contracts verified successfully. Proceeding to Entity Resolution.")

# Entity resolution
logger.info("Normalizing name properties for comparison fields...")

monitor.start_timer("entity_resolution")

df_s1_clean = df_s1_raw.filter(F.col("corporate_name_S1").isNotNull()).withColumn(
    "norm_name_s1",
    F.trim(F.regexp_replace(F.lower(F.col("corporate_name_S1")), suffix_regex, "")),
)

df_s2_clean = df_s2_raw.filter(F.col("corporate_name_S2").isNotNull()).withColumn(
    "norm_name_s2",
    F.trim(F.regexp_replace(F.lower(F.col("corporate_name_S2")), suffix_regex, "")),
)

# Cache cleaned data before expensive join operation
df_s1_clean.cache()
df_s2_clean.cache()

logger.info("Pairing records via Levenshtein Fuzzy Distance Metrics...")

match_condition = (df_s1_clean["norm_name_s1"] == df_s2_clean["norm_name_s2"]) | (
    F.levenshtein(df_s1_clean["norm_name_s1"], df_s2_clean["norm_name_s2"]) <= levenshtein_threshold
)

df_resolved_pairs = df_s1_clean.join(df_s2_clean, match_condition, "outer")

monitor.stop_timer("entity_resolution")

# Harmonization
logger.info("Generating Unified Master Corporate Identifiers...")
df_harmonized = df_resolved_pairs.withColumn(
    "canonical_corporate_name",
    F.coalesce(F.col("corporate_name_S1"), F.col("corporate_name_S2")),
).withColumn(
    "corporate_id",
    F.md5(
        F.trim(
            F.lower(
                F.regexp_replace(F.col("canonical_corporate_name"), suffix_regex, "")
            )
        )
    ),
)

window_spec = Window.partitionBy("corporate_id").orderBy(
    F.col("address").desc_nulls_last()
)

df_final_production = (
    df_harmonized.select(
        F.col("corporate_id"),
        F.col("canonical_corporate_name").alias("corporate_name"),
        F.col("address"),
        F.col("activity_places"),
        F.col("top_suppliers"),
        F.col("main_customers"),
        F.col("revenue").cast("double"),
        F.col("profit").cast("double"),
    )
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# Cache final production data (used multiple times: count + enrichment + merge)
df_final_production.cache()

# Capture input count for reconciliation logic (single count from cache)
deduplicated_input_count = df_final_production.count()
logger.info(f"Duplicate matching instances purged. Ingestion target: {deduplicated_input_count} rows.")

# Calculate the clean match rate percentage using unique golden entities
base_count = monitor.metrics["source_2_financial_row_count"]
real_match_rate = (deduplicated_input_count / base_count * 100) if base_count > 0 else 0

monitor.detect_entity_drift(
    match_rate=real_match_rate,
    historical_average=historical_avg,
    threshold_deviation=deviation,
)

# Adverse Media News via LLM
logger.info("Deploying Entity Resolution Model for Adverse Media Scanning...")

sample_feed = [
    "BREAKING: Regulatory fines suspected for Techcorp over data protection breaches.",
    "Global Logistics LLC experiences record-breaking growth this quarter.",
    "Investigation opened into financial fraud and accounting regularities at Finserve Inc.",
]


def analyze_adverse_media_via_llm(company_name):
    normalized_target = company_name.lower().split()[0]
    for headline in sample_feed:
        if normalized_target in headline.lower():
            if any(k in headline.lower() for k in ["fine", "breach", "fraud"]):
                return f"Flagged Local Risk Scan: {headline}"
    return "None Identified"


def reconcile_pipeline_delivery(
    raw_ingestion_count: int, target_iceberg_table: str, spark_session
):
    logger.info("Launching Ingestion Reconciliation Service...")
    iceberg_df = spark_session.sql(
        f"SELECT COUNT(DISTINCT corporate_id) FROM {target_iceberg_table}"
    )
    final_merged_count = iceberg_df.first()[0]

    logger.info(f"Total Unique Input Records Discovered: {raw_ingestion_count}")
    logger.info(f"Total Unique Records Committed to Iceberg: {final_merged_count}")

    if raw_ingestion_count == 0:
        logger.warning("No incoming records found to process.")
        return

    delivery_efficiency = (final_merged_count / raw_ingestion_count) * 100
    logger.info(f"Pipeline Delivery Reliability Index: {delivery_efficiency:.3f}%")

    if delivery_efficiency >= 99.9:
        logger.info("Reconciliation complete. Pipeline meets the 99.9% data delivery reliability target.")
    else:
        logger.error(f"Data discrepancy detected! Delivery reliability dropped to {delivery_efficiency:.3f}%.")


llm_udf = F.udf(analyze_adverse_media_via_llm, StringType())
df_enriched_production = df_final_production.withColumn(
    "adverse_media_news", llm_udf(F.col("corporate_name"))
)
df_enriched_production.createOrReplaceTempView(temp_view_name)

# Iceberg table creation & Transactional merge upsert
target_table = f"{iceberg_catalog}.{iceberg_db}.{iceberg_table}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {iceberg_catalog}.{iceberg_db}")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        corporate_id STRING, corporate_name STRING, address STRING, activity_places STRING,
        top_suppliers STRING, main_customers STRING, revenue DOUBLE, profit DOUBLE, adverse_media_news STRING
    ) USING iceberg
"""
)

logger.info("Performing ACID Transactional MERGE INTO (Upsert)...")
spark.sql(
    f"""
    MERGE INTO {target_table} AS target
    USING {temp_view_name} AS source
    ON target.corporate_id = source.corporate_id
    WHEN MATCHED THEN
        UPDATE SET 
            target.corporate_name = source.corporate_name,
            target.address = source.address,
            target.activity_places = source.activity_places,
            target.top_suppliers = source.top_suppliers,
            target.main_customers = source.main_customers,
            target.revenue = source.revenue,
            target.profit = source.profit,
            target.adverse_media_news = source.adverse_media_news
    WHEN NOT MATCHED THEN
        INSERT *
"""
)
logger.info(
    "    [SUCCESS] Lakehouse merge operation committed securely to transactional logs."
)

# Launching reconciliation script
reconcile_pipeline_delivery(
    raw_ingestion_count=deduplicated_input_count,
    target_iceberg_table=target_table,
    spark_session=spark,
)

# --- CLOSE DOWN INGESTION SPARK ENGINE (only after ML pipeline uses it) ---
# Hand over to ML training pipeline with SHARED Spark session (no restart needed!)
ml_telemetry_records = run_ml_training_pipeline(spark=spark)

# Export the final dashboard, now loaded with your versioned ML metrics
monitor.export_structured_health_dashboard(ml_metrics=ml_telemetry_records)

# Clean up Spark session after both pipelines complete
spark.stop()
logger.info("--> Spark engine shutdown. Pipeline execution completed successfully.")
