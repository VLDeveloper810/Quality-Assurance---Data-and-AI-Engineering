import sys
import os
import pytest
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

@pytest.fixture(scope="module")
def spark_fixture():
    """Initializes a local Spark Session dedicated to unit testing isolation."""
    spark = SparkSession.builder \
        .appName("Pytest-Pipeline-Data-Resilience-Testing") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_conflicting_financial_data_deduplication(spark_fixture):
    """
    EDGE CASE TEST: Assures the pipeline resolves conflicting financial figures
    for the same resolved corporate ID using our window spec rule.
    Rule: Prioritize the record with a valid address over NULL, then order by address.
    """
    # Setup mock data representing a post-joined, post-hashed dataframe
    # Here, 'techcorp_id' has two conflicting revenue and profit rows.
    suffix_regex = r"(?i)\b(inc|llc|corp|incorporated|group|co|ltd|international)\b|[.,_]"
    
    mock_data = [
        ("techcorp_id", "TechCorp", "123 Innovation Way", 500000.0, 120000.0), # Row A: Has Address, Higher Revenue
        ("techcorp_id", "TechCorp", None,                 450000.0, 110000.0)  # Row B: Missing Address, Lower Revenue
    ]
    
    schema = ["corporate_id", "corporate_name", "address", "revenue", "profit"]
    df_harmonized = spark_fixture.createDataFrame(mock_data, schema)
    
    # Apply the exact deduplication Window Specification from Step 5 of Main Pipeline
    window_spec = Window.partitionBy("corporate_id").orderBy(F.col("address").desc_nulls_last())
    
    df_final_production = df_harmonized.select(
        F.col("corporate_id"),
        F.col("corporate_name"),
        F.col("address"),
        F.col("revenue"),
        F.col("profit")
    ).withColumn("row_num", F.row_number().over(window_spec)) \
     .filter(F.col("row_num") == 1) \
     .drop("row_num")
     
    # Assertions (Validation Engine)
    result_records = df_final_production.collect()
    
    # Rule Assertion A: The 2 conflicting records must be crushed down to exactly 1 Golden Record
    assert len(result_records) == 1, "Data Resilience Failure: Duplicate corporate IDs were not collapsed!"
    
    golden_record = result_records[0]
    
    # Rule Assertion B: Check that the record with the complete address metadata survived the window filter
    assert golden_record["address"] == "123 Innovation Way", "Deduplication Failure: Did not preserve valid address metadata."
    assert golden_record["revenue"] == 500000.0, "Deduplication Failure: Conflicting lower financial data was chosen."


def test_name_cleaning_edge_cases(spark_fixture):
    """
    EDGE CASE TEST: Verifies that variations in capitalization, trailing spaces,
    and legal suffixes result in the exact same harmonized corporate ID.
    """
    suffix_regex = r"(?i)\b(inc|llc|corp|incorporated|group|co|ltd|international)\b|[.,_]"
    
    # Mock messy inputs that refer to the same logical entity
    mock_raw_names = [
        ("Finserve Inc.",),
        ("finserve llc",),
        ("FINSERVE international",),
        ("  Finserve,  ",)
    ]
    
    df_names = spark_fixture.createDataFrame(mock_raw_names, ["raw_name"])
    
    # Apply your pipeline transformation logic
    df_cleaned = df_names.withColumn(
        "corporate_id",
        F.md5(F.trim(F.regexp_replace(F.lower(F.col("raw_name")), suffix_regex, "")))
    )
    
    # Collect unique generated IDs
    unique_ids = [row["corporate_id"] for row in df_cleaned.select("corporate_id").distinct().collect()]
    
    # Assert that all 4 variations collapsed down to the exact same hash identifier
    assert len(unique_ids) == 1, f"Fuzzy Normalization Failure: Generated {len(unique_ids)} distinct IDs instead of 1."