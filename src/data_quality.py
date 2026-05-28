import sys
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class CircuitBreakerException(Exception):
    """Custom Exception thrown when a data contract check fails."""
    pass

def run_source_quality_checks(df: DataFrame, source_name: str, name_col: str):
    """
    Evaluates incoming DataFrames against explicit business data rules.
    """
    print(f"--> [Circuit Breaker] Evaluating data rules for {source_name}...")

    # Rule 1: Validate Critical Null Fields on the correct name column
    null_count = df.filter(F.col(name_col).isNull() | (F.col(name_col) == "")).count()
    if null_count > 0:
        raise CircuitBreakerException(
            f"CRITICAL ERROR: Found {null_count} records with null or blank '{name_col}' in {source_name}. "
            "Aborting pipeline execution to prevent downstream pollution."
        )

    # Rule 2: Check for Anomalous Revenue Drops (Source 2 only)
    if "revenue" in df.columns:
        avg_revenue = df.select(F.avg("revenue")).first()[0]
        if avg_revenue is not None and avg_revenue < 10000:
            raise CircuitBreakerException(
                f"CRITICAL ERROR: Suspicious revenue drop detected in {source_name}. "
                f"Average batch revenue is ${avg_revenue:,.2f}, which violates the contract threshold of $10,000.00. "
                "Aborting pipeline execution."
            )

    print(f"    [PASS] {source_name} successfully passed all data contract checks.")