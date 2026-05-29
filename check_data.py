import logging
import os
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("IcebergInspector")

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE_PATH = os.path.join(REPO_ROOT, "iceberg_warehouse")
TARGET_TABLE = "local_cat.db.corporate_registry"


def inspect_iceberg_table(sample_rows: int = 20):
    """Read the final Iceberg table and print a summary plus sample rows."""
    spark = (
        SparkSession.builder.appName("Iceberg-Table-Inspector")
        .master("local[1]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local_cat", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local_cat.type", "hadoop")
        .config("spark.sql.catalog.local_cat.warehouse", WAREHOUSE_PATH)
        .getOrCreate()
    )

    logger.info(f"\n[INSPECTING] Pulling records directly from Iceberg Table: {TARGET_TABLE}")

    # 2. Query the main table dataset
    df_registry = spark.sql(f"SELECT * FROM {TARGET_TABLE}")

    row_count = df_registry.count()
    logger.info(f"Total Production Record Count: {row_count}")
    logger.info("\n--- SAMPLE RECORDS OUTPUT ---")
    df_registry.show(sample_rows, truncate=False)

    # 3. Query Iceberg transactional snapshot metadata history
    logger.info("\nICEBERG TRANSACTION SNAPSHOT HISTORY")
    spark.sql(
        f"SELECT snapshot_id, committed_at, operation FROM {TARGET_TABLE}.snapshots"
    ).show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    inspect_iceberg_table()
