from pyspark.sql import SparkSession

WAREHOUSE_PATH = "D:/vs_code/qa/iceberg_warehouse"
TARGET_TABLE = "local_cat.db.corporate_registry"

# 1. Initialize an identical Iceberg Spark Engine
spark = (
    SparkSession.builder.appName("Iceberg-Table-Inspector")
    .master("local[1]")
    .config(
        "spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1"
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

print(f"\n🔍 [INSPECTING] Pulling records directly from Iceberg Table: {TARGET_TABLE}")

# 2. Query the main table dataset
df_registry = spark.sql(f"SELECT * FROM {TARGET_TABLE}")

print(f"📊 Total Production Record Count: {df_registry.count()}")
print("\n--- SAMPLE RECORDS OUTPUT ---")
df_registry.show(20, truncate=False)

# 3. BONUS: Query Iceberg Transactional Snapshot Metadata History
print("\n📜 --- ICEBERG TRANSACTION SNAPSHOT HISTORY ---")
spark.sql(
    f"SELECT snapshot_id, committed_at, operation FROM {TARGET_TABLE}.snapshots"
).show(5, truncate=False)

spark.stop()
