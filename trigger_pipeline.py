import logging
import os
import subprocess
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("PipelineOrchestrator")

# =====================================================================
#  SYSTEM LEVEL WINDOWS PATCH (COPIES HADOOP CONFIG TO ALL SUBPROCESSES)
# =====================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_HADOOP = os.path.join(BASE_DIR, "hadoop")

if os.path.exists(LOCAL_HADOOP):
    os.environ["HADOOP_HOME"] = LOCAL_HADOOP
    os.environ["PATH"] = (
        os.path.join(LOCAL_HADOOP, "bin") + os.pathsep + os.environ.get("PATH", "")
    )
    logger.info(
        f"--> [Orchestrator Patch] Global Hadoop Binaries injected: {LOCAL_HADOOP}"
    )


def run_script(script_path, description):
    logger.info("\n" + "=" * 80)
    logger.info(f"STARTING PHASE: {description}")
    logger.info(f" Execution Target: {script_path}")
    logger.info("=" * 80)

    start_time = time.time()

    # Pass os.environ down explicitly so subprocesses inherit our Hadoop paths
    process = subprocess.Popen(
        [sys.executable, script_path],
        stdout=None,
        stderr=None,
        env=os.environ.copy(),
    )
    process.communicate()

    elapsed_time = time.time() - start_time

    if process.returncode != 0:
        logger.error(
            f"\n[CRITICAL FAILURE]: {description} failed with exit code {process.returncode}."
        )
        logger.error("Orchestrator halted. Check logs above for errors.")
        sys.exit(process.returncode)

    logger.info(f"[SUCCESS]: {description} completed in {elapsed_time:.2f} seconds.")


if __name__ == "__main__":
    logger.info("=====================================================================")
    logger.info("CORE LAKEHOUSE & MACHINE LEARNING ORCHESTRATION PIPELINE ENGINE")
    logger.info("=====================================================================")

    # Phase 1: Mock Data Generation and S3 Partition Syncing
    run_script(
        "./mock_data_generator/generate_mock_data.py",
        "Data Generation & AWS S3 Upload",
    )

    # Phase 2: Ingestion, Quality Checks, LLM Enrichment, and Iceberg Upsert
    run_script("./src/main.py", "Lakehouse Ingestion & Iceberg Merge (ETL)")

    # Phase 3: Spark ML Training and MLflow Registration
    run_script(
        "./src/train_ml_pipeline.py", "Feature Engineering & Spark ML Model Training"
    )

    # Phase 4: Inspect final Iceberg output table and show samples
    run_script("./check_data.py", "Iceberg Table Inspection & Output Preview")

    logger.info("ALL PIPELINE PHASES EXECUTED AND COMMITTED SUCCESSFULLY!")
    logger.info(
        "Run 'mlflow ui' in your terminal to see your registered model parameters."
    )
