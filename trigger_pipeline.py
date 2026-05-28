import subprocess
import sys
import time


def run_script(script_path, description):
    print("\n" + "=" * 80)
    print(f"🚀 STARTING PHASE: {description}")
    print(f"📁 Execution Target: {script_path}")
    print("=" * 80)

    start_time = time.time()

    process = subprocess.Popen([sys.executable, script_path], stdout=None, stderr=None)
    process.communicate()

    elapsed_time = time.time() - start_time

    if process.returncode != 0:
        print(
            f"\n❌ [CRITICAL FAILURE]: {description} failed with exit code {process.returncode}."
        )
        print("⛔ Orchestrator halted. Check logs above for errors.")
        sys.exit(process.returncode)

    print(f"✅ [SUCCESS]: {description} completed in {elapsed_time:.2f} seconds.")


if __name__ == "__main__":
    print("=====================================================================")
    print("🤖 CORE LAKEHOUSE & MACHINE LEARNING ORCHESTRATION PIPELINE ENGINE")
    print("=====================================================================")

    # Phase 1: Mock Data Generation and S3 Partition Syncing
    run_script(
        "./mock_data_generator/generate_mock_data.py", "Data Generation & AWS S3 Upload"
    )

    # Phase 2: Ingestion, Quality Checks, LLM Enrichment, and Iceberg Upsert
    run_script("./src/main.py", "Lakehouse Ingestion & Iceberg Merge (ETL)")

    # Phase 3: Spark ML Training and MLflow Registration
    run_script(
        "./src/train_ml_pipeline.py", "Feature Engineering & Spark ML Model Training"
    )

    print("\n" + "🎉" * 30)
    print("🔥 ALL PIPELINE PHASES EXECUTED AND COMMITTED SUCCESSFULLY!")
    print(
        "👉 Run 'mlflow ui' in your terminal to see your registered model parameters."
    )
    print("🎉" * 30)
