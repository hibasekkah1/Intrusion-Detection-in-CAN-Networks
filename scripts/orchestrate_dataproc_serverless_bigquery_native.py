import os
import subprocess
import sys
from pathlib import Path

PROJECT_ID = os.getenv("PROJECT_ID", "project-e6de9b55-41d5-4f13-ae0")
REGION = os.getenv("REGION", "europe-southwest1")
DEPS_BUCKET = os.getenv("DEPS_BUCKET", "can-ids-data")
CONFIG_URI = os.getenv("CONFIG_URI", "gs://can-ids-data/config/config.yml")
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "all").lower()
RUNTIME_VERSION = os.getenv("DATAPROC_RUNTIME_VERSION", "2.1")


def run_local_python(script: str):
    print(f"Running local step: {script}", flush=True)
    cmd = [sys.executable, script, "--config_path", CONFIG_URI]
    subprocess.run(cmd, check=True)


def run_batch(name: str, script: str):
    batch = f"{name}-{os.getpid()}".lower().replace("_", "-")[:60]
    print(f"Submitting Dataproc Serverless batch: {name} -> {script}", flush=True)
    cmd = [
        "gcloud", "dataproc", "batches", "submit", "pyspark", script,
        f"--project={PROJECT_ID}",
        f"--region={REGION}",
        f"--batch={batch}",
        f"--deps-bucket={DEPS_BUCKET}",
        f"--version={RUNTIME_VERSION}",
        "--",
        "--config_path", CONFIG_URI,
    ]
    subprocess.run(cmd, check=True)


def analytics_steps():
    run_batch("bronze-analytics", "spark_jobs/01_bronze_analytics_ingestion.py")
    run_batch("silver-analytics", "spark_jobs/02_silver_analytics_clean.py")
    run_batch("gold-analytics", "spark_jobs/03_gold_analytics_signal_window_5min.py")


def ml_steps():
    run_batch("bronze-ml", "spark_jobs/01_bronze_ml_ingestion.py")
    run_batch("silver-ml", "spark_jobs/02_silver_ml_clean.py")
    run_batch("gold-ml", "spark_jobs/03_gold_ml_signal_big_table.py")


def main():
    run_local_python("spark_jobs/00_create_bigquery_native_schema.py")
    if PIPELINE_MODE in {"all", "analytics"}:
        analytics_steps()
    if PIPELINE_MODE in {"all", "ml"}:
        ml_steps()
    run_batch("quality-checks", "spark_jobs/05_quality_checks_datamesh.py")
    print("Pipeline completed successfully", flush=True)


if __name__ == "__main__":
    main()
