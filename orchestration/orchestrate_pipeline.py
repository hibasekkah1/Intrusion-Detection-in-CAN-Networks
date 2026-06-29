import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud import dataproc_v1


class CloudLoggingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "severity":  record.levelname,
            "message":   record.getMessage(),
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "logger":    record.name,
            "labels": {
                "pipeline":   "can-ids-bqnative",
                "component":  "orchestrator",
                "project_id": os.getenv("PROJECT_ID", ""),
            },
        }
        return json.dumps(entry, ensure_ascii=False)


def setup_logging() -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    if os.getenv("K_SERVICE"):
        handler.setFormatter(CloudLoggingFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S"
        ))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    return logging.getLogger("orchestrator")


logger = setup_logging()



PROJECT_ID   = os.getenv("PROJECT_ID",   "project-e6de9b55-41d5-4f13-ae0")
REGION       = os.getenv("REGION",       "europe-southwest1")
ZONE         = os.getenv("ZONE",         "europe-southwest1-a")

CLUSTER_NAME    = os.getenv("CLUSTER_NAME",    "can-ids-spark-cluster")
BUCKET_NAME     = os.getenv("BUCKET_NAME",     "can-ids-data-bqnative")
SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT", f"can-ids-sa@{PROJECT_ID}.iam.gserviceaccount.com")

CONFIG_URI     = os.getenv("CONFIG_URI",     f"gs://{BUCKET_NAME}/config/config-native-bigquery.yml")
SPARK_JOBS_URI = os.getenv("SPARK_JOBS_URI", f"gs://{BUCKET_NAME}/spark_jobs")
BIGQUERY_JAR   = os.getenv("BIGQUERY_JAR",   "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
COMMON_PY      = f"{SPARK_JOBS_URI.rstrip('/')}/common_bq.py"

DELETE_CLUSTER_AT_END  = os.getenv("DELETE_CLUSTER_AT_END",  "true").lower()  == "true"
RUN_BRONZE_ANALYTICS   = os.getenv("RUN_BRONZE_ANALYTICS",   "true").lower()  == "true"
RUN_SILVER_ANALYTICS   = os.getenv("RUN_SILVER_ANALYTICS",   "true").lower()  == "true"
RUN_GOLD_ANALYTICS     = os.getenv("RUN_GOLD_ANALYTICS",     "true").lower()  == "true"
RUN_BRONZE_ML          = os.getenv("RUN_BRONZE_ML",          "true").lower()  == "true"
RUN_SILVER_ML_RAW      = os.getenv("RUN_SILVER_ML_RAW",      "true").lower()  == "true"
RUN_SILVER_ML_SIGNAL   = os.getenv("RUN_SILVER_ML_SIGNAL",   "true").lower()  == "true"
RUN_GOLD_ML            = os.getenv("RUN_GOLD_ML",            "true").lower()  == "true"
RUN_QUALITY_CHECKS     = os.getenv("RUN_QUALITY_CHECKS",     "true").lower()  == "true"

ATTACK_TYPES = os.getenv("ATTACK_TYPES", "benign,fabr,fuzz,masq,repl,susp").split(",")

MASTER_MACHINE_TYPE = os.getenv("MASTER_MACHINE_TYPE", "e2-standard-4")
WORKER_MACHINE_TYPE = os.getenv("WORKER_MACHINE_TYPE", "e2-standard-4")
NUM_WORKERS         = int(os.getenv("NUM_WORKERS",      "1"))
MASTER_DISK_GB      = int(os.getenv("MASTER_DISK_GB",   "100"))
WORKER_DISK_GB      = int(os.getenv("WORKER_DISK_GB",   "100"))
IMAGE_VERSION       = os.getenv("DATAPROC_IMAGE_VERSION", "2.1-debian11")

SPARK_PROPERTIES = {
    "spark.dynamicAllocation.enabled":   os.getenv("SPARK_DYNAMIC_ALLOCATION",     "false"),
    "spark.executor.instances":           os.getenv("SPARK_EXECUTOR_INSTANCES",     "1"),
    "spark.executor.cores":               os.getenv("SPARK_EXECUTOR_CORES",         "1"),
    "spark.executor.memory":              os.getenv("SPARK_EXECUTOR_MEMORY",        "3g"),
    "spark.driver.memory":                os.getenv("SPARK_DRIVER_MEMORY",          "3g"),
    "spark.executor.memoryOverhead":      os.getenv("SPARK_EXECUTOR_MEMORY_OVERHEAD","1024"),
    "spark.driver.memoryOverhead":        os.getenv("SPARK_DRIVER_MEMORY_OVERHEAD", "1024"),
    "spark.sql.shuffle.partitions":       os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"),
    "spark.default.parallelism":          os.getenv("SPARK_DEFAULT_PARALLELISM",    "8"),
    "spark.task.maxFailures":             os.getenv("SPARK_TASK_MAX_FAILURES",      "10"),
}


# ═══════════════════════════════════════════════════════════════
# DATAPROC — cluster
# ═══════════════════════════════════════════════════════════════

def _cluster_client() -> dataproc_v1.ClusterControllerClient:
    return dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )


def _job_client() -> dataproc_v1.JobControllerClient:
    return dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )


def cluster_exists() -> bool:
    try:
        _cluster_client().get_cluster(
            request={"project_id": PROJECT_ID, "region": REGION, "cluster_name": CLUSTER_NAME}
        )
        return True
    except NotFound:
        return False


def create_cluster_if_needed() -> None:
    if cluster_exists():
        logger.info("Cluster already exists: %s", CLUSTER_NAME)
        return

    logger.info("Creating Dataproc cluster: %s", CLUSTER_NAME)
    cluster = {
        "project_id":   PROJECT_ID,
        "cluster_name": CLUSTER_NAME,
        "config": {
            "config_bucket": BUCKET_NAME,
            "gce_cluster_config": {
                "zone_uri":        ZONE,
                "service_account": SERVICE_ACCOUNT,
            },
            "master_config": {
                "num_instances":    1,
                "machine_type_uri": MASTER_MACHINE_TYPE,
                "disk_config":      {"boot_disk_size_gb": MASTER_DISK_GB},
            },
            "worker_config": {
                "num_instances":    NUM_WORKERS,
                "machine_type_uri": WORKER_MACHINE_TYPE,
                "disk_config":      {"boot_disk_size_gb": WORKER_DISK_GB},
            },
            "software_config": {
                "image_version": IMAGE_VERSION,
                "properties": {
                    "spark:spark.sql.shuffle.partitions":       SPARK_PROPERTIES["spark.sql.shuffle.partitions"],
                    "spark:spark.default.parallelism":          SPARK_PROPERTIES["spark.default.parallelism"],
                    "spark:spark.dynamicAllocation.enabled":    SPARK_PROPERTIES["spark.dynamicAllocation.enabled"],
                },
            },
        },
    }
    try:
        _cluster_client().create_cluster(
            request={"project_id": PROJECT_ID, "region": REGION, "cluster": cluster}
        ).result()
        logger.info("Cluster created: %s", CLUSTER_NAME)
    except AlreadyExists:
        logger.info("Cluster already created by concurrent process: %s", CLUSTER_NAME)


def delete_cluster() -> None:
    if not DELETE_CLUSTER_AT_END:
        logger.info("DELETE_CLUSTER_AT_END=false — cluster kept alive.")
        return
    try:
        logger.info("Deleting cluster: %s", CLUSTER_NAME)
        _cluster_client().delete_cluster(
            request={"project_id": PROJECT_ID, "region": REGION, "cluster_name": CLUSTER_NAME}
        ).result()
        logger.info("Cluster deleted: %s", CLUSTER_NAME)
    except NotFound:
        logger.warning("Cluster not found during deletion: %s", CLUSTER_NAME)


# ═══════════════════════════════════════════════════════════════
# DATAPROC — jobs
# ═══════════════════════════════════════════════════════════════

def _build_job(step_label: str, script_name: str, args: List[str]) -> Dict:
    return {
        "reference":  {"project_id": PROJECT_ID},
        "placement":  {"cluster_name": CLUSTER_NAME},
        "labels": {
            "pipeline": "can-ids-bqnative",
            "step":     step_label.replace("_", "-")[:63],
        },
        "pyspark_job": {
            "main_python_file_uri": f"{SPARK_JOBS_URI.rstrip('/')}/{script_name}",
            "python_file_uris":     [COMMON_PY],
            "jar_file_uris":        [BIGQUERY_JAR],
            "args":                 args,
            "properties":           SPARK_PROPERTIES,
        },
    }


def _wait_for_job(job_id: str, step: str) -> None:
    client = _job_client()
    while True:
        job   = client.get_job(
            request={"project_id": PROJECT_ID, "region": REGION, "job_id": job_id}
        )
        state = job.status.state.name
        logger.info("Dataproc job %s [%s] state=%s", job_id, step, state)
        if state == "DONE":
            return
        if state in {"ERROR", "CANCELLED"}:
            raise RuntimeError(
                f"Dataproc job {job_id} [{step}] failed: state={state} | {job.status.details}"
            )
        time.sleep(30)


def submit_step(step_label: str, script_name: str, args: List[str]) -> float:
    """
    Soumet un job Dataproc, attend sa complétion.
    Retourne la durée en secondes.
    """
    t0 = time.time()
    logger.info(">>> STEP START: %s | script=%s", step_label, script_name)

    submitted = _job_client().submit_job(
        request={
            "project_id": PROJECT_ID,
            "region":     REGION,
            "job":        _build_job(step_label, script_name, args),
        }
    )
    job_id = submitted.reference.job_id
    logger.info("Job submitted: job_id=%s step=%s", job_id, step_label)

    _wait_for_job(job_id, step_label)
    duration = time.time() - t0
    logger.info("<<< STEP OK: %s (%.1fs)", step_label, duration)
    return duration


# ═══════════════════════════════════════════════════════════════
# PIPELINE
# ═══════════════════════════════════════════════════════════════

def run_pipeline(only_step: Optional[str] = None) -> None:
    """
    Exécute le pipeline complet ou un seul step si only_step est fourni.

    Ordre :
      Analytics : Bronze → Silver → Gold
      ML        : Bronze → Silver RAW → Silver SIGNAL → Gold
      Quality   : checks
    """
    t_global = time.time()
    logger.info(
        "=== PIPELINE START | project=%s region=%s cluster=%s ===",
        PROJECT_ID, REGION, CLUSTER_NAME,
    )

    steps_ok     = []
    steps_failed = []

    def run(label: str, script: str, args: List[str]) -> None:
        """Lance un step si activé (ou si c'est le step ciblé)."""
        if only_step and only_step != label:
            return
        try:
            duration = submit_step(label, script, args)
            steps_ok.append((label, duration))
        except Exception as exc:
            logger.error("STEP FAILED [%s]: %s", label, exc, exc_info=True)
            steps_failed.append(label)
            raise   # stoppe le pipeline dès un échec critique

    try:
        create_cluster_if_needed()

        # ── Analytics ──────────────────────────────────────────
        if RUN_BRONZE_ANALYTICS:
            run("bronze-analytics",
                "01_bronze_analytics_ingestion.py",
                ["--config_path", CONFIG_URI])

        if RUN_SILVER_ANALYTICS:
            run("silver-analytics",
                "02_silver_analytics_clean.py",
                ["--config_path", CONFIG_URI])

        if RUN_GOLD_ANALYTICS:
            run("gold-analytics",
                "03_gold_analytics_window_5min_wide.py",
                ["--config_path", CONFIG_URI, "--attack_type", "all"])

        # ── ML ─────────────────────────────────────────────────
        if RUN_BRONZE_ML:
            run("bronze-ml",
                "01_bronze_ml_ingestion.py",
                ["--config_path", CONFIG_URI])

        if RUN_SILVER_ML_RAW:
            for attack in ATTACK_TYPES:
                run(f"silver-ml-raw-{attack}",
                    "02_silver_ml_clean.py",
                    ["--config_path", CONFIG_URI,
                     "--representation", "raw",
                     "--attack_type", attack])

        if RUN_SILVER_ML_SIGNAL:
            for attack in ATTACK_TYPES:
                run(f"silver-ml-signal-{attack}",
                    "02_silver_ml_clean.py",
                    ["--config_path", CONFIG_URI,
                     "--representation", "signal",
                     "--attack_type", attack])

        if RUN_GOLD_ML:
            run("gold-ml",
                "03_gold_ml_signal_big_table.py",
                ["--config_path", CONFIG_URI])

        # ── Quality ────────────────────────────────────────────
        if RUN_QUALITY_CHECKS:
            run("quality-checks",
                "05_quality_checks_datamesh.py",
                ["--config_path", CONFIG_URI])

    finally:
        delete_cluster()

    # ── Résumé
    duration = time.time() - t_global
    logger.info("=== PIPELINE SUMMARY | total=%.1fs ===", duration)
    for label, dur in steps_ok:
        logger.info("  %-40s %.1fs", label, dur)
    for label in steps_failed:
        logger.error("  ❌ %s", label)

    if steps_failed:
        logger.error("Pipeline finished WITH ERRORS: %s", steps_failed)
        sys.exit(1)
    else:
        logger.info("Pipeline finished successfully in %.1fs", duration)


# ═══════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="CAN IDS Pipeline Orchestrator — Cloud Run Job")
    parser.add_argument("--run-pipeline", action="store_true",
                        help="Lancer le pipeline complet")
    parser.add_argument("--step", type=str, default=None,
                        help="Lancer un seul step (ex: gold-ml, bronze-analytics)")
    args = parser.parse_args()

    if args.run_pipeline or os.getenv("K_SERVICE"):
        run_pipeline(only_step=args.step)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()