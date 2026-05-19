import os
import time
from datetime import datetime, timezone

from google.api_core.exceptions import NotFound, AlreadyExists
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import dataproc_v1


PROJECT_ID = os.getenv("PROJECT_ID", "project-e6de9b55-41d5-4f13-ae0")
REGION = os.getenv("REGION", "europe-southwest1")
ZONE = os.getenv("ZONE", "europe-southwest1-a")

CLUSTER_NAME = os.getenv("CLUSTER_NAME", "can-ids-spark-cluster")

BUCKET_NAME = os.getenv("BUCKET_NAME", "can-ids-data")
RAW_PREFIX = os.getenv("RAW_PREFIX", "raw/")

CONFIG_URI = os.getenv("CONFIG_URI", "gs://can-ids-data/config/config.yml")
DBC_URI = os.getenv("DBC_URI", "gs://can-ids-data/dbc/hyundai_2015_ccan.dbc")

TEMP_BUCKET = os.getenv("TEMP_BUCKET", "can-ids-data")

INIT_ACTION_URI = os.getenv("INIT_ACTION_URI", "")
DELETE_CLUSTER_AT_END = os.getenv("DELETE_CLUSTER_AT_END", "true").lower() == "true"

AUDIT_FILE_STATUS_TABLE = f"{PROJECT_ID}.can_ids_audit.file_processing_status"


ANALYST_VIEW_SQL = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.can_ids_gold.vw_gold_features_analyst` AS
SELECT
  f.source_file,
  f.session_id,
  f.window_start_us,
  f.window_end_us,

  f.arbitration_id,
  f.aid_hex,

  p.message_name,
  p.signals_count,
  p.signal_names,
  p.messages_count AS total_messages_for_can_id,
  p.uniq_data_count,
  p.interval_mean AS global_interval_mean_sec,
  p.interval_std AS global_interval_std_sec,
  p.uniq_dlc AS global_uniq_dlc,

  f.label,
  f.msg_count,
  f.msg_per_sec,
  f.iat_mean_us,
  f.iat_std_us,
  f.iat_cv,
  f.entropy_bits,
  f.dlc_mean,
  f.dlc_std,
  f.dlc_distinct_count,
  f.avg_zeros_ratio,

  f.computed_at AS feature_computed_at,
  p.computed_at AS profile_computed_at

FROM `{PROJECT_ID}.can_ids_gold.gold_features_window` f
LEFT JOIN `{PROJECT_ID}.can_ids_gold.can_id_profile` p
ON f.arbitration_id = p.arbitration_id
"""


DATA_SCIENCE_PROFILE_BY_SESSION_SQL = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.can_ids_gold.vw_can_id_profile_by_session` AS
WITH dbc_ref AS (
  SELECT
    arbitration_id,
    ANY_VALUE(message_name) AS message_name,
    ANY_VALUE(signals_count) AS signals_count
  FROM `{PROJECT_ID}.can_ids_silver.dbc_messages_reference`
  GROUP BY arbitration_id
)

SELECT
  m.session_id,
  ANY_VALUE(m.source_file) AS source_file,

  m.arbitration_id,

  FORMAT('%03X', m.arbitration_id) AS aid_hex,

  COALESCE(d.message_name, '[N/A]') AS message_name,

  d.signals_count AS signals_count,

  COUNT(*) AS messages_count,

  COUNT(DISTINCT m.data) AS uniq_data_count,

  AVG(
    CASE
      WHEN m.iat_us IS NOT NULL THEN m.iat_us / 1000000.0
      ELSE NULL
    END
  ) AS interval_mean,

  STDDEV_SAMP(
    CASE
      WHEN m.iat_us IS NOT NULL THEN m.iat_us / 1000000.0
      ELSE NULL
    END
  ) AS interval_std,

  ARRAY_AGG(DISTINCT m.dlc ORDER BY m.dlc) AS uniq_dlc

FROM `{PROJECT_ID}.can_ids_silver.messages_with_iat` AS m

LEFT JOIN dbc_ref AS d
ON m.arbitration_id = d.arbitration_id

GROUP BY
  m.session_id,
  m.arbitration_id,
  d.message_name,
  d.signals_count
"""



def now_utc():
    return datetime.now(timezone.utc).isoformat()


def log(message):
    print(f"[{now_utc()}] {message}", flush=True)



def get_cluster_client():
    return dataproc_v1.ClusterControllerClient(
        client_options={
            "api_endpoint": f"{REGION}-dataproc.googleapis.com:443"
        }
    )


def get_job_client():
    return dataproc_v1.JobControllerClient(
        client_options={
            "api_endpoint": f"{REGION}-dataproc.googleapis.com:443"
        }
    )


def list_raw_parquet_files():
    storage_client = storage.Client(project=PROJECT_ID)
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=RAW_PREFIX)

    files = [
        blob.name
        for blob in blobs
        if blob.name.endswith(".parquet")
    ]

    return files


def get_successfully_bronzed_files():
    bq_client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    SELECT source_file
    FROM (
      SELECT
        source_file,
        bronze_status,
        updated_at,
        ROW_NUMBER() OVER (
          PARTITION BY source_file
          ORDER BY updated_at DESC
        ) AS rn
      FROM `{AUDIT_FILE_STATUS_TABLE}`
      WHERE source_file IS NOT NULL
    )
    WHERE rn = 1
      AND bronze_status = 'SUCCESS'
    """

    try:
        rows = bq_client.query(query, location=REGION).result()
        return {row.source_file for row in rows}
    except NotFound:
        return set()


def count_pending_or_failed_work():
    bq_client = bigquery.Client(project=PROJECT_ID)

    query = f"""
    SELECT COUNT(*) AS work_count
    FROM `{AUDIT_FILE_STATUS_TABLE}`
    WHERE bronze_status = 'SUCCESS'
      AND (
        COALESCE(silver_clean_status, 'PENDING') != 'SUCCESS'
        OR COALESCE(silver_iat_status, 'PENDING') != 'SUCCESS'
        OR COALESCE(gold_window_status, 'PENDING') != 'SUCCESS'
        OR COALESCE(can_id_profile_status, 'PENDING') != 'SUCCESS'
      )
    """

    try:
        rows = list(bq_client.query(query, location=REGION).result())
        if not rows:
            return 0
        return int(rows[0].work_count)
    except NotFound:
        return 0


def has_work_to_do():
    raw_files = list_raw_parquet_files()
    bronzed_files = get_successfully_bronzed_files()

    new_files = [
        source_file
        for source_file in raw_files
        if source_file not in bronzed_files
    ]

    pending_or_failed = count_pending_or_failed_work()

    log(f"Fichiers raw détectés : {len(raw_files)}")
    log(f"Nouveaux fichiers non Bronze SUCCESS : {len(new_files)}")
    log(f"Fichiers avec étapes downstream à traiter : {pending_or_failed}")

    return len(new_files) > 0 or pending_or_failed > 0


def cluster_exists():
    cluster_client = get_cluster_client()

    try:
        cluster_client.get_cluster(
            request={
                "project_id": PROJECT_ID,
                "region": REGION,
                "cluster_name": CLUSTER_NAME,
            }
        )
        return True
    except NotFound:
        return False


def build_cluster_config():
    cluster_config = {
        "gce_cluster_config": {
            "zone_uri": ZONE,
        },
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n2-standard-4",
            "disk_config": {
                "boot_disk_size_gb": 100,
            },
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n2-standard-4",
            "disk_config": {
                "boot_disk_size_gb": 100,
            },
        },
        "software_config": {
            "image_version": "2.2-debian12",
            "properties": {
                "spark:spark.sql.shuffle.partitions": "400",
            },
        },
    }

    if INIT_ACTION_URI:
        cluster_config["initialization_actions"] = [
            {
                "executable_file": INIT_ACTION_URI,
            }
        ]

    return cluster_config


def create_cluster_if_needed():
    if cluster_exists():
        log(f"Cluster Dataproc déjà existant : {CLUSTER_NAME}")
        return

    log(f"Création du cluster Dataproc : {CLUSTER_NAME}")

    cluster_client = get_cluster_client()

    cluster = {
        "project_id": PROJECT_ID,
        "cluster_name": CLUSTER_NAME,
        "config": build_cluster_config(),
    }

    try:
        operation = cluster_client.create_cluster(
            request={
                "project_id": PROJECT_ID,
                "region": REGION,
                "cluster": cluster,
            }
        )
        operation.result()
        log("Cluster créé avec succès.")
    except AlreadyExists:
        log("Cluster déjà créé par un autre processus.")


def delete_cluster():
    if not DELETE_CLUSTER_AT_END:
        log("DELETE_CLUSTER_AT_END=false, cluster conservé.")
        return

    cluster_client = get_cluster_client()

    try:
        log(f"Suppression du cluster Dataproc : {CLUSTER_NAME}")
        operation = cluster_client.delete_cluster(
            request={
                "project_id": PROJECT_ID,
                "region": REGION,
                "cluster_name": CLUSTER_NAME,
            }
        )
        operation.result()
        log("Cluster supprimé avec succès.")
    except NotFound:
        log("Cluster déjà supprimé ou inexistant.")


def build_pyspark_job(step_label, script_uri, args):
    return {
        "reference": {
            "project_id": PROJECT_ID,
        },
        "placement": {
            "cluster_name": CLUSTER_NAME,
        },
        "labels": {
            "pipeline": "can-ids",
            "step": step_label,
        },
        "pyspark_job": {
            "main_python_file_uri": script_uri,
            "args": args,
        },
    }


def wait_for_job(job_id):
    job_client = get_job_client()

    while True:
        job = job_client.get_job(
            request={
                "project_id": PROJECT_ID,
                "region": REGION,
                "job_id": job_id,
            }
        )

        state = job.status.state.name
        log(f"Job {job_id} state = {state}")

        if state == "DONE":
            return

        if state in {"ERROR", "CANCELLED"}:
            details = job.status.details
            raise RuntimeError(
                f"Dataproc job {job_id} failed with state={state}, details={details}"
            )

        time.sleep(30)


def submit_and_wait(step_label, script_uri, args):
    job_client = get_job_client()

    job = build_pyspark_job(
        step_label=step_label,
        script_uri=script_uri,
        args=args,
    )

    log(f"Soumission job : {step_label}")

    submitted_job = job_client.submit_job(
        request={
            "project_id": PROJECT_ID,
            "region": REGION,
            "job": job,
        }
    )

    job_id = submitted_job.reference.job_id

    log(f"Job soumis : {step_label}, job_id={job_id}")

    wait_for_job(job_id)

    log(f"Job terminé avec succès : {step_label}")


def run_bigquery_query(name, query):
    log(f"Exécution BigQuery : {name}")
    bq_client = bigquery.Client(project=PROJECT_ID)
    bq_client.query(query, location=REGION).result()
    log(f"BigQuery terminé : {name}")


def bigquery_table_exists(table_id):
    bq_client = bigquery.Client(project=PROJECT_ID)

    try:
        bq_client.get_table(table_id)
        return True
    except NotFound:
        return False


def create_bigquery_views():
    gold_features_table = f"{PROJECT_ID}.can_ids_gold.gold_features_window"
    can_id_profile_table = f"{PROJECT_ID}.can_ids_gold.can_id_profile"
    silver_iat_table = f"{PROJECT_ID}.can_ids_silver.messages_with_iat"
    dbc_reference_table = f"{PROJECT_ID}.can_ids_silver.dbc_messages_reference"

    # Vue analyste Power BI
    if (
        bigquery_table_exists(gold_features_table)
        and bigquery_table_exists(can_id_profile_table)
    ):
        run_bigquery_query(
            name="create_analyst_view",
            query=ANALYST_VIEW_SQL,
        )
    else:
        log(
            "Tables gold_features_window ou can_id_profile absentes. "
            "Création de vw_gold_features_analyst ignorée."
        )

    # Vue Data Science profil par session
    if (
        bigquery_table_exists(silver_iat_table)
        and bigquery_table_exists(dbc_reference_table)
    ):
        run_bigquery_query(
            name="create_data_science_profile_by_session_view",
            query=DATA_SCIENCE_PROFILE_BY_SESSION_SQL,
        )
    else:
        log(
            "Tables messages_with_iat ou dbc_messages_reference absentes. "
            "Création de vw_can_id_profile_by_session ignorée."
        )



def run_pipeline():
    log("Démarrage orchestrateur CAN IDS.")


    if not has_work_to_do():
        log("Aucun nouveau travail détecté. Création/mise à jour des vues BigQuery.")
        create_bigquery_views()
        return

    cluster_created_or_used = False

    try:
        create_cluster_if_needed()
        cluster_created_or_used = True

        submit_and_wait(
            step_label="bronze-ingestion-spark",
            script_uri="gs://can-ids-data/spark_jobs/bronze_ingestion_spark.py",
            args=[
                "--config_path",
                CONFIG_URI,
            ],
        )

        submit_and_wait(
            step_label="silver-clean-spark",
            script_uri="gs://can-ids-data/spark_jobs/silver_clean_spark.py",
            args=[
                "--config_path",
                CONFIG_URI,
            ],
        )

        submit_and_wait(
            step_label="silver-iat-spark",
            script_uri="gs://can-ids-data/spark_jobs/silver_iat_spark.py",
            args=[
                "--config_path",
                CONFIG_URI,
            ],
        )

        submit_and_wait(
            step_label="dbc-messages-reference-spark",
            script_uri="gs://can-ids-data/spark_jobs/dbc_messages_reference_spark.py",
            args=[
                "--config_path",
                CONFIG_URI,
                "--dbc_path",
                DBC_URI,
            ],
        )

        submit_and_wait(
            step_label="gold-features-window-spark",
            script_uri="gs://can-ids-data/spark_jobs/gold_features_window_spark.py",
            args=[
                "--config_path",
                CONFIG_URI,
            ],
        )

        submit_and_wait(
            step_label="can-id-profile-spark",
            script_uri="gs://can-ids-data/spark_jobs/can_id_profile_spark.py",
            args=[
                "--config_path",
                CONFIG_URI,
            ],
        )

        create_bigquery_views()


        submit_and_wait(
            step_label="gx-quality-spark",
            script_uri="gs://can-ids-data/spark_jobs/gx_quality_spark.py",
            args=[
                "--config_path",
                CONFIG_URI,
            ],
        )

        log("Pipeline CAN IDS terminé avec succès.")

    finally:
        if cluster_created_or_used:
            delete_cluster()


if __name__ == "__main__":
    run_pipeline()