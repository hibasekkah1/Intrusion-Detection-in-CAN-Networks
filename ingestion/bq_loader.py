from pathlib import Path

from google.cloud import bigquery
from google.api_core.exceptions import NotFound


def ensure_dataset_exists(client, project_id, dataset_id, location):
    dataset_ref = f"{project_id}.{dataset_id}"

    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)


def ensure_messages_raw_exists(client, project_id, dataset_id, table_id):
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    schema = [
        bigquery.SchemaField("session_id", "STRING"),
        bigquery.SchemaField("timestamp_us", "INT64"),
        bigquery.SchemaField("arbitration_id", "INT64"),
        bigquery.SchemaField("dlc", "INT64"),
        bigquery.SchemaField("data", "BYTES"),
        bigquery.SchemaField("label", "INT64"),
        bigquery.SchemaField("source_file", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    try:
        client.get_table(full_table_id)
    except NotFound:
        table = bigquery.Table(full_table_id, schema=schema)
        client.create_table(table)


def ensure_messages_rejected_exists(client, project_id, dataset_id, table_id):
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    schema = [
        bigquery.SchemaField("session_id", "STRING"),
        bigquery.SchemaField("timestamp_us", "INT64"),
        bigquery.SchemaField("arbitration_id", "INT64"),
        bigquery.SchemaField("dlc", "INT64"),
        bigquery.SchemaField("data", "BYTES"),
        bigquery.SchemaField("label", "INT64"),
        bigquery.SchemaField("source_file", "STRING"),
        bigquery.SchemaField("rejection_reason", "STRING"),
        bigquery.SchemaField("rejected_at", "TIMESTAMP"),
    ]

    try:
        client.get_table(full_table_id)
    except NotFound:
        table = bigquery.Table(full_table_id, schema=schema)
        client.create_table(table)


def ensure_pipeline_runs_exists(client, project_id, dataset_id, table_id):
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    schema = [
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("pipeline_name", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("started_at", "TIMESTAMP"),
        bigquery.SchemaField("finished_at", "TIMESTAMP"),
        bigquery.SchemaField("files_processed", "INT64"),
        bigquery.SchemaField("files_skipped", "INT64"),
        bigquery.SchemaField("rows_loaded", "INT64"),
        bigquery.SchemaField("rows_rejected", "INT64"),
        bigquery.SchemaField("error_message", "STRING"),
    ]

    try:
        client.get_table(full_table_id)
    except NotFound:
        table = bigquery.Table(full_table_id, schema=schema)
        client.create_table(table)


def file_already_loaded(client, raw_table_id, source_file, location):
    query = f"""
    SELECT COUNT(*) AS row_count
    FROM `{raw_table_id}`
    WHERE source_file = @source_file
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_file", "STRING", source_file)
        ]
    )

    result = client.query(
        query,
        job_config=job_config,
        location=location,
    ).result()

    for row in result:
        return row.row_count > 0

    return False


def insert_pipeline_run(
    client,
    pipeline_table_id,
    run_id,
    pipeline_name,
    status,
    started_at,
    finished_at,
    files_processed,
    files_skipped,
    rows_loaded,
    rows_rejected,
    error_message,
    location,
):
    query = f"""
    INSERT INTO `{pipeline_table_id}` (
        run_id,
        pipeline_name,
        status,
        started_at,
        finished_at,
        files_processed,
        files_skipped,
        rows_loaded,
        rows_rejected,
        error_message
    )
    VALUES (
        @run_id,
        @pipeline_name,
        @status,
        @started_at,
        @finished_at,
        @files_processed,
        @files_skipped,
        @rows_loaded,
        @rows_rejected,
        @error_message
    )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("pipeline_name", "STRING", pipeline_name),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", started_at),
            bigquery.ScalarQueryParameter("finished_at", "TIMESTAMP", finished_at),
            bigquery.ScalarQueryParameter("files_processed", "INT64", files_processed),
            bigquery.ScalarQueryParameter("files_skipped", "INT64", files_skipped),
            bigquery.ScalarQueryParameter("rows_loaded", "INT64", rows_loaded),
            bigquery.ScalarQueryParameter("rows_rejected", "INT64", rows_rejected),
            bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
        ]
    )

    job = client.query(
        query,
        job_config=job_config,
        location=location,
    )

    job.result()


def initialize_bigquery(config):
    project_id = config["gcp"]["project_id"]
    location = config["bigquery"]["location"]

    bronze_dataset = config["bigquery"]["datasets"]["bronze"]

    raw_table = config["bigquery"]["tables"]["bronze_raw"]
    rejected_table = config["bigquery"]["tables"]["bronze_rejected"]
    pipeline_table = config["bigquery"]["tables"]["pipeline_runs"]

    client = bigquery.Client(
        project=project_id,
        location=location,
    )

    ensure_dataset_exists(
        client=client,
        project_id=project_id,
        dataset_id=bronze_dataset,
        location=location,
    )

    ensure_messages_raw_exists(
        client=client,
        project_id=project_id,
        dataset_id=bronze_dataset,
        table_id=raw_table,
    )

    ensure_messages_rejected_exists(
        client=client,
        project_id=project_id,
        dataset_id=bronze_dataset,
        table_id=rejected_table,
    )

    ensure_pipeline_runs_exists(
        client=client,
        project_id=project_id,
        dataset_id=bronze_dataset,
        table_id=pipeline_table,
    )

    return client


def load_gcs_parquet_to_bronze(bucket_name, blob_name, config):
    project_id = config["gcp"]["project_id"]
    location = config["bigquery"]["location"]

    bronze_dataset = config["bigquery"]["datasets"]["bronze"]

    raw_table = config["bigquery"]["tables"]["bronze_raw"]
    rejected_table = config["bigquery"]["tables"]["bronze_rejected"]

    skip_existing_files = config["ingestion"].get("skip_existing_files", True)

    client = bigquery.Client(
        project=project_id,
        location=location,
    )

    session_id = Path(blob_name).stem

    staging_table = f"stg_{session_id}"
    staging_table = staging_table.replace("-", "_").replace(".", "_")

    staging_table_id = f"{project_id}.{bronze_dataset}.{staging_table}"
    raw_table_id = f"{project_id}.{bronze_dataset}.{raw_table}"
    rejected_table_id = f"{project_id}.{bronze_dataset}.{rejected_table}"

    if skip_existing_files and file_already_loaded(
        client=client,
        raw_table_id=raw_table_id,
        source_file=blob_name,
        location=location,
    ):
        return {
            "status": "SKIPPED",
            "rows_loaded": 0,
            "rows_rejected": 0,
            "message": f"Fichier déjà chargé : {blob_name}",
        }

    gcs_uri = f"gs://{bucket_name}/{blob_name}"

    load_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        staging_table_id,
        job_config=load_config,
        location=location,
    )

    load_job.result()

    query_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("session_id", "STRING", session_id),
            bigquery.ScalarQueryParameter("source_file", "STRING", blob_name),
        ]
    )

    insert_valid_query = f"""
    INSERT INTO `{raw_table_id}` (
        session_id,
        timestamp_us,
        arbitration_id,
        dlc,
        data,
        label,
        source_file,
        ingested_at
    )
    SELECT
        @session_id AS session_id,
        CAST(timestamp AS INT64) AS timestamp_us,
        CAST(arbitration_id AS INT64) AS arbitration_id,
        CAST(dlc AS INT64) AS dlc,
        data,
        CAST(label AS INT64) AS label,
        @source_file AS source_file,
        CURRENT_TIMESTAMP() AS ingested_at
    FROM `{staging_table_id}`
    WHERE timestamp IS NOT NULL
      AND arbitration_id IS NOT NULL
      AND arbitration_id BETWEEN 0 AND 2047
      AND dlc IS NOT NULL
      AND dlc BETWEEN 0 AND 8
      AND label IS NOT NULL
      AND label IN (0, 1)
      AND data IS NOT NULL
    """

    valid_job = client.query(
        insert_valid_query,
        job_config=query_config,
        location=location,
    )

    valid_job.result()

    insert_rejected_query = f"""
    INSERT INTO `{rejected_table_id}` (
        session_id,
        timestamp_us,
        arbitration_id,
        dlc,
        data,
        label,
        source_file,
        rejection_reason,
        rejected_at
    )
    SELECT
        @session_id AS session_id,
        SAFE_CAST(timestamp AS INT64) AS timestamp_us,
        SAFE_CAST(arbitration_id AS INT64) AS arbitration_id,
        SAFE_CAST(dlc AS INT64) AS dlc,
        data,
        SAFE_CAST(label AS INT64) AS label,
        @source_file AS source_file,
        CASE
            WHEN timestamp IS NULL THEN 'timestamp_null'
            WHEN arbitration_id IS NULL THEN 'arbitration_id_null'
            WHEN arbitration_id < 0 OR arbitration_id > 2047 THEN 'arbitration_id_out_of_range'
            WHEN dlc IS NULL THEN 'dlc_null'
            WHEN dlc < 0 OR dlc > 8 THEN 'dlc_out_of_range'
            WHEN label IS NULL THEN 'label_null'
            WHEN label NOT IN (0, 1) THEN 'label_invalid'
            WHEN data IS NULL THEN 'data_null'
            ELSE 'unknown_reason'
        END AS rejection_reason,
        CURRENT_TIMESTAMP() AS rejected_at
    FROM `{staging_table_id}`
    WHERE timestamp IS NULL
       OR arbitration_id IS NULL
       OR arbitration_id < 0
       OR arbitration_id > 2047
       OR dlc IS NULL
       OR dlc < 0
       OR dlc > 8
       OR label IS NULL
       OR label NOT IN (0, 1)
       OR data IS NULL
    """

    rejected_job = client.query(
        insert_rejected_query,
        job_config=query_config,
        location=location,
    )

    rejected_job.result()

    client.delete_table(
        staging_table_id,
        not_found_ok=True,
    )

    return {
        "status": "LOADED",
        "rows_loaded": valid_job.num_dml_affected_rows or 0,
        "rows_rejected": rejected_job.num_dml_affected_rows or 0,
        "message": f"Fichier traité : {blob_name}",
    }