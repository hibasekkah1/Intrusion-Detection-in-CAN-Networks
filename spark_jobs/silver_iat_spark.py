import argparse
import yaml
from uuid import uuid4
from datetime import datetime, timezone

from google.cloud import storage
from google.cloud import bigquery

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lag,
    row_number,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_path", required=True)
    return parser.parse_args()


def read_gcs_text(gcs_uri):
    path = gcs_uri.replace("gs://", "")
    bucket_name = path.split("/", 1)[0]
    blob_name = path.split("/", 1)[1]

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    return blob.download_as_text()


def load_config(config_path):
    if config_path.startswith("gs://"):
        return yaml.safe_load(read_gcs_text(config_path))

    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def create_spark_session(config):
    project_id = config["gcp"]["project_id"]
    temp_bucket = config["gcp"]["temp_bucket"]

    spark = (
        SparkSession.builder
        .appName("can-ids-silver-iat-spark")
        .config("spark.sql.shuffle.partitions", "400")
        .config("temporaryGcsBucket", temp_bucket)
        .getOrCreate()
    )

    spark.conf.set("parentProject", project_id)

    return spark


def bigquery_table(config, dataset_key, table_key):
    project_id = config["gcp"]["project_id"]
    dataset = config["bigquery"]["datasets"][dataset_key]
    table = config["bigquery"]["tables"][table_key]

    return f"{project_id}.{dataset}.{table}"


def read_from_bigquery(spark, config, dataset_key, table_key):
    table_id = bigquery_table(config, dataset_key, table_key)

    return (
        spark.read
        .format("bigquery")
        .option("table", table_id)
        .load()
    )


def write_to_bigquery(df, config, dataset_key, table_key, mode="append"):
    table_id = bigquery_table(config, dataset_key, table_key)
    temp_bucket = config["gcp"]["temp_bucket"]

    (
        df.write
        .format("bigquery")
        .option("table", table_id)
        .option("temporaryGcsBucket", temp_bucket)
        .mode(mode)
        .save()
    )


def get_sessions_to_process(config):
    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, "audit", "file_processing_status")

    query = f"""
    SELECT DISTINCT session_id
    FROM `{table_id}`
    WHERE silver_clean_status = 'SUCCESS'
      AND COALESCE(silver_iat_status, 'PENDING') != 'SUCCESS'
      AND session_id IS NOT NULL
      AND session_id != ''
    """

    client = bigquery.Client(project=project_id)
    rows = client.query(query).result()

    return [row.session_id for row in rows]


def get_files_to_update(config):
    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, "audit", "file_processing_status")

    query = f"""
    SELECT DISTINCT source_file
    FROM `{table_id}`
    WHERE silver_clean_status = 'SUCCESS'
      AND COALESCE(silver_iat_status, 'PENDING') != 'SUCCESS'
      AND source_file IS NOT NULL
    """

    client = bigquery.Client(project=project_id)
    rows = client.query(query).result()

    return [row.source_file for row in rows]


def delete_existing_iat_rows(config, session_ids):
    if not session_ids:
        return

    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, "silver", "silver_iat")

    query = f"""
    DELETE FROM `{table_id}`
    WHERE session_id IN UNNEST(@session_ids)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("session_ids", "STRING", session_ids)
        ]
    )

    client = bigquery.Client(project=project_id)
    client.query(query, job_config=job_config).result()


def update_file_status_success(config, source_files, run_id):
    if not source_files:
        return

    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, "audit", "file_processing_status")

    query = f"""
    UPDATE `{table_id}`
    SET
      silver_iat_status = 'SUCCESS',
      gold_window_status = 'PENDING',
      can_id_profile_status = 'PENDING',
      run_id = @run_id,
      updated_at = CURRENT_TIMESTAMP(),
      error_message = NULL
    WHERE source_file IN UNNEST(@source_files)
      AND silver_clean_status = 'SUCCESS'
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("source_files", "STRING", source_files),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )

    client = bigquery.Client(project=project_id)
    client.query(query, job_config=job_config).result()


def update_file_status_failed(config, source_files, run_id, error_message):
    if not source_files:
        return

    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, "audit", "file_processing_status")

    query = f"""
    UPDATE `{table_id}`
    SET
      silver_iat_status = 'FAILED',
      run_id = @run_id,
      updated_at = CURRENT_TIMESTAMP(),
      error_message = @error_message
    WHERE source_file IN UNNEST(@source_files)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("source_files", "STRING", source_files),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
        ]
    )

    client = bigquery.Client(project=project_id)
    client.query(query, job_config=job_config).result()


def create_messages_with_iat(messages_clean_df):
    iat_window = (
        Window
        .partitionBy("session_id", "arbitration_id")
        .orderBy("timestamp_us")
    )

    return (
        messages_clean_df
        .withColumn(
            "iat_us",
            col("timestamp_us") - lag("timestamp_us").over(iat_window)
        )
        .withColumn(
            "seq_num",
            row_number().over(iat_window)
        )
        .select(
            "message_id",
            "session_id",
            "timestamp_us",
            "arbitration_id",
            "aid_hex",
            "dlc",
            "data",
            "label",
            "byte_0",
            "byte_1",
            "byte_2",
            "byte_3",
            "byte_4",
            "byte_5",
            "byte_6",
            "byte_7",
            "zero_byte_count",
            "zeros_ratio",
            "iat_us",
            "seq_num",
            "source_file",
            "ingested_at",
        )
    )


def create_pipeline_run_df(
    spark,
    run_id,
    step_name,
    status,
    started_at,
    input_rows,
    output_rows,
    files_processed,
    error_message,
):
    finished_at = datetime.now(timezone.utc)
    duration_seconds = (finished_at - started_at).total_seconds()

    schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("step_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("finished_at", TimestampType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("input_rows", LongType(), True),
        StructField("output_rows", LongType(), True),
        StructField("rejected_rows", LongType(), True),
        StructField("files_processed", LongType(), True),
        StructField("files_skipped", LongType(), True),
        StructField("source_layer", StringType(), True),
        StructField("target_layer", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ])

    data = [(
        run_id,
        "can_ids_full_cloud_pipeline",
        step_name,
        status,
        started_at,
        finished_at,
        duration_seconds,
        input_rows,
        output_rows,
        0,
        files_processed,
        0,
        "Silver.messages_clean",
        "Silver.messages_with_iat",
        error_message,
        datetime.now(timezone.utc),
    )]

    return spark.createDataFrame(data, schema)


def write_pipeline_run(
    spark,
    config,
    run_id,
    step_name,
    status,
    started_at,
    input_rows,
    output_rows,
    files_processed,
    error_message,
):
    run_df = create_pipeline_run_df(
        spark=spark,
        run_id=run_id,
        step_name=step_name,
        status=status,
        started_at=started_at,
        input_rows=input_rows,
        output_rows=output_rows,
        files_processed=files_processed,
        error_message=error_message,
    )

    write_to_bigquery(
        df=run_df,
        config=config,
        dataset_key="audit",
        table_key="pipeline_runs",
        mode="append",
    )


def run():
    args = parse_args()
    config = load_config(args.config_path)

    spark = create_spark_session(config)

    run_id = f"run_{uuid4()}"
    step_name = "silver_iat_spark"
    started_at = datetime.now(timezone.utc)

    input_rows = 0
    output_rows = 0
    session_ids = []
    source_files = []

    try:
        session_ids = get_sessions_to_process(config)
        source_files = get_files_to_update(config)

        if not session_ids:
            write_pipeline_run(
                spark=spark,
                config=config,
                run_id=run_id,
                step_name=step_name,
                status="SUCCESS",
                started_at=started_at,
                input_rows=0,
                output_rows=0,
                files_processed=0,
                error_message=None,
            )

            print("Aucune session à traiter pour Silver IAT.")
            return

        messages_clean_df = (
            read_from_bigquery(
                spark=spark,
                config=config,
                dataset_key="silver",
                table_key="silver_clean",
            )
            .filter(col("session_id").isin(session_ids))
        )

        input_rows = messages_clean_df.count()

        messages_with_iat_df = create_messages_with_iat(messages_clean_df)

        output_rows = messages_with_iat_df.count()

        delete_existing_iat_rows(config, session_ids)

        if output_rows > 0:
            write_to_bigquery(
                df=messages_with_iat_df,
                config=config,
                dataset_key="silver",
                table_key="silver_iat",
                mode="append",
            )

        update_file_status_success(config, source_files, run_id)

        write_pipeline_run(
            spark=spark,
            config=config,
            run_id=run_id,
            step_name=step_name,
            status="SUCCESS",
            started_at=started_at,
            input_rows=input_rows,
            output_rows=output_rows,
            files_processed=len(source_files),
            error_message=None,
        )

        print("Silver IAT terminé.")
        print(f"Sessions traitées : {len(session_ids)}")
        print(f"Fichiers mis à jour : {len(source_files)}")
        print(f"Lignes lues : {input_rows}")
        print(f"Lignes écrites : {output_rows}")

    except Exception as error:
        error_message = str(error)

        update_file_status_failed(config, source_files, run_id, error_message)

        write_pipeline_run(
            spark=spark,
            config=config,
            run_id=run_id,
            step_name=step_name,
            status="FAILED",
            started_at=started_at,
            input_rows=input_rows,
            output_rows=output_rows,
            files_processed=len(source_files),
            error_message=error_message,
        )

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    run()