import argparse
import yaml
from uuid import uuid4
from datetime import datetime, timezone

from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    md5,
    concat_ws,
    hex,
    format_string,
    expr,
    when,
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
        .appName("can-ids-silver-clean-spark")
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


def get_files_to_process(config):
    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, "audit", "file_processing_status")

    query = f"""
    SELECT DISTINCT source_file
    FROM `{table_id}`
    WHERE bronze_status = 'SUCCESS'
      AND COALESCE(silver_clean_status, 'PENDING') != 'SUCCESS'
      AND source_file IS NOT NULL
    """

    client = bigquery.Client(project=project_id)
    rows = client.query(query).result()

    return [row.source_file for row in rows]


def delete_existing_silver_rows(config, source_files):
    """
    Supprime les anciennes lignes Silver pour les fichiers à retraiter.
    Si la table Silver n'existe pas encore, on ignore le DELETE.
    Cela permet de repartir de zéro sans erreur 404.
    """
    if not source_files:
        return

    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, "silver", "silver_clean")

    client = bigquery.Client(project=project_id)

    try:
        client.get_table(table_id)
    except NotFound:
        print(f"Table {table_id} does not exist yet. Skipping delete.")
        return

    query = f"""
    DELETE FROM `{table_id}`
    WHERE source_file IN UNNEST(@source_files)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "source_files",
                "STRING",
                source_files,
            )
        ]
    )

    client.query(query, job_config=job_config).result()

    print(
        f"Deleted existing rows from {table_id} "
        f"for {len(source_files)} source files."
    )


def update_file_status_success(config, source_files, run_id):
    if not source_files:
        return

    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, "audit", "file_processing_status")

    query = f"""
    UPDATE `{table_id}`
    SET
      silver_clean_status = 'SUCCESS',
      silver_iat_status = 'PENDING',
      decoded_signals_status = COALESCE(decoded_signals_status, 'PENDING'),
      gold_window_status = 'PENDING',
      can_id_profile_status = 'PENDING',
      run_id = @run_id,
      updated_at = CURRENT_TIMESTAMP(),
      error_message = NULL
    WHERE source_file IN UNNEST(@source_files)
      AND bronze_status = 'SUCCESS'
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
      silver_clean_status = 'FAILED',
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


def extract_payload_bytes(df):
    return (
        df
        .withColumn(
            "byte_0",
            expr(
                "CASE WHEN length(data) >= 1 "
                "THEN CAST(conv(hex(substring(data, 1, 1)), 16, 10) AS BIGINT) END"
            ),
        )
        .withColumn(
            "byte_1",
            expr(
                "CASE WHEN length(data) >= 2 "
                "THEN CAST(conv(hex(substring(data, 2, 1)), 16, 10) AS BIGINT) END"
            ),
        )
        .withColumn(
            "byte_2",
            expr(
                "CASE WHEN length(data) >= 3 "
                "THEN CAST(conv(hex(substring(data, 3, 1)), 16, 10) AS BIGINT) END"
            ),
        )
        .withColumn(
            "byte_3",
            expr(
                "CASE WHEN length(data) >= 4 "
                "THEN CAST(conv(hex(substring(data, 4, 1)), 16, 10) AS BIGINT) END"
            ),
        )
        .withColumn(
            "byte_4",
            expr(
                "CASE WHEN length(data) >= 5 "
                "THEN CAST(conv(hex(substring(data, 5, 1)), 16, 10) AS BIGINT) END"
            ),
        )
        .withColumn(
            "byte_5",
            expr(
                "CASE WHEN length(data) >= 6 "
                "THEN CAST(conv(hex(substring(data, 6, 1)), 16, 10) AS BIGINT) END"
            ),
        )
        .withColumn(
            "byte_6",
            expr(
                "CASE WHEN length(data) >= 7 "
                "THEN CAST(conv(hex(substring(data, 7, 1)), 16, 10) AS BIGINT) END"
            ),
        )
        .withColumn(
            "byte_7",
            expr(
                "CASE WHEN length(data) >= 8 "
                "THEN CAST(conv(hex(substring(data, 8, 1)), 16, 10) AS BIGINT) END"
            ),
        )
    )


def create_messages_clean(bronze_df):
    deduplicated_df = bronze_df.dropDuplicates(
        [
            "session_id",
            "timestamp_us",
            "arbitration_id",
            "dlc",
            "data",
            "label",
            "source_file",
        ]
    )

    enriched_df = (
        deduplicated_df
        .filter(col("session_id").isNotNull())
        .filter(col("session_id") != "")
        .filter(col("timestamp_us").isNotNull())
        .filter(col("arbitration_id").isNotNull())
        .filter(col("dlc").isNotNull())
        .filter(col("label").isNotNull())
        .filter(col("data").isNotNull())

        # Bronze contient des timestamps en nanosecondes.
        # Silver standardise en microsecondes.
        .withColumn("timestamp_us", (col("timestamp_us") / lit(1000)).cast("long"))

        .withColumn(
            "message_id",
            md5(
                concat_ws(
                    "|",
                    col("session_id").cast("string"),
                    col("timestamp_us").cast("string"),
                    col("arbitration_id").cast("string"),
                    hex(col("data")),
                )
            ),
        )
        .withColumn("aid_hex", format_string("0x%03X", col("arbitration_id")))
    )

    bytes_df = extract_payload_bytes(enriched_df)

    final_df = (
        bytes_df
        .withColumn(
            "zero_byte_count",
            (
                when((col("dlc") >= 1) & (col("byte_0") == 0), 1).otherwise(0)
                + when((col("dlc") >= 2) & (col("byte_1") == 0), 1).otherwise(0)
                + when((col("dlc") >= 3) & (col("byte_2") == 0), 1).otherwise(0)
                + when((col("dlc") >= 4) & (col("byte_3") == 0), 1).otherwise(0)
                + when((col("dlc") >= 5) & (col("byte_4") == 0), 1).otherwise(0)
                + when((col("dlc") >= 6) & (col("byte_5") == 0), 1).otherwise(0)
                + when((col("dlc") >= 7) & (col("byte_6") == 0), 1).otherwise(0)
                + when((col("dlc") >= 8) & (col("byte_7") == 0), 1).otherwise(0)
            ),
        )
        .withColumn(
            "zeros_ratio",
            when(col("dlc") == 0, None)
            .otherwise(col("zero_byte_count") / col("dlc")),
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
            "source_file",
            "ingested_at",
        )
    )

    return final_df


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

    schema = StructType(
        [
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
        ]
    )

    data = [
        (
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
            "Bronze.messages_raw",
            "Silver.messages_clean",
            error_message,
            datetime.now(timezone.utc),
        )
    ]

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
    step_name = "silver_clean_spark"
    started_at = datetime.now(timezone.utc)

    input_rows = 0
    output_rows = 0
    source_files = []

    try:
        source_files = get_files_to_process(config)

        if not source_files:
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

            print("Aucun fichier à traiter pour Silver Clean.")
            return

        bronze_df = (
            read_from_bigquery(
                spark=spark,
                config=config,
                dataset_key="bronze",
                table_key="bronze_raw",
            )
            .filter(col("source_file").isin(source_files))
        )

        input_rows = bronze_df.count()

        messages_clean_df = create_messages_clean(bronze_df)

        output_rows = messages_clean_df.count()

        delete_existing_silver_rows(config, source_files)

        if output_rows > 0:
            write_to_bigquery(
                df=messages_clean_df,
                config=config,
                dataset_key="silver",
                table_key="silver_clean",
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

        print("Silver Clean terminé.")
        print(f"Fichiers traités : {len(source_files)}")
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