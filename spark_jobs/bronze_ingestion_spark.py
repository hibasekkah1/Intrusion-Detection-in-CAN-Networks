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
    current_timestamp,
    input_file_name,
    regexp_extract,
    regexp_replace,
    split,
    element_at,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
    IntegerType,
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
        content = read_gcs_text(config_path)
        return yaml.safe_load(content)

    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def create_spark_session(config):
    project_id = config["gcp"]["project_id"]
    temp_bucket = config["gcp"]["temp_bucket"]

    spark = (
        SparkSession.builder
        .appName("can-ids-bronze-ingestion-incremental-spark")
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


def list_raw_parquet_files(config):
    bucket_name = config["gcp"]["bucket_name"]
    raw_prefix = config["storage"]["raw_prefix"]

    client = storage.Client(project=config["gcp"]["project_id"])
    blobs = client.list_blobs(bucket_name, prefix=raw_prefix)

    files = []

    for blob in blobs:
        if blob.name.endswith(".parquet"):
            files.append(
                {
                    "source_file": blob.name,
                    "gcs_uri": f"gs://{bucket_name}/{blob.name}",
                }
            )

    return files


def get_already_ingested_files(config):
    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, "audit", "file_processing_status")

    client = bigquery.Client(project=project_id)

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
      FROM `{table_id}`
      WHERE source_file IS NOT NULL
    )
    WHERE rn = 1
      AND bronze_status = 'SUCCESS'
    """

    try:
        rows = client.query(query).result()
        return {row.source_file for row in rows}

    except NotFound:
        return set()


def get_new_files(config):
    raw_files = list_raw_parquet_files(config)
    already_ingested = get_already_ingested_files(config)

    new_files = [
        file_info
        for file_info in raw_files
        if file_info["source_file"] not in already_ingested
    ]

    return raw_files, new_files


def read_raw_parquet(spark, files_to_process):
    gcs_paths = [file_info["gcs_uri"] for file_info in files_to_process]

    df = spark.read.parquet(*gcs_paths)

    df = df.withColumn("source_file_path", input_file_name())

    df = df.withColumn(
        "source_file",
        regexp_extract(col("source_file_path"), r"gs://[^/]+/(.*)", 1),
    )

    # Cas attendu :
    # source_file = raw/dump1.parquet
    # session_id  = dump1
    #
    # Cas possible avec certains readers :
    # source_file = raw/dump1.parquet/part-0000.snappy.parquet
    # session_id  = dump1
    df = df.withColumn(
        "session_id",
        regexp_extract(col("source_file"), r"(?:^|/)([^/]+)\.parquet(?:/|$)", 1),
    )

    # Fallback au cas où la regex principale ne matche pas
    df = df.withColumn(
        "session_id",
        when(
            (col("session_id").isNull()) | (col("session_id") == ""),
            regexp_replace(
                element_at(split(col("source_file"), "/"), -1),
                r"\.parquet$",
                "",
            ),
        ).otherwise(col("session_id")),
    )

    df = df.drop("source_file_path")

    return df


def normalize_columns(df):
    if "timestamp" in df.columns and "timestamp_us" not in df.columns:
        df = df.withColumnRenamed("timestamp", "timestamp_us")

    if "__index_level_0__" in df.columns and "timestamp_us" not in df.columns:
        df = df.withColumnRenamed("__index_level_0__", "timestamp_us")

    return (
        df
        .withColumn("timestamp_us", col("timestamp_us").cast(LongType()))
        .withColumn("arbitration_id", col("arbitration_id").cast(IntegerType()))
        .withColumn("dlc", col("dlc").cast(IntegerType()))
        .withColumn("label", col("label").cast(IntegerType()))
        .withColumn("ingested_at", current_timestamp())
    )


def split_valid_and_rejected(df, config):
    rules = config["can_rules"]

    valid_condition = (
        col("timestamp_us").isNotNull()
        & col("arbitration_id").isNotNull()
        & (col("arbitration_id") >= rules["arbitration_id_min"])
        & (col("arbitration_id") <= rules["arbitration_id_max"])
        & col("dlc").isNotNull()
        & (col("dlc") >= rules["dlc_min"])
        & (col("dlc") <= rules["dlc_max"])
        & col("label").isNotNull()
        & col("label").isin(rules["accepted_labels"])
        & col("data").isNotNull()
    )

    valid_df = (
        df
        .filter(valid_condition)
        .select(
            "session_id",
            "timestamp_us",
            "arbitration_id",
            "dlc",
            "data",
            "label",
            "source_file",
            "ingested_at",
        )
    )

    rejected_df = (
        df
        .filter(~valid_condition)
        .withColumn(
            "rejection_reason",
            when(col("timestamp_us").isNull(), lit("timestamp_null"))
            .when(col("arbitration_id").isNull(), lit("arbitration_id_null"))
            .when(
                (col("arbitration_id") < rules["arbitration_id_min"])
                | (col("arbitration_id") > rules["arbitration_id_max"]),
                lit("arbitration_id_out_of_range"),
            )
            .when(col("dlc").isNull(), lit("dlc_null"))
            .when(
                (col("dlc") < rules["dlc_min"])
                | (col("dlc") > rules["dlc_max"]),
                lit("dlc_out_of_range"),
            )
            .when(col("label").isNull(), lit("label_null"))
            .when(~col("label").isin(rules["accepted_labels"]), lit("label_invalid"))
            .when(col("data").isNull(), lit("data_null"))
            .otherwise(lit("unknown_reason"))
        )
        .withColumn("rejected_at", current_timestamp())
        .select(
            "session_id",
            "timestamp_us",
            "arbitration_id",
            "dlc",
            "data",
            "label",
            "source_file",
            "rejection_reason",
            "rejected_at",
        )
    )

    return valid_df, rejected_df


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


def delete_source_files_from_table(config, dataset_key, table_key, source_files):
    if not source_files:
        return

    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, dataset_key, table_key)

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
            bigquery.ArrayQueryParameter("source_files", "STRING", source_files)
        ]
    )

    client.query(query, job_config=job_config).result()

    print(f"Deleted existing rows from {table_id} for {len(source_files)} source files.")


def create_file_status_df(spark, raw_df, run_id):
    return (
        raw_df
        .groupBy("source_file", "session_id")
        .count()
        .withColumnRenamed("count", "row_count")
        .withColumn("bronze_status", lit("SUCCESS"))
        .withColumn("silver_clean_status", lit("PENDING"))
        .withColumn("silver_iat_status", lit("PENDING"))
        .withColumn("gold_window_status", lit("PENDING"))
        .withColumn("can_id_profile_status", lit("PENDING"))
        .withColumn("run_id", lit(run_id))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
        .withColumn("error_message", lit(None).cast(StringType()))
        .select(
            "source_file",
            "session_id",
            "bronze_status",
            "silver_clean_status",
            "silver_iat_status",
            "gold_window_status",
            "can_id_profile_status",
            "row_count",
            "run_id",
            "created_at",
            "updated_at",
            "error_message",
        )
    )


def extract_session_id_from_source_file(source_file):
    parts = source_file.split("/")

    for part in parts:
        if part.endswith(".parquet"):
            return part.replace(".parquet", "")

    return parts[-1].replace(".parquet", "")


def create_failed_file_status_df(spark, files_to_process, run_id, error_message):
    now = datetime.now(timezone.utc)

    rows = []

    for file_info in files_to_process:
        source_file = file_info["source_file"]
        session_id = extract_session_id_from_source_file(source_file)

        rows.append(
            (
                source_file,
                session_id,
                "FAILED",
                "PENDING",
                "PENDING",
                "PENDING",
                "PENDING",
                0,
                run_id,
                now,
                now,
                error_message,
            )
        )

    schema = StructType([
        StructField("source_file", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("bronze_status", StringType(), True),
        StructField("silver_clean_status", StringType(), True),
        StructField("silver_iat_status", StringType(), True),
        StructField("gold_window_status", StringType(), True),
        StructField("can_id_profile_status", StringType(), True),
        StructField("row_count", LongType(), True),
        StructField("run_id", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("error_message", StringType(), True),
    ])

    return spark.createDataFrame(rows, schema)


def create_pipeline_run_df(
    spark,
    run_id,
    pipeline_name,
    step_name,
    status,
    started_at,
    finished_at,
    input_rows,
    output_rows,
    rejected_rows,
    files_processed,
    files_skipped,
    source_layer,
    target_layer,
    error_message,
):
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
        pipeline_name,
        step_name,
        status,
        started_at,
        finished_at,
        duration_seconds,
        input_rows,
        output_rows,
        rejected_rows,
        files_processed,
        files_skipped,
        source_layer,
        target_layer,
        error_message,
        datetime.now(timezone.utc),
    )]

    return spark.createDataFrame(data, schema)


def write_pipeline_run(
    spark,
    config,
    run_id,
    pipeline_name,
    step_name,
    status,
    started_at,
    input_rows,
    output_rows,
    rejected_rows,
    files_processed,
    files_skipped,
    source_layer,
    target_layer,
    error_message,
):
    finished_at = datetime.now(timezone.utc)

    run_df = create_pipeline_run_df(
        spark=spark,
        run_id=run_id,
        pipeline_name=pipeline_name,
        step_name=step_name,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        input_rows=input_rows,
        output_rows=output_rows,
        rejected_rows=rejected_rows,
        files_processed=files_processed,
        files_skipped=files_skipped,
        source_layer=source_layer,
        target_layer=target_layer,
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
    pipeline_name = "can_ids_full_cloud_pipeline"
    step_name = "bronze_ingestion_incremental_spark"
    started_at = datetime.now(timezone.utc)

    input_rows = 0
    output_rows = 0
    rejected_rows = 0
    files_processed = 0
    files_skipped = 0
    files_to_process = []

    try:
        raw_files, files_to_process = get_new_files(config)

        files_processed = len(files_to_process)
        files_skipped = len(raw_files) - files_processed

        if files_processed == 0:
            write_pipeline_run(
                spark=spark,
                config=config,
                run_id=run_id,
                pipeline_name=pipeline_name,
                step_name=step_name,
                status="SUCCESS",
                started_at=started_at,
                input_rows=0,
                output_rows=0,
                rejected_rows=0,
                files_processed=0,
                files_skipped=files_skipped,
                source_layer="GCS",
                target_layer="Bronze",
                error_message=None,
            )

            print("Aucun nouveau fichier à ingérer.")
            print(f"Fichiers ignorés : {files_skipped}")
            return

        source_files = [file_info["source_file"] for file_info in files_to_process]

        print("Nouveaux fichiers détectés :")
        for source_file in source_files:
            print(f"- {source_file}")

        raw_df = read_raw_parquet(spark, files_to_process)
        raw_df = normalize_columns(raw_df)

        input_rows = raw_df.count()

        valid_df, rejected_df = split_valid_and_rejected(raw_df, config)

        output_rows = valid_df.count()
        rejected_rows = rejected_df.count()

        # Sécurité idempotence :
        # si un ancien run a écrit partiellement ces fichiers, on nettoie avant append.
        delete_source_files_from_table(
            config=config,
            dataset_key="bronze",
            table_key="bronze_raw",
            source_files=source_files,
        )

        delete_source_files_from_table(
            config=config,
            dataset_key="bronze",
            table_key="bronze_rejected",
            source_files=source_files,
        )

        if output_rows > 0:
            write_to_bigquery(
                df=valid_df,
                config=config,
                dataset_key="bronze",
                table_key="bronze_raw",
                mode="append",
            )

        if rejected_rows > 0:
            write_to_bigquery(
                df=rejected_df,
                config=config,
                dataset_key="bronze",
                table_key="bronze_rejected",
                mode="append",
            )

        file_status_df = create_file_status_df(
            spark=spark,
            raw_df=raw_df,
            run_id=run_id,
        )

        write_to_bigquery(
            df=file_status_df,
            config=config,
            dataset_key="audit",
            table_key="file_processing_status",
            mode="append",
        )

        write_pipeline_run(
            spark=spark,
            config=config,
            run_id=run_id,
            pipeline_name=pipeline_name,
            step_name=step_name,
            status="SUCCESS",
            started_at=started_at,
            input_rows=input_rows,
            output_rows=output_rows,
            rejected_rows=rejected_rows,
            files_processed=files_processed,
            files_skipped=files_skipped,
            source_layer="GCS",
            target_layer="Bronze",
            error_message=None,
        )

        print("Ingestion Bronze incrémentale terminée.")
        print(f"Fichiers traités : {files_processed}")
        print(f"Fichiers ignorés : {files_skipped}")
        print(f"Lignes source : {input_rows}")
        print(f"Lignes chargées : {output_rows}")
        print(f"Lignes rejetées : {rejected_rows}")

    except Exception as error:
        error_message = str(error)

        if files_to_process:
            failed_status_df = create_failed_file_status_df(
                spark=spark,
                files_to_process=files_to_process,
                run_id=run_id,
                error_message=error_message,
            )

            write_to_bigquery(
                df=failed_status_df,
                config=config,
                dataset_key="audit",
                table_key="file_processing_status",
                mode="append",
            )

        write_pipeline_run(
            spark=spark,
            config=config,
            run_id=run_id,
            pipeline_name=pipeline_name,
            step_name=step_name,
            status="FAILED",
            started_at=started_at,
            input_rows=input_rows,
            output_rows=output_rows,
            rejected_rows=rejected_rows,
            files_processed=files_processed,
            files_skipped=files_skipped,
            source_layer="GCS",
            target_layer="Bronze",
            error_message=error_message,
        )

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    run()