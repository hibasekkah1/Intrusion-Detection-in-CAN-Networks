import argparse
import yaml
from uuid import uuid4
from datetime import datetime, timezone

from google.cloud import storage

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    input_file_name,
    regexp_extract,
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
        .appName("can-ids-bronze-ingestion-spark")
        .config("spark.sql.shuffle.partitions", "400")
        .config("temporaryGcsBucket", temp_bucket)
        .getOrCreate()
    )

    spark.conf.set("parentProject", project_id)

    return spark


def read_raw_parquet(spark, config):
    bucket_name = config["gcp"]["bucket_name"]
    raw_prefix = config["storage"]["raw_prefix"]

    source_path = f"gs://{bucket_name}/{raw_prefix}*.parquet"

    df = spark.read.parquet(source_path)

    df = df.withColumn("source_file_path", input_file_name())

    df = df.withColumn(
        "source_file",
        regexp_extract(col("source_file_path"), r"gs://[^/]+/(.*)", 1),
    )

    df = df.withColumn(
        "session_id",
        regexp_extract(col("source_file"), r"([^/]+)\.parquet$", 1),
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


def bigquery_table(config, dataset_key, table_key):
    project_id = config["gcp"]["project_id"]
    dataset = config["bigquery"]["datasets"][dataset_key]
    table = config["bigquery"]["tables"][table_key]
    return f"{project_id}.{dataset}.{table}"


def read_existing_source_files(spark, config):
    table_id = bigquery_table(config, "bronze", "bronze_raw")

    try:
        existing_df = (
            spark.read
            .format("bigquery")
            .option("table", table_id)
            .load()
        )

        if "source_file" not in existing_df.columns:
            return None

        return existing_df.select("source_file").distinct()

    except Exception:
        return None


def remove_already_loaded_files(df, existing_files_df):
    if existing_files_df is None:
        return df

    return df.join(
        existing_files_df,
        on="source_file",
        how="left_anti",
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

    data = [
        (
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
        )
    ]

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
    )


def run():
    args = parse_args()
    config = load_config(args.config_path)

    spark = create_spark_session(config)

    run_id = f"run_{uuid4()}"
    pipeline_name = "can_ids_full_cloud_pipeline"
    step_name = "bronze_ingestion_spark"
    started_at = datetime.now(timezone.utc)

    input_rows = 0
    output_rows = 0
    rejected_rows = 0
    files_processed = 0
    files_skipped = 0

    try:
        raw_df = read_raw_parquet(spark, config)
        raw_df = normalize_columns(raw_df)

        input_rows = raw_df.count()
        total_files_before = raw_df.select("source_file").distinct().count()

        existing_files_df = read_existing_source_files(spark, config)
        raw_df = remove_already_loaded_files(raw_df, existing_files_df)

        total_files_after = raw_df.select("source_file").distinct().count()

        files_processed = total_files_after
        files_skipped = total_files_before - total_files_after

        if files_processed == 0:
            write_pipeline_run(
                spark=spark,
                config=config,
                run_id=run_id,
                pipeline_name=pipeline_name,
                step_name=step_name,
                status="SUCCESS",
                started_at=started_at,
                input_rows=input_rows,
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

        valid_df, rejected_df = split_valid_and_rejected(raw_df, config)

        output_rows = valid_df.count()
        rejected_rows = rejected_df.count()

        write_to_bigquery(valid_df, config, "bronze", "bronze_raw")
        write_to_bigquery(rejected_df, config, "bronze", "bronze_rejected")

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

        print("Ingestion Bronze Spark terminée.")
        print(f"Fichiers traités : {files_processed}")
        print(f"Fichiers ignorés : {files_skipped}")
        print(f"Lignes source : {input_rows}")
        print(f"Lignes chargées : {output_rows}")
        print(f"Lignes rejetées : {rejected_rows}")

    except Exception as error:
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
            error_message=str(error),
        )

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    run()