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
    count,
    avg,
    stddev,
    countDistinct,
    max as spark_max,
    when,
    current_timestamp,
    floor,
    array,
    explode,
    log as spark_log,
    sum as spark_sum,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)


WINDOW_SIZE_US = 1_000_000


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
        .appName("can-ids-gold-features-window-spark")
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
    WHERE silver_iat_status = 'SUCCESS'
      AND COALESCE(gold_window_status, 'PENDING') != 'SUCCESS'
      AND source_file IS NOT NULL
    """

    client = bigquery.Client(project=project_id)
    rows = client.query(query).result()

    return [row.source_file for row in rows]


def delete_existing_gold_rows(config, source_files):
    if not source_files:
        return

    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, "gold", "gold_features_window")

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


def update_file_status_success(config, source_files, run_id):
    if not source_files:
        return

    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, "audit", "file_processing_status")

    query = f"""
    UPDATE `{table_id}`
    SET
      gold_window_status = 'SUCCESS',
      run_id = @run_id,
      updated_at = CURRENT_TIMESTAMP(),
      error_message = NULL
    WHERE source_file IN UNNEST(@source_files)
      AND silver_iat_status = 'SUCCESS'
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
      gold_window_status = 'FAILED',
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


def add_window_columns(df):
    return (
        df
        .withColumn(
            "window_start_us",
            floor(col("timestamp_us") / lit(WINDOW_SIZE_US)).cast("long")
            * lit(WINDOW_SIZE_US),
        )
        .withColumn(
            "window_end_us",
            col("window_start_us") + lit(WINDOW_SIZE_US),
        )
    )


def create_entropy_df(windowed_df):
    keys = [
        "source_file",
        "session_id",
        "arbitration_id",
        "window_start_us",
        "window_end_us",
    ]

    byte_df = (
        windowed_df
        .select(
            *keys,
            explode(
                array(
                    col("byte_0"),
                    col("byte_1"),
                    col("byte_2"),
                    col("byte_3"),
                    col("byte_4"),
                    col("byte_5"),
                    col("byte_6"),
                    col("byte_7"),
                )
            ).alias("payload_byte"),
        )
        .filter(col("payload_byte").isNotNull())
    )

    byte_counts_df = (
        byte_df
        .groupBy(*keys, "payload_byte")
        .agg(count("*").alias("byte_count"))
    )

    total_bytes_df = (
        byte_counts_df
        .groupBy(*keys)
        .agg(spark_sum("byte_count").alias("total_byte_count"))
    )

    probabilities_df = (
        byte_counts_df
        .join(total_bytes_df, keys, "inner")
        .withColumn("p", col("byte_count") / col("total_byte_count"))
    )

    entropy_df = (
        probabilities_df
        .groupBy(*keys)
        .agg(
            (
                -spark_sum(
                    col("p") * (spark_log(col("p")) / spark_log(lit(2.0)))
                )
            ).alias("entropy_bits")
        )
    )

    return entropy_df


def create_gold_features_window(messages_with_iat_df):
    windowed_df = add_window_columns(messages_with_iat_df)

    keys = [
        "source_file",
        "session_id",
        "arbitration_id",
        "aid_hex",
        "window_start_us",
        "window_end_us",
    ]

    features_df = (
        windowed_df
        .groupBy(*keys)
        .agg(
            spark_max("label").alias("label"),
            count("*").alias("msg_count"),
            avg("iat_us").alias("iat_mean_us"),
            stddev("iat_us").alias("iat_std_us"),
            avg("dlc").alias("dlc_mean"),
            stddev("dlc").alias("dlc_std"),
            countDistinct("dlc").alias("dlc_distinct_count"),
            avg("zeros_ratio").alias("avg_zeros_ratio"),
        )
        .withColumn(
            "iat_cv",
            when(
                col("iat_mean_us").isNull() | (col("iat_mean_us") == 0),
                None,
            ).otherwise(col("iat_std_us") / col("iat_mean_us")),
        )
        .withColumn(
            "msg_per_sec",
            col("msg_count") / lit(WINDOW_SIZE_US / 1_000_000.0),
        )
    )

    entropy_df = create_entropy_df(windowed_df)

    features_with_entropy_df = (
        features_df
        .join(
            entropy_df,
            on=[
                "source_file",
                "session_id",
                "arbitration_id",
                "window_start_us",
                "window_end_us",
            ],
            how="left",
        )
        .withColumn("computed_at", current_timestamp())
        .select(
            "source_file",
            "session_id",
            "arbitration_id",
            "aid_hex",
            "window_start_us",
            "window_end_us",
            "label",
            "msg_count",
            "iat_mean_us",
            "iat_std_us",
            "iat_cv",
            "msg_per_sec",
            "entropy_bits",
            "dlc_mean",
            "dlc_std",
            "dlc_distinct_count",
            "avg_zeros_ratio",
            "computed_at",
        )
    )

    return features_with_entropy_df


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
        "Silver.messages_with_iat",
        "Gold.gold_features_window",
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
    step_name = "gold_features_window_spark"
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

            print("Aucun fichier à traiter pour Gold Features Window.")
            return

        messages_with_iat_df = (
            read_from_bigquery(
                spark=spark,
                config=config,
                dataset_key="silver",
                table_key="silver_iat",
            )
            .filter(col("source_file").isin(source_files))
        )

        input_rows = messages_with_iat_df.count()

        if input_rows == 0:
            error_message = "Aucune ligne Silver IAT trouvée pour les fichiers à traiter."

            update_file_status_failed(
                config=config,
                source_files=source_files,
                run_id=run_id,
                error_message=error_message,
            )

            write_pipeline_run(
                spark=spark,
                config=config,
                run_id=run_id,
                step_name=step_name,
                status="FAILED",
                started_at=started_at,
                input_rows=0,
                output_rows=0,
                files_processed=len(source_files),
                error_message=error_message,
            )

            print(error_message)
            return

        gold_features_df = create_gold_features_window(messages_with_iat_df)

        output_rows = gold_features_df.count()

        delete_existing_gold_rows(config, source_files)

        if output_rows > 0:
            write_to_bigquery(
                df=gold_features_df,
                config=config,
                dataset_key="gold",
                table_key="gold_features_window",
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

        print("Gold Features Window terminé.")
        print(f"Fichiers traités : {len(source_files)}")
        print(f"Lignes lues : {input_rows}")
        print(f"Lignes Gold écrites : {output_rows}")

    except Exception as error:
        error_message = str(error)

        update_file_status_failed(
            config=config,
            source_files=source_files,
            run_id=run_id,
            error_message=error_message,
        )

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