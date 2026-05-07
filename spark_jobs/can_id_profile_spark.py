import argparse
import yaml
from uuid import uuid4
from datetime import datetime, timezone

from google.cloud import storage
from google.cloud import bigquery

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    avg,
    stddev,
    collect_set,
    sort_array,
    concat_ws,
    current_timestamp,
    hex,
    first,
    coalesce,
    format_string,
    lit,
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
        .appName("can-ids-can-id-profile-spark")
        .config("spark.sql.shuffle.partitions", "200")
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


def write_to_bigquery(df, config, dataset_key, table_key, mode="overwrite"):
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


def update_can_id_profile_status_success(config, run_id):
    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, "audit", "file_processing_status")

    query = f"""
    UPDATE `{table_id}`
    SET
      can_id_profile_status = 'SUCCESS',
      run_id = @run_id,
      updated_at = CURRENT_TIMESTAMP(),
      error_message = NULL
    WHERE silver_iat_status = 'SUCCESS'
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )

    client = bigquery.Client(project=project_id)
    client.query(query, job_config=job_config).result()


def update_can_id_profile_status_failed(config, run_id, error_message):
    project_id = config["gcp"]["project_id"]
    table_id = bigquery_table(config, "audit", "file_processing_status")

    query = f"""
    UPDATE `{table_id}`
    SET
      can_id_profile_status = 'FAILED',
      run_id = @run_id,
      updated_at = CURRENT_TIMESTAMP(),
      error_message = @error_message
    WHERE silver_iat_status = 'SUCCESS'
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
        ]
    )

    client = bigquery.Client(project=project_id)
    client.query(query, job_config=job_config).result()


def create_can_id_profile(messages_with_iat_df, dbc_reference_df):
    can_stats_df = (
        messages_with_iat_df
        .groupBy("arbitration_id")
        .agg(
            first("aid_hex", ignorenulls=True).alias("aid_hex_from_data"),
            count("*").alias("messages_count"),
            countDistinct(hex(col("data"))).alias("uniq_data_count"),
            avg("iat_us").alias("interval_mean_us"),
            stddev("iat_us").alias("interval_std_us"),
            concat_ws(
                ",",
                sort_array(collect_set(col("dlc").cast("string")))
            ).alias("uniq_dlc"),
        )
        .withColumn("interval_mean", col("interval_mean_us") / lit(1000000.0))
        .withColumn("interval_std", col("interval_std_us") / lit(1000000.0))
        .drop("interval_mean_us", "interval_std_us")
    )

    dbc_df = (
        dbc_reference_df
        .select(
            "arbitration_id",
            col("aid_hex").alias("aid_hex_from_dbc"),
            "message_name",
            "signals_count",
            "signal_names",
        )
    )

    profile_df = (
        can_stats_df
        .join(
            dbc_df,
            on="arbitration_id",
            how="left",
        )
        .withColumn(
            "aid_hex",
            coalesce(
                col("aid_hex_from_dbc"),
                col("aid_hex_from_data"),
                format_string("0x%03X", col("arbitration_id")),
            )
        )
        .withColumn(
            "message_name",
            coalesce(
                col("message_name"),
                format_string("UNKNOWN_0x%03X", col("arbitration_id")),
            )
        )
        .withColumn(
            "signal_names",
            coalesce(col("signal_names"), lit(""))
        )
        .withColumn("computed_at", current_timestamp())
        .select(
            "arbitration_id",
            "aid_hex",
            "message_name",
            "signals_count",
            "signal_names",
            "messages_count",
            "uniq_data_count",
            "interval_mean",
            "interval_std",
            "uniq_dlc",
            "computed_at",
        )
    )

    return profile_df


def create_pipeline_run_df(
    spark,
    run_id,
    step_name,
    status,
    started_at,
    input_rows,
    output_rows,
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
        float(duration_seconds),
        input_rows,
        output_rows,
        0,
        0,
        0,
        "Silver.messages_with_iat + Silver.dbc_messages_reference",
        "Gold.can_id_profile",
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
    step_name = "can_id_profile_spark"
    started_at = datetime.now(timezone.utc)

    input_rows = 0
    output_rows = 0

    try:
        messages_with_iat_df = read_from_bigquery(
            spark=spark,
            config=config,
            dataset_key="silver",
            table_key="silver_iat",
        )

        dbc_reference_df = read_from_bigquery(
            spark=spark,
            config=config,
            dataset_key="silver",
            table_key="dbc_messages_reference",
        )

        input_rows = messages_with_iat_df.count()

        can_id_profile_df = create_can_id_profile(
            messages_with_iat_df=messages_with_iat_df,
            dbc_reference_df=dbc_reference_df,
        )

        output_rows = can_id_profile_df.count()

        write_to_bigquery(
            df=can_id_profile_df,
            config=config,
            dataset_key="gold",
            table_key="can_id_profile",
            mode="overwrite",
        )

        update_can_id_profile_status_success(config, run_id)

        write_pipeline_run(
            spark=spark,
            config=config,
            run_id=run_id,
            step_name=step_name,
            status="SUCCESS",
            started_at=started_at,
            input_rows=input_rows,
            output_rows=output_rows,
            error_message=None,
        )

        print("CAN ID Profile terminé.")
        print(f"Lignes Silver lues : {input_rows}")
        print(f"Lignes Gold écrites : {output_rows}")

    except Exception as error:
        error_message = str(error)

        update_can_id_profile_status_failed(config, run_id, error_message)

        write_pipeline_run(
            spark=spark,
            config=config,
            run_id=run_id,
            step_name=step_name,
            status="FAILED",
            started_at=started_at,
            input_rows=input_rows,
            output_rows=output_rows,
            error_message=error_message,
        )

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    run()