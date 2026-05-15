import argparse
import os
import yaml
from uuid import uuid4
from datetime import datetime, timezone

import cantools
from google.cloud import storage

from pyspark import SparkFiles
from pyspark.sql import SparkSession
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
    parser.add_argument("--dbc_path", required=True)
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
        .appName("can-ids-dbc-messages-reference-spark")
        .config("spark.sql.shuffle.partitions", "20")
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


def prepare_dbc_file(spark, dbc_path):
    """
    Ajoute le fichier DBC au contexte Spark.
    Le fichier sera disponible localement via SparkFiles.get().
    """
    spark.sparkContext.addFile(dbc_path)
    return os.path.basename(dbc_path)


def create_dbc_reference_df(spark, dbc_file_name):
    """
    Crée une table de référence DBC :
    1 ligne = 1 message DBC / arbitration_id.
    """

    dbc_local_path = SparkFiles.get(dbc_file_name)

    db = cantools.database.load_file(dbc_local_path)

    computed_at = datetime.now(timezone.utc)

    rows = []

    for message in db.messages:
        signal_names_list = [signal.name for signal in message.signals]
        signal_names = ", ".join(signal_names_list)

        rows.append(
            (
                int(message.frame_id),
                f"0x{int(message.frame_id):03X}",
                message.name,
                int(len(signal_names_list)),
                signal_names,
                computed_at,
            )
        )

    schema = StructType(
        [
            StructField("arbitration_id", LongType(), True),
            StructField("aid_hex", StringType(), True),
            StructField("message_name", StringType(), True),
            StructField("signals_count", LongType(), True),
            StructField("signal_names", StringType(), True),
            StructField("computed_at", TimestampType(), True),
        ]
    )

    return spark.createDataFrame(rows, schema)


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
            float(duration_seconds),
            input_rows,
            output_rows,
            0,
            1,
            0,
            "DBC file",
            "Silver.dbc_messages_reference",
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
    step_name = "dbc_messages_reference_spark"
    started_at = datetime.now(timezone.utc)

    input_rows = 1
    output_rows = 0

    try:
        dbc_file_name = prepare_dbc_file(
            spark=spark,
            dbc_path=args.dbc_path,
        )

        dbc_reference_df = create_dbc_reference_df(
            spark=spark,
            dbc_file_name=dbc_file_name,
        )

        output_rows = dbc_reference_df.count()

        write_to_bigquery(
            df=dbc_reference_df,
            config=config,
            dataset_key="silver",
            table_key="dbc_messages_reference",
            mode="overwrite",
        )

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

        print("DBC messages reference terminé.")
        print(f"Messages DBC écrits : {output_rows}")

    except Exception as error:
        error_message = str(error)

        try:
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
        except Exception as audit_error:
            print(f"Erreur lors de l'écriture audit pipeline_runs : {audit_error}")

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    run()