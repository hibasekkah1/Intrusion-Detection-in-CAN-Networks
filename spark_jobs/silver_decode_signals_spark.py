import argparse
import tempfile
import subprocess
import traceback

import cantools

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    udf,
    explode,
    current_timestamp,
    coalesce,
)
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
    DoubleType,
)


DEFAULT_PROJECT_ID = "project-e6de9b55-41d5-4f13-ae0"
DEFAULT_TEMP_BUCKET = "can-ids-data"
DEFAULT_LOCATION = "europe-southwest1"


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--project_id", default=DEFAULT_PROJECT_ID)
    parser.add_argument("--location", default=DEFAULT_LOCATION)
    parser.add_argument("--temp_bucket", default=DEFAULT_TEMP_BUCKET)

    parser.add_argument("--dbc_path", required=True)
    parser.add_argument("--input_table", required=True)
    parser.add_argument("--output_table", required=True)
    parser.add_argument("--status_table", required=True)

    return parser.parse_args()


def create_spark_session(project_id, temp_bucket):
    spark = (
        SparkSession.builder
        .appName("can-ids-silver-decode-signals")
        .config("spark.sql.shuffle.partitions", "400")
        .config("temporaryGcsBucket", temp_bucket)
        .getOrCreate()
    )

    spark.conf.set("parentProject", project_id)

    return spark


def load_dbc_from_gcs(dbc_path):
    local_path = tempfile.NamedTemporaryFile(delete=False, suffix=".dbc").name

    subprocess.check_call(
        [
            "gsutil",
            "cp",
            dbc_path,
            local_path,
        ]
    )

    return cantools.database.load_file(local_path)


def normalize_payload(data_value):
    """
    Convertit le payload CAN en bytes.
    Gère les cas :
    - bytes
    - bytearray
    - chaîne hexadécimale
    """
    if data_value is None:
        return None

    if isinstance(data_value, bytes):
        return data_value

    if isinstance(data_value, bytearray):
        return bytes(data_value)

    value = str(data_value).strip()

    # Cas possibles : "0x010203", "01 02 03", "01-02-03"
    value = value.replace("0x", "")
    value = value.replace(" ", "")
    value = value.replace("-", "")
    value = value.replace(",", "")

    # Si Spark affiche parfois b'\x01\x02', ce cas est plus compliqué
    # et sera ignoré proprement.
    if value.startswith("b'") or value.startswith('b"'):
        return None

    if len(value) % 2 != 0:
        return None

    try:
        return bytes.fromhex(value)
    except Exception:
        return None


def ensure_audit_columns(project_id, location, status_table):
    """
    Ajoute les colonnes nécessaires au suivi du décodage si elles n'existent pas.
    """
    client = bigquery.Client(project=project_id)

    try:
        client.get_table(status_table)
    except NotFound:
        print(f"Status table {status_table} does not exist. Skipping audit columns creation.")
        return

    statements = [
        f"""
        ALTER TABLE `{status_table}`
        ADD COLUMN IF NOT EXISTS decoded_signals_status STRING
        """,
        f"""
        ALTER TABLE `{status_table}`
        ADD COLUMN IF NOT EXISTS decoded_signals_updated_at TIMESTAMP
        """,
        f"""
        ALTER TABLE `{status_table}`
        ADD COLUMN IF NOT EXISTS decoded_signals_error STRING
        """,
    ]

    for sql in statements:
        client.query(sql, location=location).result()

    print("Decoded signals audit columns checked.")


def get_files_to_decode(spark, status_table):
    """
    Sélectionne uniquement les fichiers dont silver_iat_status = SUCCESS
    et decoded_signals_status != SUCCESS.
    """
    status_df = (
        spark.read
        .format("bigquery")
        .option("table", status_table)
        .load()
    )

    files_to_decode_df = (
        status_df
        .filter(
            (col("silver_iat_status") == lit("SUCCESS"))
            & (
                coalesce(
                    col("decoded_signals_status"),
                    lit("PENDING")
                ) != lit("SUCCESS")
            )
        )
        .select("source_file")
        .distinct()
    )

    return files_to_decode_df


def collect_source_files(files_df):
    return [row["source_file"] for row in files_df.collect()]


def update_files_status(
    project_id,
    location,
    status_table,
    source_files,
    status,
    error_message=None,
):
    """
    Met à jour le statut decoded_signals_status dans file_processing_status.
    """
    if not source_files:
        return

    client = bigquery.Client(project=project_id)

    sql = f"""
    UPDATE `{status_table}`
    SET
      decoded_signals_status = @status,
      decoded_signals_updated_at = CURRENT_TIMESTAMP(),
      decoded_signals_error = @error_message,
      updated_at = CURRENT_TIMESTAMP()
    WHERE source_file IN UNNEST(@source_files)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
            bigquery.ArrayQueryParameter("source_files", "STRING", source_files),
        ]
    )

    client.query(
        sql,
        job_config=job_config,
        location=location,
    ).result()


def delete_existing_decoded_rows(
    project_id,
    location,
    output_table,
    source_files,
):
    """
    Supprime les anciennes lignes décodées des fichiers à retraiter.
    Si la table n'existe pas encore, on ignore le DELETE.
    """
    if not source_files:
        return

    client = bigquery.Client(project=project_id)

    try:
        client.get_table(output_table)
    except NotFound:
        print(f"Output table does not exist yet: {output_table}. Skipping delete.")
        return

    sql = f"""
    DELETE FROM `{output_table}`
    WHERE source_file IN UNNEST(@source_files)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("source_files", "STRING", source_files),
        ]
    )

    client.query(
        sql,
        job_config=job_config,
        location=location,
    ).result()

    print(
        f"Deleted existing decoded rows from {output_table} "
        f"for {len(source_files)} source files."
    )


def main():
    args = parse_args()

    project_id = args.project_id
    location = args.location
    temp_bucket = args.temp_bucket

    spark = create_spark_session(
        project_id=project_id,
        temp_bucket=temp_bucket,
    )

    files_to_decode = []

    try:
        ensure_audit_columns(
            project_id=project_id,
            location=location,
            status_table=args.status_table,
        )

        files_to_decode_df = get_files_to_decode(
            spark=spark,
            status_table=args.status_table,
        )

        files_to_decode = collect_source_files(files_to_decode_df)

        if not files_to_decode:
            print("No source_file to decode. Skipping decoded signals step.")
            return

        print(f"Files to decode: {len(files_to_decode)}")
        for source_file in files_to_decode:
            print(f" - {source_file}")

        update_files_status(
            project_id=project_id,
            location=location,
            status_table=args.status_table,
            source_files=files_to_decode,
            status="RUNNING",
            error_message=None,
        )

        delete_existing_decoded_rows(
            project_id=project_id,
            location=location,
            output_table=args.output_table,
            source_files=files_to_decode,
        )

        db = load_dbc_from_gcs(args.dbc_path)
        broadcast_db = spark.sparkContext.broadcast(db)

        decoded_schema = ArrayType(
            StructType(
                [
                    StructField("message_name", StringType(), True),
                    StructField("signal_name", StringType(), True),
                    StructField("signal_value", DoubleType(), True),
                    StructField("signal_unit", StringType(), True),
                ]
            )
        )

        def decode_payload(arbitration_id, data_value):
            try:
                if arbitration_id is None:
                    return []

                db_local = broadcast_db.value
                frame_id = int(arbitration_id)

                try:
                    message = db_local.get_message_by_frame_id(frame_id)
                except Exception:
                    return []

                if message is None:
                    return []

                payload = normalize_payload(data_value)

                if payload is None:
                    return []

                decoded_values = message.decode(
                    payload,
                    decode_choices=False,
                    scaling=True,
                )

                results = []

                for signal in message.signals:
                    signal_name = signal.name
                    signal_unit = signal.unit

                    if signal_name not in decoded_values:
                        continue

                    value = decoded_values[signal_name]

                    try:
                        signal_value = float(value)
                    except Exception:
                        signal_value = None

                    results.append(
                        {
                            "message_name": message.name,
                            "signal_name": signal_name,
                            "signal_value": signal_value,
                            "signal_unit": signal_unit,
                        }
                    )

                return results

            except Exception:
                return []

        decode_udf = udf(decode_payload, decoded_schema)

        input_df = (
            spark.read
            .format("bigquery")
            .option("table", args.input_table)
            .load()
        )

        input_incremental_df = (
            input_df
            .join(
                files_to_decode_df,
                on="source_file",
                how="inner",
            )
        )

        if input_incremental_df.limit(1).count() == 0:
            print("No matching rows found in input table for files to decode.")

            update_files_status(
                project_id=project_id,
                location=location,
                status_table=args.status_table,
                source_files=files_to_decode,
                status="FAILED",
                error_message="No matching rows found in input table.",
            )

            return
        print("Input rows to decode:", input_incremental_df.count())
        print("Distinct arbitration_id to decode:", 
        input_incremental_df.select("arbitration_id").distinct().count())

        decoded_df = (
            input_incremental_df
            .withColumn(
                "decoded_signals",
                decode_udf(
                    col("arbitration_id"),
                    col("data"),
                ),
            )
            .withColumn(
                "decoded_signal",
                explode(col("decoded_signals")),
            )
            .select(
                col("source_file"),
                col("session_id"),
                col("timestamp_us"),
                col("arbitration_id"),
                col("aid_hex"),
                col("dlc"),
                col("data"),
                col("decoded_signal.message_name").alias("message_name"),
                col("decoded_signal.signal_name").alias("signal_name"),
                col("decoded_signal.signal_value").alias("signal_value"),
                col("decoded_signal.signal_unit").alias("signal_unit"),
                col("label"),
                current_timestamp().alias("decoded_at"),
            )
        )

        decoded_count = decoded_df.count()

        if decoded_count == 0:
            print("No decoded signal rows produced. Marking as FAILED but not crashing job.")

            update_files_status(
                project_id=project_id,
                location=location,
                status_table=args.status_table,
                source_files=files_to_decode,
                status="FAILED",
                error_message="No decoded signal rows produced.",
            )

            return


        print(f"Decoded rows count: {decoded_count}")

        (
            decoded_df.write
            .format("bigquery")
            .option("table", args.output_table)
            .option("temporaryGcsBucket", temp_bucket)
            .mode("append")
            .save()
        )

        update_files_status(
            project_id=project_id,
            location=location,
            status_table=args.status_table,
            source_files=files_to_decode,
            status="SUCCESS",
            error_message=None,
        )

        print(f"Decoded signals written successfully to {args.output_table}")

    except Exception as exc:
        error_message = str(exc)
        traceback.print_exc()

        if files_to_decode:
            update_files_status(
                project_id=project_id,
                location=location,
                status_table=args.status_table,
                source_files=files_to_decode,
                status="FAILED",
                error_message=error_message[:1000],
            )

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()