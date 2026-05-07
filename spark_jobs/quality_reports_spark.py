import argparse
import yaml
from uuid import uuid4
from datetime import datetime, timezone

from google.cloud import storage
from google.cloud import bigquery

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    LongType,
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
        .appName("can-ids-quality-reports-spark")
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


def run_scalar_query(project_id, query):
    client = bigquery.Client(project=project_id)
    rows = list(client.query(query).result())

    if not rows:
        return 0.0

    first_row = rows[0]
    first_value = list(first_row.values())[0]

    if first_value is None:
        return 0.0

    return float(first_value)


def build_check(
    run_id,
    check_name,
    layer_name,
    table_name,
    metric_value,
    threshold_value,
    operator,
    severity,
    details,
):
    if operator == ">":
        passed = metric_value > threshold_value
    elif operator == ">=":
        passed = metric_value >= threshold_value
    elif operator == "=":
        passed = metric_value == threshold_value
    elif operator == "<=":
        passed = metric_value <= threshold_value
    elif operator == "<":
        passed = metric_value < threshold_value
    else:
        passed = False

    status = "PASS" if passed else "FAIL"

    return (
        run_id,
        check_name,
        layer_name,
        table_name,
        status,
        severity,
        float(metric_value),
        float(threshold_value),
        details,
        datetime.now(timezone.utc),
    )


def create_quality_reports_df(spark, records):
    schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("check_name", StringType(), True),
        StructField("layer_name", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("metric_value", DoubleType(), True),
        StructField("threshold_value", DoubleType(), True),
        StructField("details", StringType(), True),
        StructField("computed_at", TimestampType(), True),
    ])

    return spark.createDataFrame(records, schema)


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
        "Bronze/Silver/Gold/Audit",
        "Audit.quality_reports",
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

    project_id = config["gcp"]["project_id"]

    run_id = f"run_{uuid4()}"
    step_name = "quality_reports_spark"
    started_at = datetime.now(timezone.utc)

    records = []

    try:
        bronze_raw = bigquery_table(config, "bronze", "bronze_raw")
        silver_clean = bigquery_table(config, "silver", "silver_clean")
        silver_iat = bigquery_table(config, "silver", "silver_iat")
        dbc_reference = bigquery_table(config, "silver", "dbc_messages_reference")
        gold_features_window = bigquery_table(config, "gold", "gold_features_window")
        can_id_profile = bigquery_table(config, "gold", "can_id_profile")
        file_processing_status = bigquery_table(config, "audit", "file_processing_status")

        # 1. Bronze has rows
        bronze_rows = run_scalar_query(
            project_id,
            f"SELECT COUNT(*) FROM `{bronze_raw}`"
        )
        records.append(build_check(
            run_id=run_id,
            check_name="bronze_raw_has_rows",
            layer_name="Bronze",
            table_name="messages_raw",
            metric_value=bronze_rows,
            threshold_value=0,
            operator=">",
            severity="CRITICAL",
            details="Bronze messages_raw must contain rows.",
        ))

        # 2. Bronze session_id not empty
        bronze_empty_session = run_scalar_query(
            project_id,
            f"""
            SELECT COUNT(*)
            FROM `{bronze_raw}`
            WHERE session_id IS NULL OR session_id = ''
            """
        )
        records.append(build_check(
            run_id=run_id,
            check_name="bronze_session_id_not_empty",
            layer_name="Bronze",
            table_name="messages_raw",
            metric_value=bronze_empty_session,
            threshold_value=0,
            operator="=",
            severity="CRITICAL",
            details="No row in Bronze should have empty session_id.",
        ))

        # 3. Silver Clean has rows
        silver_clean_rows = run_scalar_query(
            project_id,
            f"SELECT COUNT(*) FROM `{silver_clean}`"
        )
        records.append(build_check(
            run_id=run_id,
            check_name="silver_clean_has_rows",
            layer_name="Silver",
            table_name="messages_clean",
            metric_value=silver_clean_rows,
            threshold_value=0,
            operator=">",
            severity="CRITICAL",
            details="Silver messages_clean must contain rows.",
        ))

        # 4. Silver Clean session_id not empty
        silver_clean_empty_session = run_scalar_query(
            project_id,
            f"""
            SELECT COUNT(*)
            FROM `{silver_clean}`
            WHERE session_id IS NULL OR session_id = ''
            """
        )
        records.append(build_check(
            run_id=run_id,
            check_name="silver_clean_session_id_not_empty",
            layer_name="Silver",
            table_name="messages_clean",
            metric_value=silver_clean_empty_session,
            threshold_value=0,
            operator="=",
            severity="CRITICAL",
            details="No row in Silver Clean should have empty session_id.",
        ))

        # 5. Silver IAT has rows
        silver_iat_rows = run_scalar_query(
            project_id,
            f"SELECT COUNT(*) FROM `{silver_iat}`"
        )
        records.append(build_check(
            run_id=run_id,
            check_name="silver_iat_has_rows",
            layer_name="Silver",
            table_name="messages_with_iat",
            metric_value=silver_iat_rows,
            threshold_value=0,
            operator=">",
            severity="CRITICAL",
            details="Silver messages_with_iat must contain rows.",
        ))

        # 6. No negative IAT
        negative_iat = run_scalar_query(
            project_id,
            f"""
            SELECT COUNT(*)
            FROM `{silver_iat}`
            WHERE iat_us < 0
            """
        )
        records.append(build_check(
            run_id=run_id,
            check_name="silver_iat_no_negative_values",
            layer_name="Silver",
            table_name="messages_with_iat",
            metric_value=negative_iat,
            threshold_value=0,
            operator="=",
            severity="CRITICAL",
            details="IAT values must not be negative.",
        ))

        # 7. DBC reference has rows
        dbc_reference_rows = run_scalar_query(
            project_id,
            f"SELECT COUNT(*) FROM `{dbc_reference}`"
        )
        records.append(build_check(
            run_id=run_id,
            check_name="dbc_reference_has_rows",
            layer_name="Silver",
            table_name="dbc_messages_reference",
            metric_value=dbc_reference_rows,
            threshold_value=0,
            operator=">",
            severity="CRITICAL",
            details="DBC messages reference must contain rows.",
        ))

        # 8. Gold Features Window has rows
        gold_window_rows = run_scalar_query(
            project_id,
            f"SELECT COUNT(*) FROM `{gold_features_window}`"
        )
        records.append(build_check(
            run_id=run_id,
            check_name="gold_features_window_has_rows",
            layer_name="Gold",
            table_name="gold_features_window",
            metric_value=gold_window_rows,
            threshold_value=0,
            operator=">",
            severity="CRITICAL",
            details="Gold features window table must contain rows.",
        ))

        # 9. Gold msg_per_sec valid
        invalid_msg_per_sec = run_scalar_query(
            project_id,
            f"""
            SELECT COUNT(*)
            FROM `{gold_features_window}`
            WHERE msg_per_sec IS NULL OR msg_per_sec < 0
            """
        )
        records.append(build_check(
            run_id=run_id,
            check_name="gold_msg_per_sec_valid",
            layer_name="Gold",
            table_name="gold_features_window",
            metric_value=invalid_msg_per_sec,
            threshold_value=0,
            operator="=",
            severity="CRITICAL",
            details="msg_per_sec must not be null or negative.",
        ))

        # 10. Gold window end > start
        invalid_windows = run_scalar_query(
            project_id,
            f"""
            SELECT COUNT(*)
            FROM `{gold_features_window}`
            WHERE window_end_us <= window_start_us
            """
        )
        records.append(build_check(
            run_id=run_id,
            check_name="gold_window_end_after_start",
            layer_name="Gold",
            table_name="gold_features_window",
            metric_value=invalid_windows,
            threshold_value=0,
            operator="=",
            severity="CRITICAL",
            details="window_end_us must be greater than window_start_us.",
        ))

        # 11. CAN ID Profile has rows
        can_id_profile_rows = run_scalar_query(
            project_id,
            f"SELECT COUNT(*) FROM `{can_id_profile}`"
        )
        records.append(build_check(
            run_id=run_id,
            check_name="can_id_profile_has_rows",
            layer_name="Gold",
            table_name="can_id_profile",
            metric_value=can_id_profile_rows,
            threshold_value=0,
            operator=">",
            severity="CRITICAL",
            details="CAN ID profile must contain rows.",
        ))

        # 12. CAN ID Profile has aid_hex
        missing_aid_hex = run_scalar_query(
            project_id,
            f"""
            SELECT COUNT(*)
            FROM `{can_id_profile}`
            WHERE aid_hex IS NULL OR aid_hex = ''
            """
        )
        records.append(build_check(
            run_id=run_id,
            check_name="can_id_profile_aid_hex_not_empty",
            layer_name="Gold",
            table_name="can_id_profile",
            metric_value=missing_aid_hex,
            threshold_value=0,
            operator="=",
            severity="CRITICAL",
            details="CAN ID profile must have aid_hex for all arbitration_id values.",
        ))

        # 13. Unknown CAN IDs controlled
        unknown_can_ids = run_scalar_query(
            project_id,
            f"""
            SELECT COUNT(*)
            FROM `{can_id_profile}`
            WHERE message_name LIKE 'UNKNOWN_%'
            """
        )
        records.append(build_check(
            run_id=run_id,
            check_name="can_id_profile_unknown_can_ids",
            layer_name="Gold",
            table_name="can_id_profile",
            metric_value=unknown_can_ids,
            threshold_value=20,
            operator="<=",
            severity="WARNING",
            details="Number of CAN IDs not covered by the DBC should remain controlled.",
        ))

        # 14. Failed processing statuses
        failed_files = run_scalar_query(
            project_id,
            f"""
            SELECT COUNT(*)
            FROM `{file_processing_status}`
            WHERE bronze_status = 'FAILED'
               OR silver_clean_status = 'FAILED'
               OR silver_iat_status = 'FAILED'
               OR gold_window_status = 'FAILED'
               OR can_id_profile_status = 'FAILED'
            """
        )
        records.append(build_check(
            run_id=run_id,
            check_name="file_processing_no_failed_status",
            layer_name="Audit",
            table_name="file_processing_status",
            metric_value=failed_files,
            threshold_value=0,
            operator="=",
            severity="CRITICAL",
            details="No processing step should be in FAILED status.",
        ))

        # 15. Pending after full run
        pending_files = run_scalar_query(
            project_id,
            f"""
            SELECT COUNT(*)
            FROM `{file_processing_status}`
            WHERE bronze_status = 'SUCCESS'
              AND (
                silver_clean_status != 'SUCCESS'
                OR silver_iat_status != 'SUCCESS'
                OR gold_window_status != 'SUCCESS'
                OR can_id_profile_status != 'SUCCESS'
              )
            """
        )
        records.append(build_check(
            run_id=run_id,
            check_name="file_processing_no_pending_after_full_run",
            layer_name="Audit",
            table_name="file_processing_status",
            metric_value=pending_files,
            threshold_value=0,
            operator="=",
            severity="WARNING",
            details="After a full successful run, no file should remain pending.",
        ))

        reports_df = create_quality_reports_df(spark, records)

        write_to_bigquery(
            df=reports_df,
            config=config,
            dataset_key="audit",
            table_key="quality_reports",
            mode="append",
        )

        critical_failures = [
            record for record in records
            if record[4] == "FAIL" and record[5] == "CRITICAL"
        ]

        final_status = "SUCCESS" if not critical_failures else "FAILED"
        error_message = None

        if critical_failures:
            error_message = f"{len(critical_failures)} critical quality check(s) failed."

        write_pipeline_run(
            spark=spark,
            config=config,
            run_id=run_id,
            step_name=step_name,
            status=final_status,
            started_at=started_at,
            input_rows=len(records),
            output_rows=len(records),
            error_message=error_message,
        )

        print("Quality Reports terminé.")
        print(f"Checks exécutés : {len(records)}")
        print(f"Critical failures : {len(critical_failures)}")

        if critical_failures:
            raise RuntimeError(error_message)

    except Exception as error:
        print(f"Quality Reports échoué : {str(error)}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    run()