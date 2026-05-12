import argparse
import json
from datetime import datetime, timezone
from uuid import uuid4

import yaml
from google.cloud import storage

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

from great_expectations.dataset import SparkDFDataset


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


def get_config_value(config, path, default=None):
    current = config

    for key in path:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]

    return current


def create_spark_session(config):
    project_id = get_config_value(config, ["gcp", "project_id"])
    temp_bucket = get_config_value(config, ["gcp", "temp_bucket"], "can-ids-data")

    spark = (
        SparkSession.builder
        .appName("can-ids-great-expectations-quality")
        .config("spark.sql.shuffle.partitions", "20")
        .config("temporaryGcsBucket", temp_bucket)
        .getOrCreate()
    )

    spark.conf.set("parentProject", project_id)

    return spark


def bigquery_table(config, dataset_key, table_key):
    project_id = get_config_value(config, ["gcp", "project_id"])

    dataset = get_config_value(
        config,
        ["bigquery", "datasets", dataset_key],
    )

    table = get_config_value(
        config,
        ["bigquery", "tables", table_key],
    )

    return f"{project_id}.{dataset}.{table}"


def read_bigquery_table(spark, table_id):
    return (
        spark.read
        .format("bigquery")
        .option("table", table_id)
        .load()
    )


def write_to_bigquery(df, config, dataset_key, table_key, mode="append"):
    table_id = bigquery_table(config, dataset_key, table_key)
    temp_bucket = get_config_value(config, ["gcp", "temp_bucket"], "can-ids-data")

    (
        df.write
        .format("bigquery")
        .option("table", table_id)
        .option("temporaryGcsBucket", temp_bucket)
        .mode(mode)
        .save()
    )


def now_utc():
    return datetime.now(timezone.utc)


def safe_details(result):
    try:
        result_dict = result.to_json_dict()
    except Exception:
        try:
            result_dict = dict(result)
        except Exception:
            return str(result)

    compact = {
        "success": result_dict.get("success"),
        "expectation_type": result_dict.get("expectation_config", {}).get("expectation_type"),
        "kwargs": result_dict.get("expectation_config", {}).get("kwargs"),
        "result": result_dict.get("result"),
    }

    return json.dumps(compact, default=str)[:8000]


def observed_metric(result):
    try:
        result_dict = result.to_json_dict()
    except Exception:
        try:
            result_dict = dict(result)
        except Exception:
            return None

    result_section = result_dict.get("result", {})

    if "observed_value" in result_section:
        try:
            return float(result_section["observed_value"])
        except Exception:
            return None

    if "unexpected_count" in result_section:
        try:
            return float(result_section["unexpected_count"])
        except Exception:
            return None

    return None


def build_record(
    run_id,
    check_name,
    layer_name,
    table_name,
    result,
    severity="CRITICAL",
    threshold_value=0.0,
):
    success = bool(result.success)
    metric_value = observed_metric(result)

    if metric_value is None:
        metric_value = 1.0 if success else 0.0

    return (
        run_id,
        check_name,
        layer_name,
        table_name,
        "PASS" if success else "FAIL",
        severity,
        float(metric_value),
        float(threshold_value),
        safe_details(result),
        now_utc(),
    )


def create_quality_df(spark, records):
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


def validate_bronze(bronze_df, run_id):
    records = []

    gx_df = SparkDFDataset(bronze_df)

    records.append(build_record(
        run_id,
        "gx_bronze_raw_has_rows",
        "Bronze",
        "messages_raw",
        gx_df.expect_table_row_count_to_be_between(min_value=1),
        threshold_value=1.0,
    ))

    records.append(build_record(
        run_id,
        "gx_bronze_source_file_not_null",
        "Bronze",
        "messages_raw",
        gx_df.expect_column_values_to_not_be_null("source_file"),
    ))

    records.append(build_record(
        run_id,
        "gx_bronze_session_id_not_null",
        "Bronze",
        "messages_raw",
        gx_df.expect_column_values_to_not_be_null("session_id"),
    ))

    records.append(build_record(
        run_id,
        "gx_bronze_timestamp_us_not_null",
        "Bronze",
        "messages_raw",
        gx_df.expect_column_values_to_not_be_null("timestamp_us"),
    ))

    records.append(build_record(
        run_id,
        "gx_bronze_arbitration_id_between_0_2047",
        "Bronze",
        "messages_raw",
        gx_df.expect_column_values_to_be_between(
            "arbitration_id",
            min_value=0,
            max_value=2047,
        ),
    ))

    records.append(build_record(
        run_id,
        "gx_bronze_dlc_between_0_8",
        "Bronze",
        "messages_raw",
        gx_df.expect_column_values_to_be_between(
            "dlc",
            min_value=0,
            max_value=8,
        ),
    ))

    records.append(build_record(
        run_id,
        "gx_bronze_label_in_0_1",
        "Bronze",
        "messages_raw",
        gx_df.expect_column_values_to_be_in_set(
            "label",
            value_set=[0, 1],
        ),
    ))

    return records


def validate_silver_iat(silver_iat_df, run_id):
    records = []

    gx_df = SparkDFDataset(silver_iat_df)

    records.append(build_record(
        run_id,
        "gx_silver_iat_has_rows",
        "Silver",
        "messages_with_iat",
        gx_df.expect_table_row_count_to_be_between(min_value=1),
        threshold_value=1.0,
    ))

    records.append(build_record(
        run_id,
        "gx_silver_session_id_not_null",
        "Silver",
        "messages_with_iat",
        gx_df.expect_column_values_to_not_be_null("session_id"),
    ))

    records.append(build_record(
        run_id,
        "gx_silver_aid_hex_not_null",
        "Silver",
        "messages_with_iat",
        gx_df.expect_column_values_to_not_be_null("aid_hex"),
    ))

    records.append(build_record(
        run_id,
        "gx_silver_timestamp_us_not_null",
        "Silver",
        "messages_with_iat",
        gx_df.expect_column_values_to_not_be_null("timestamp_us"),
    ))

    iat_not_null_df = silver_iat_df.filter("iat_us IS NOT NULL")
    gx_iat_df = SparkDFDataset(iat_not_null_df)

    records.append(build_record(
        run_id,
        "gx_silver_iat_not_negative",
        "Silver",
        "messages_with_iat",
        gx_iat_df.expect_column_values_to_be_between(
            "iat_us",
            min_value=0,
        ),
    ))

    return records


def validate_gold_features(gold_df, run_id):
    records = []

    gx_df = SparkDFDataset(gold_df)

    records.append(build_record(
        run_id,
        "gx_gold_features_has_rows",
        "Gold",
        "gold_features_window",
        gx_df.expect_table_row_count_to_be_between(min_value=1),
        threshold_value=1.0,
    ))

    records.append(build_record(
        run_id,
        "gx_gold_arbitration_id_not_null",
        "Gold",
        "gold_features_window",
        gx_df.expect_column_values_to_not_be_null("arbitration_id"),
    ))

    records.append(build_record(
        run_id,
        "gx_gold_aid_hex_not_null",
        "Gold",
        "gold_features_window",
        gx_df.expect_column_values_to_not_be_null("aid_hex"),
    ))

    records.append(build_record(
        run_id,
        "gx_gold_msg_per_sec_not_negative",
        "Gold",
        "gold_features_window",
        gx_df.expect_column_values_to_be_between(
            "msg_per_sec",
            min_value=0,
        ),
    ))

    records.append(build_record(
        run_id,
        "gx_gold_iat_mean_not_negative",
        "Gold",
        "gold_features_window",
        gx_df.expect_column_values_to_be_between(
            "iat_mean_us",
            min_value=0,
        ),
    ))

    records.append(build_record(
        run_id,
        "gx_gold_entropy_not_negative",
        "Gold",
        "gold_features_window",
        gx_df.expect_column_values_to_be_between(
            "entropy_bits",
            min_value=0,
        ),
    ))

    records.append(build_record(
        run_id,
        "gx_gold_avg_zeros_ratio_between_0_1",
        "Gold",
        "gold_features_window",
        gx_df.expect_column_values_to_be_between(
            "avg_zeros_ratio",
            min_value=0,
            max_value=1,
        ),
    ))

    records.append(build_record(
        run_id,
        "gx_gold_dlc_distinct_count_positive",
        "Gold",
        "gold_features_window",
        gx_df.expect_column_values_to_be_between(
            "dlc_distinct_count",
            min_value=1,
        ),
    ))

    return records


def validate_can_id_profile(profile_df, run_id):
    records = []

    gx_df = SparkDFDataset(profile_df)

    records.append(build_record(
        run_id,
        "gx_profile_has_rows",
        "Gold",
        "can_id_profile",
        gx_df.expect_table_row_count_to_be_between(min_value=1),
        threshold_value=1.0,
    ))

    records.append(build_record(
        run_id,
        "gx_profile_arbitration_id_not_null",
        "Gold",
        "can_id_profile",
        gx_df.expect_column_values_to_not_be_null("arbitration_id"),
    ))

    records.append(build_record(
        run_id,
        "gx_profile_aid_hex_not_null",
        "Gold",
        "can_id_profile",
        gx_df.expect_column_values_to_not_be_null("aid_hex"),
    ))

    records.append(build_record(
        run_id,
        "gx_profile_message_name_not_null",
        "Gold",
        "can_id_profile",
        gx_df.expect_column_values_to_not_be_null("message_name"),
    ))

    records.append(build_record(
        run_id,
        "gx_profile_messages_count_positive",
        "Gold",
        "can_id_profile",
        gx_df.expect_column_values_to_be_between(
            "messages_count",
            min_value=1,
        ),
    ))

    return records


def run():
    args = parse_args()
    config = load_config(args.config_path)

    spark = create_spark_session(config)

    run_id = f"gx_run_{uuid4()}"

    try:
        bronze_table = bigquery_table(config, "bronze", "bronze_raw")
        silver_iat_table = bigquery_table(config, "silver", "silver_iat")
        gold_features_table = bigquery_table(config, "gold", "gold_features_window")
        can_id_profile_table = bigquery_table(config, "gold", "can_id_profile")

        print(f"Reading Bronze table: {bronze_table}")
        bronze_df = read_bigquery_table(spark, bronze_table)

        print(f"Reading Silver IAT table: {silver_iat_table}")
        silver_iat_df = read_bigquery_table(spark, silver_iat_table)

        print(f"Reading Gold Features table: {gold_features_table}")
        gold_df = read_bigquery_table(spark, gold_features_table)

        print(f"Reading CAN ID Profile table: {can_id_profile_table}")
        profile_df = read_bigquery_table(spark, can_id_profile_table)

        records = []

        records.extend(validate_bronze(bronze_df, run_id))
        records.extend(validate_silver_iat(silver_iat_df, run_id))
        records.extend(validate_gold_features(gold_df, run_id))
        records.extend(validate_can_id_profile(profile_df, run_id))

        quality_df = create_quality_df(spark, records)

        write_to_bigquery(
            df=quality_df,
            config=config,
            dataset_key="audit",
            table_key="quality_reports",
            mode="append",
        )

        failures = [
            record for record in records
            if record[4] == "FAIL" and record[5] == "CRITICAL"
        ]

        print("Great Expectations checks completed.")
        print(f"Total checks: {len(records)}")
        print(f"Critical failures: {len(failures)}")

        if failures:
            failed_names = [record[1] for record in failures]
            raise RuntimeError(
                f"{len(failures)} GX critical check(s) failed: {failed_names}"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    run()