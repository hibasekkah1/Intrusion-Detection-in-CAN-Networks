import argparse
import logging
from typing import Any, Dict, List, Set

from google.cloud import bigquery
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    concat_ws,
    count,
    floor,
    lit,
    max as spark_max,
    md5,
    min as spark_min,
    sum as spark_sum,
    when,
)

from common_bq import (
    ATTACK_TYPES,
    bq_table,
    create_spark,
    load_yaml,
    processed_sources,
    read_bq,
    table_exists,
    write_audit_status,
    write_bq,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("gold_analytics_window_5min_wide_v3_gold_time_fix")

GOLD_TECHNICAL_COLUMNS = {
    "window_5min",
    "capture_id",
    "window_id",
    "attack_id",
    "window_start_s",
    "window_end_s",
    "timestamp_norm_s",
    "start_timestamp_norm_s",
}


def metadata_columns() -> Set[str]:
    return {
        "event_id",
        "timestamp",
        "elapsed_seconds",
        "label",
        "is_attack",
        "source_file",
        "dump_id",
        "dataset_type",
        "attack_type",
        "attack_parameter",
        "target_aid",
        "domain",
        "representation",
        "ingested_at",
        "validated_at",
        "fuzz_rate",
        "replay_start_sec",
        "replay_end_sec",
        "ml_split",
        *GOLD_TECHNICAL_COLUMNS,
    }


def safe_optional(df: DataFrame) -> DataFrame:
    defaults = [
        ("attack_parameter", "", "string"),
        ("target_aid", "", "string"),
        ("fuzz_rate", None, "int"),
        ("replay_start_sec", None, "double"),
        ("replay_end_sec", None, "double"),
    ]
    for name, value, dtype in defaults:
        if name not in df.columns:
            df = df.withColumn(name, lit(value).cast(dtype))
    return df


def signal_columns(df: DataFrame) -> List[str]:
    meta = metadata_columns()
    return [c for c in df.columns if c not in meta]


def normalize_time_in_gold(df: DataFrame) -> DataFrame:
    """
    Correct elapsed_seconds in Gold without requiring Silver recomputation.

    Some Silver signal tables contain timestamp values in microseconds, for example
    25000000 meaning 25 seconds. If Gold uses the Silver elapsed_seconds directly,
    it can create hundreds of thousands of 5-minute windows.

    This function creates timestamp_norm_s and recomputes elapsed_seconds in seconds
    per source_file + dump_id before windowing.
    """
    if "timestamp" not in df.columns:
        logger.warning("Column timestamp is missing. Gold will use existing elapsed_seconds.")
        return df

    df = df.withColumn("timestamp", col("timestamp").cast("double"))

    df = df.withColumn(
        "timestamp_norm_s",
        when(spark_abs(col("timestamp")) >= lit(1000000.0), col("timestamp") / lit(1000000.0))
        .when(spark_abs(col("timestamp")) >= lit(1000.0), col("timestamp") / lit(1000.0))
        .otherwise(col("timestamp")),
    )

    starts = df.groupBy("source_file", "dump_id").agg(
        spark_min("timestamp_norm_s").alias("start_timestamp_norm_s")
    )

    df = df.join(starts, ["source_file", "dump_id"], "left")
    df = df.withColumn("elapsed_seconds", col("timestamp_norm_s") - col("start_timestamp_norm_s"))

    return df


def with_window_keys(df: DataFrame) -> DataFrame:
    df = safe_optional(df)
    df = normalize_time_in_gold(df)

    return (
        df.withColumn("window_5min", floor(col("elapsed_seconds") / lit(300.0)).cast("int"))
        .withColumn("capture_id", md5(concat_ws("|", col("source_file"), col("dump_id"))))
        .withColumn(
            "window_id",
            md5(concat_ws("|", col("source_file"), col("dump_id"), col("window_5min").cast("string"))),
        )
        .withColumn(
            "attack_id",
            md5(concat_ws("|", col("attack_type"), col("attack_parameter"), col("target_aid"))),
        )
        .withColumn("window_start_s", col("window_5min") * lit(300.0))
        .withColumn("window_end_s", (col("window_5min") + lit(1)) * lit(300.0))
    )


def build_fact_wide(df: DataFrame) -> DataFrame:
    logger.info("Detected signal columns before Gold keys: %s", len(signal_columns(df)))

    df = with_window_keys(df)
    sig_cols = signal_columns(df)
    logger.info("Detected signal columns for wide fact after excluding technical columns: %s", len(sig_cols))

    if not sig_cols:
        raise RuntimeError("No signal columns detected in silver signal_clean table")

    signal_aggs = [avg(col(c).cast("double")).alias(f"avg_{c}") for c in sig_cols]

    fact = df.groupBy(
        "capture_id",
        "window_id",
        "attack_id",
        "source_file",
        "dump_id",
        "window_5min",
        "window_start_s",
        "window_end_s",
        "attack_type",
    ).agg(
        count("*").alias("total_snapshot_count"),
        spark_sum(when(col("is_attack"), lit(1)).otherwise(lit(0))).alias("attack_snapshot_count"),
        *signal_aggs,
    )

    return (
        fact.withColumn("normal_snapshot_count", col("total_snapshot_count") - col("attack_snapshot_count"))
        .withColumn("attack_rate_pct", (col("attack_snapshot_count") / col("total_snapshot_count")) * lit(100.0))
        .withColumn("is_attack_window", col("attack_snapshot_count") > 0)
    )


def dim_attack(df: DataFrame) -> DataFrame:
    df = safe_optional(df)
    return (
        df.withColumn("attack_id", md5(concat_ws("|", col("attack_type"), col("attack_parameter"), col("target_aid"))))
        .select("attack_id", "attack_type", "attack_parameter", "target_aid", "fuzz_rate", "replay_start_sec", "replay_end_sec")
        .dropDuplicates(["attack_id"])
        .withColumn(
            "severity",
            when(col("attack_type") == "benign", lit("LOW"))
            .when(col("attack_type").isin("fuzz", "repl"), lit("MEDIUM"))
            .otherwise(lit("HIGH")),
        )
    )


def dim_capture(df: DataFrame) -> DataFrame:
    df = normalize_time_in_gold(df)
    grouped = df.groupBy("source_file", "dump_id").agg(
        spark_min("elapsed_seconds").alias("min_elapsed_seconds"),
        spark_max("elapsed_seconds").alias("max_elapsed_seconds"),
        count("*").alias("row_count"),
        spark_sum(when(col("label") == 0, lit(1)).otherwise(lit(0))).alias("label_0_count"),
        spark_sum(when(col("label") == 1, lit(1)).otherwise(lit(0))).alias("label_1_count"),
    )

    return (
        grouped.withColumn("capture_id", md5(concat_ws("|", col("source_file"), col("dump_id"))))
        .withColumn("dataset_type", when(col("label_1_count") > 0, lit("intrusion")).otherwise(lit("benign")))
        .withColumn("representation", lit("signal"))
        .withColumn("duration_seconds", col("max_elapsed_seconds") - col("min_elapsed_seconds"))
        .select("capture_id", "source_file", "dump_id", "dataset_type", "representation", "duration_seconds", "row_count", "label_0_count", "label_1_count")
    )


def dim_window(df: DataFrame) -> DataFrame:
    keyed = with_window_keys(df)
    bounds = keyed.filter(col("is_attack")).groupBy("source_file", "dump_id").agg(
        spark_min("elapsed_seconds").alias("attack_start_s"),
        spark_max("elapsed_seconds").alias("attack_end_s"),
    )

    result = (
        keyed.select("source_file", "dump_id", "window_id", "window_5min", "window_start_s", "window_end_s")
        .dropDuplicates(["window_id"])
        .join(bounds, ["source_file", "dump_id"], "left")
    )

    return (
        result.withColumn(
            "capture_phase",
            when(col("attack_start_s").isNull(), lit("no_attack"))
            .when(col("window_end_s") < col("attack_start_s"), lit("pre_attack"))
            .when(col("window_start_s") > col("attack_end_s"), lit("post_attack"))
            .otherwise(lit("during_attack")),
        )
        .select("window_id", "window_5min", "window_start_s", "window_end_s", "capture_phase")
    )


def collect_distinct(df: DataFrame, column_name: str) -> List[str]:
    return [r[column_name] for r in df.select(column_name).distinct().collect()]


def delete_by_ids(cfg: Dict[str, Any], dataset_key: str, table_name: str, id_column: str, ids: List[str]) -> None:
    if not ids or not table_exists(cfg, dataset_key, table_name):
        return

    client = bigquery.Client(project=cfg["bigquery"]["project_id"])
    table_id = bq_table(cfg, dataset_key, table_name)
    query = f"""
    DELETE FROM `{table_id}`
    WHERE {id_column} IN UNNEST(@ids)
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", ids)])
    logger.info("Deleting from %s by %s, ids=%s", table_id, id_column, len(ids))
    client.query(query, job_config=job_config).result()


def process_attack(spark, cfg: Dict[str, Any], attack_type: str, done_sources: set) -> None:
    table_name = f"signal_clean_{attack_type}"
    if not table_exists(cfg, "silver_analytics", table_name):
        logger.warning("Missing silver table, skipping: %s", bq_table(cfg, "silver_analytics", table_name))
        return

    logger.info("Processing wide Gold Analytics attack=%s from %s", attack_type, table_name)
    df = read_bq(spark, cfg, "silver_analytics", table_name)

    all_sources = collect_distinct(df, "source_file")
    new_sources = [s for s in all_sources if s not in done_sources]

    if not new_sources:
        logger.info("No new sources for attack=%s", attack_type)
        return

    logger.info("New sources for attack=%s: %s", attack_type, len(new_sources))
    df_new = df.filter(col("source_file").isin(new_sources)).persist(StorageLevel.MEMORY_AND_DISK)

    try:
        fact = build_fact_wide(df_new).persist(StorageLevel.MEMORY_AND_DISK)

        capture_ids = collect_distinct(fact, "capture_id")
        window_ids = collect_distinct(fact, "window_id")
        attack_ids = collect_distinct(fact, "attack_id")

        logger.info("Gold keys for attack=%s: capture_ids=%s, window_ids=%s, attack_ids=%s", attack_type, len(capture_ids), len(window_ids), len(attack_ids))

        delete_by_ids(cfg, "gold_analytics", "fact_window_5min_wide", "capture_id", capture_ids)
        delete_by_ids(cfg, "gold_analytics", "dim_capture", "capture_id", capture_ids)
        delete_by_ids(cfg, "gold_analytics", "dim_window", "window_id", window_ids)
        delete_by_ids(cfg, "gold_analytics", "dim_attack", "attack_id", attack_ids)

        write_bq(fact, cfg, "gold_analytics", "fact_window_5min_wide", "append")
        write_bq(dim_attack(df_new), cfg, "gold_analytics", "dim_attack", "append")
        write_bq(dim_capture(df_new), cfg, "gold_analytics", "dim_capture", "append")
        write_bq(dim_window(df_new), cfg, "gold_analytics", "dim_window", "append")

        for source in new_sources:
            write_audit_status(cfg, "gold", "analytics", "signal", source, "SUCCESS")

        fact.unpersist()
        logger.info("Completed wide Gold Analytics attack=%s", attack_type)
    finally:
        df_new.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold Analytics 5-minute wide fact table v3 with Gold-side time normalization")
    parser.add_argument("--config_path", required=True)
    parser.add_argument("--attack_type", default="all", choices=ATTACK_TYPES + ["all"])
    args = parser.parse_args()

    cfg = load_yaml(args.config_path)
    spark = create_spark("gold_analytics_window_5min_wide_v3_gold_time_fix")

    try:
        done_sources = processed_sources(cfg, "gold", "analytics", "signal")
        attacks = ATTACK_TYPES if args.attack_type == "all" else [args.attack_type]
        logger.info("Attacks to process: %s", attacks)

        for attack in attacks:
            process_attack(spark, cfg, attack, done_sources)
            done_sources = processed_sources(cfg, "gold", "analytics", "signal")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()