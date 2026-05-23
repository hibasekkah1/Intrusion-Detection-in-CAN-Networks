import argparse
import json
import logging
import os
import tempfile
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
from google.cloud import bigquery, storage
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, md5, concat_ws, min as spark_min
from pyspark.sql.types import DoubleType, IntegerType, LongType

try:
    import pyarrow.parquet as pq
except Exception:
    pq = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
ATTACK_TYPES = ["benign", "fuzz", "fabr", "masq", "susp", "repl"]


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_gcs_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {uri}")
    return uri.replace("gs://", "", 1).split("/", 1)


def load_yaml(path: str) -> Dict[str, Any]:
    if path.startswith("gs://"):
        bucket, blob = parse_gcs_uri(path)
        return yaml.safe_load(storage.Client().bucket(bucket).blob(blob).download_as_text())
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "120")
        .config("spark.default.parallelism", "120")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )


def bq_table(cfg: Dict[str, Any], dataset_key: str, table_name: str) -> str:
    project = cfg["bigquery"]["project_id"]
    dataset = cfg["bigquery"]["datasets"][dataset_key]
    return f"{project}.{dataset}.{table_name}"


def read_bq(spark: SparkSession, cfg: Dict[str, Any], dataset_key: str, table_name: str) -> DataFrame:
    return spark.read.format("bigquery").option("table", bq_table(cfg, dataset_key, table_name)).load()


def write_bq(df: DataFrame, cfg: Dict[str, Any], dataset_key: str, table_name: str, mode: str = "append") -> None:
    table_id = bq_table(cfg, dataset_key, table_name)
    logging.getLogger("bq_writer").info("Writing BigQuery native table: %s", table_id)
    (
        df.write
        .format("bigquery")
        .option("table", table_id)
        .option("writeMethod", "direct")
        .mode(mode)
        .save()
    )


def audit_table(cfg: Dict[str, Any], table_name: str) -> str:
    project = cfg["bigquery"]["project_id"]
    dataset = cfg["bigquery"]["datasets"]["audit"]
    return f"{project}.{dataset}.{table_name}"


def processed_sources(cfg: Dict[str, Any], layer: str, domain: str, representation: str) -> Set[str]:
    client = bigquery.Client(project=cfg["bigquery"]["project_id"])
    table = audit_table(cfg, "file_processing_status")
    query = f"""
    SELECT source_file
    FROM `{table}`
    WHERE layer = @layer
      AND domain = @domain
      AND representation = @representation
      AND status = 'SUCCESS'
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("layer", "STRING", layer),
            bigquery.ScalarQueryParameter("domain", "STRING", domain),
            bigquery.ScalarQueryParameter("representation", "STRING", representation),
        ]
    )
    try:
        return {row["source_file"] for row in client.query(query, job_config=job_config).result()}
    except Exception:
        return set()


def write_audit_status(cfg: Dict[str, Any], layer: str, domain: str, representation: str, source_file: str, status: str = "SUCCESS", rows_written: int = -1, error_message: Optional[str] = None) -> None:
    client = bigquery.Client(project=cfg["bigquery"]["project_id"])
    table = audit_table(cfg, "file_processing_status")
    rows = [{
        "layer": layer,
        "domain": domain,
        "representation": representation,
        "source_file": source_file,
        "status": status,
        "rows_written": rows_written,
        "error_message": error_message,
        "processed_at": now_utc(),
    }]
    errors = client.insert_rows_json(table, rows)
    if errors:
        raise RuntimeError(f"Failed to insert audit row: {errors}")

from pyspark import StorageLevel
from pyspark.sql.functions import array, avg, count, explode, floor, max as spark_max, stddev_pop, struct, sum as spark_sum, when
logger = logging.getLogger("gold_analytics_bigquery_native")


def metadata_columns() -> set:
    return {"event_id", "timestamp", "elapsed_seconds", "label", "is_attack", "source_file", "dump_id", "dataset_type", "attack_type", "attack_parameter", "target_aid", "domain", "representation", "ingested_at", "validated_at", "fuzz_rate", "replay_start_sec", "replay_end_sec", "ml_split"}


def safe_optional(df: DataFrame) -> DataFrame:
    defaults = [("attack_parameter", "", "string"), ("target_aid", "", "string"), ("fuzz_rate", None, "int"), ("replay_start_sec", None, "double"), ("replay_end_sec", None, "double")]
    for c, v, t in defaults:
        if c not in df.columns:
            df = df.withColumn(c, lit(v).cast(t))
    return df


def build_fact(df: DataFrame) -> DataFrame:
    df = safe_optional(df)
    signal_cols = [c for c in df.columns if c not in metadata_columns()]
    structs = [struct(lit(c).alias("signal_column_name"), col(c).cast("double").alias("signal_value")) for c in signal_cols]
    df = df.withColumn("window_5min", floor(col("elapsed_seconds") / lit(300)).cast("int"))
    df = df.withColumn("capture_id", md5(concat_ws("|", col("source_file"), col("dump_id"))))
    df = df.withColumn("window_id", md5(concat_ws("|", col("source_file"), col("dump_id"), col("window_5min").cast("string"))))
    df = df.withColumn("attack_id", md5(concat_ws("|", col("attack_type"), col("attack_parameter"), col("target_aid"))))
    long_df = df.select("capture_id", "window_id", "attack_id", "window_5min", "attack_type", "is_attack", explode(array(*structs)).alias("signal"))
    long_df = long_df.select("capture_id", "window_id", "attack_id", "window_5min", "attack_type", "is_attack", col("signal.signal_column_name"), col("signal.signal_value")).filter(col("signal_value").isNotNull())
    fact = long_df.groupBy("capture_id", "window_id", "attack_id", "window_5min", "attack_type", "signal_column_name").agg(avg("signal_value").alias("avg_signal_value"), spark_min("signal_value").alias("min_signal_value"), spark_max("signal_value").alias("max_signal_value"), stddev_pop("signal_value").alias("std_signal_value"), count("*").alias("total_snapshot_count"), spark_sum(when(col("is_attack"), lit(1)).otherwise(lit(0))).alias("attack_snapshot_count"))
    return fact.withColumn("normal_snapshot_count", col("total_snapshot_count") - col("attack_snapshot_count")).withColumn("signal_variation", col("max_signal_value") - col("min_signal_value")).withColumn("attack_rate_pct", (col("attack_snapshot_count") / col("total_snapshot_count")) * lit(100.0)).withColumn("is_attack_window", col("attack_snapshot_count") > 0).withColumn("signal_id", md5(col("signal_column_name"))).withColumn("window_signal_id", md5(concat_ws("|", col("window_id"), col("signal_column_name"))))


def dim_signal(fact: DataFrame) -> DataFrame:
    return fact.select("signal_id", "signal_column_name").dropDuplicates(["signal_id"])


def dim_attack(df: DataFrame) -> DataFrame:
    df = safe_optional(df)
    d = df.withColumn("attack_id", md5(concat_ws("|", col("attack_type"), col("attack_parameter"), col("target_aid")))).select("attack_id", "attack_type", "attack_parameter", "target_aid", "fuzz_rate", "replay_start_sec", "replay_end_sec").dropDuplicates(["attack_id"])
    return d.withColumn("severity", when(col("attack_type") == "benign", lit("LOW")).when(col("attack_type").isin("fuzz", "repl"), lit("MEDIUM")).otherwise(lit("HIGH")))


def dim_capture(df: DataFrame) -> DataFrame:
    b = df.groupBy("source_file", "dump_id").agg(spark_min("elapsed_seconds").alias("min_elapsed_seconds"), spark_max("elapsed_seconds").alias("max_elapsed_seconds"), count("*").alias("row_count"), spark_sum(when(col("label") == 0, lit(1)).otherwise(lit(0))).alias("label_0_count"), spark_sum(when(col("label") == 1, lit(1)).otherwise(lit(0))).alias("label_1_count"))
    return b.withColumn("capture_id", md5(concat_ws("|", col("source_file"), col("dump_id")))).withColumn("dataset_type", when(col("label_1_count") > 0, lit("intrusion")).otherwise(lit("benign"))).withColumn("representation", lit("signal")).withColumn("duration_seconds", col("max_elapsed_seconds") - col("min_elapsed_seconds")).select("capture_id", "source_file", "dump_id", "dataset_type", "representation", "duration_seconds", "row_count", "label_0_count", "label_1_count")


def dim_window(df: DataFrame) -> DataFrame:
    w = df.withColumn("window_5min", floor(col("elapsed_seconds") / lit(300)).cast("int")).withColumn("window_id", md5(concat_ws("|", col("source_file"), col("dump_id"), col("window_5min").cast("string")))).withColumn("window_start_s", col("window_5min") * lit(300.0)).withColumn("window_end_s", (col("window_5min") + lit(1)) * lit(300.0))
    bounds = df.filter(col("is_attack")).groupBy("source_file", "dump_id").agg(spark_min("elapsed_seconds").alias("attack_start_s"), spark_max("elapsed_seconds").alias("attack_end_s"))
    d = w.select("source_file", "dump_id", "window_id", "window_5min", "window_start_s", "window_end_s").dropDuplicates(["window_id"]).join(bounds, ["source_file", "dump_id"], "left")
    return d.withColumn("capture_phase", when(col("attack_start_s").isNull(), lit("no_attack")).when(col("window_end_s") < col("attack_start_s"), lit("pre_attack")).when(col("window_start_s") > col("attack_end_s"), lit("post_attack")).otherwise(lit("during_attack"))).select("window_id", "window_5min", "window_start_s", "window_end_s", "capture_phase")


def main():
    parser = argparse.ArgumentParser(description="Gold Analytics Star Schema to BigQuery native")
    parser.add_argument("--config_path", required=True)
    args = parser.parse_args()
    cfg = load_yaml(args.config_path)
    spark = create_spark("gold_analytics_bigquery_native")
    try:
        df = read_bq(spark, cfg, "silver_analytics", "signal_clean")
        sources = [r["source_file"] for r in df.select("source_file").distinct().collect()]
        done = processed_sources(cfg, "gold", "analytics", "signal")
        new_sources = [s for s in sources if s not in done]
        if not new_sources:
            logger.info("No new silver analytics signal rows for gold analytics")
            return
        df_new = df.filter(col("source_file").isin(new_sources)).persist(StorageLevel.MEMORY_AND_DISK)
        fact = build_fact(df_new).persist(StorageLevel.MEMORY_AND_DISK)
        write_bq(fact, cfg, "gold_analytics", "fact_signal_window_5min", "append")
        write_bq(dim_signal(fact), cfg, "gold_analytics", "dim_signal", "append")
        write_bq(dim_attack(df_new), cfg, "gold_analytics", "dim_attack", "append")
        write_bq(dim_capture(df_new), cfg, "gold_analytics", "dim_capture", "append")
        write_bq(dim_window(df_new), cfg, "gold_analytics", "dim_window", "append")
        for source in new_sources:
            write_audit_status(cfg, "gold", "analytics", "signal", source, "SUCCESS")
        fact.unpersist(); df_new.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
