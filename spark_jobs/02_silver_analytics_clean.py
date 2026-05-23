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
logger = logging.getLogger("silver_analytics_bigquery_native")
analytics = "analytics"


def transform_raw(df: DataFrame) -> DataFrame:
    if "timestamp_us" in df.columns:
        df = df.withColumn("timestamp_us", col("timestamp_us").cast(LongType()))
    if "arbitration_id" in df.columns:
        df = df.withColumn("arbitration_id", col("arbitration_id").cast(IntegerType()))
    if "dlc" in df.columns:
        df = df.withColumn("dlc", col("dlc").cast(IntegerType()))
    if "label" in df.columns:
        df = df.withColumn("label", col("label").cast(IntegerType()))
    start = df.groupBy("source_file", "dump_id").agg(spark_min("timestamp_us").alias("start_ts"))
    df = df.join(start, ["source_file", "dump_id"], "left")
    return (
        df.withColumn("event_id", md5(concat_ws("|", col("source_file"), col("timestamp_us").cast("string"), col("arbitration_id").cast("string"), col("dlc").cast("string"))))
        .withColumn("elapsed_seconds", (col("timestamp_us") - col("start_ts")) / lit(1000000.0))
        .withColumn("is_attack", col("label") == 1)
        .drop("start_ts")
        .dropDuplicates(["event_id"])
    )


def transform_signal(df: DataFrame) -> DataFrame:
    if "timestamp" in df.columns:
        df = df.withColumn("timestamp", col("timestamp").cast(DoubleType()))
    if "label" in df.columns:
        df = df.withColumn("label", col("label").cast(IntegerType()))
    start = df.groupBy("source_file", "dump_id").agg(spark_min("timestamp").alias("start_ts"))
    df = df.join(start, ["source_file", "dump_id"], "left")
    return (
        df.withColumn("event_id", md5(concat_ws("|", col("source_file"), col("timestamp").cast("string"), col("label").cast("string"))))
        .withColumn("elapsed_seconds", col("timestamp") - col("start_ts"))
        .withColumn("is_attack", col("label") == 1)
        .drop("start_ts")
        .dropDuplicates(["event_id"])
    )


def process_representation(spark: SparkSession, cfg: Dict[str, Any], rep: str) -> None:
    in_table = "raw_valid" if rep == "raw" else "signal_valid"
    out_table = "raw_clean" if rep == "raw" else "signal_clean"
    df = read_bq(spark, cfg, f"bronze_{analytics}", in_table)
    sources = [r["source_file"] for r in df.select("source_file").distinct().collect()]
    done = processed_sources(cfg, "silver", analytics, rep)
    new_sources = [s for s in sources if s not in done]
    if not new_sources:
        logger.info("No new bronze %s rows for silver %s", rep, analytics)
        return
    result = (transform_raw(df.filter(col("source_file").isin(new_sources))) if rep == "raw" else transform_signal(df.filter(col("source_file").isin(new_sources)))).persist(StorageLevel.MEMORY_AND_DISK)
    write_bq(result, cfg, f"silver_{analytics}", out_table, "append")
    for source in new_sources:
        write_audit_status(cfg, "silver", analytics, rep, source, "SUCCESS")
    result.unpersist()


def main():
    parser = argparse.ArgumentParser(description="Silver analytics clean to BigQuery native")
    parser.add_argument("--config_path", required=True)
    args = parser.parse_args()
    cfg = load_yaml(args.config_path)
    spark = create_spark("silver_analytics_bigquery_native")
    try:
        process_representation(spark, cfg, "raw")
        process_representation(spark, cfg, "signal")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
