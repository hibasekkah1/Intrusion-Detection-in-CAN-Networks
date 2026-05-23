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
from pyspark.sql.functions import pmod, abs as spark_abs, hash as spark_hash, when
logger = logging.getLogger("gold_ml_bigquery_native")


def add_split(df: DataFrame) -> DataFrame:
    df = df.withColumn("split_bucket", pmod(spark_abs(spark_hash(col("event_id"))), lit(100)))
    return df.withColumn("ml_split", when(col("split_bucket") < 70, lit("train")).when(col("split_bucket") < 90, lit("validation")).otherwise(lit("test"))).drop("split_bucket")


def main():
    parser = argparse.ArgumentParser(description="Gold ML One Big Table to BigQuery native")
    parser.add_argument("--config_path", required=True)
    args = parser.parse_args()
    cfg = load_yaml(args.config_path)
    spark = create_spark("gold_ml_bigquery_native")
    try:
        df = read_bq(spark, cfg, "silver_ml", "signal_clean")
        sources = [r["source_file"] for r in df.select("source_file").distinct().collect()]
        done = processed_sources(cfg, "gold", "ml", "signal")
        new_sources = [s for s in sources if s not in done]
        if not new_sources:
            logger.info("No new silver ML signal rows for gold ML")
            return
        out = add_split(df.filter(col("source_file").isin(new_sources))).persist(StorageLevel.MEMORY_AND_DISK)
        write_bq(out, cfg, "gold_ml", "signal_big_table", "append")
        for source in new_sources:
            write_audit_status(cfg, "gold", "ml", "signal", source, "SUCCESS")
        out.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
