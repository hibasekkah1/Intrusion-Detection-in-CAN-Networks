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

logger = logging.getLogger("bronze_analytics_bigquery_native")
analytics = "analytics"


def list_parquet_files(prefix_uri: str) -> List[Dict[str, str]]:
    bucket, prefix = parse_gcs_uri(prefix_uri.rstrip("/"))
    files = []
    for blob in storage.Client().list_blobs(bucket, prefix=prefix.rstrip("/") + "/"):
        if blob.name.endswith(".parquet"):
            files.append({"source_file": os.path.basename(blob.name), "gcs_uri": f"gs://{bucket}/{blob.name}"})
    return files


def make_unique_columns(cols: List[str]) -> List[str]:
    counts = Counter()
    out = []
    for c in cols:
        safe = str(c).strip().replace(" ", "_").replace("-", "_").lower()
        counts[safe] += 1
        out.append(safe if counts[safe] == 1 else f"{safe}_{counts[safe]}")
    return out


def deduplicate_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*make_unique_columns(df.columns))


def download_gcs_file(gcs_uri: str, local_path: str) -> None:
    bucket, blob = parse_gcs_uri(gcs_uri)
    storage.Client().bucket(bucket).blob(blob).download_to_filename(local_path)


def read_parquet_resilient(spark: SparkSession, gcs_uri: str) -> Optional[DataFrame]:
    try:
        return deduplicate_columns(spark.read.option("mergeSchema", "false").parquet(gcs_uri))
    except Exception as err:
        text = str(err)
        logger.warning("Spark read failed for %s: %s", gcs_uri, text)
        if "COLUMN_ALREADY_EXISTS" not in text and "already exists" not in text:
            raise
        if pq is None:
            raise RuntimeError("pyarrow is required to repair duplicate-column parquet files") from err
        with tempfile.TemporaryDirectory() as tmp:
            local_file = os.path.join(tmp, os.path.basename(gcs_uri))
            download_gcs_file(gcs_uri, local_file)
            table = pq.read_table(local_file)
            table = table.rename_columns(make_unique_columns(table.schema.names))
            pdf = table.to_pandas().fillna("")
            if pdf.empty:
                return None
            return spark.createDataFrame(pdf)


def detect_attack(source_file: str) -> str:
    lower = source_file.lower()
    for attack in ATTACK_TYPES:
        if attack in lower:
            return attack
    return "benign"


def add_metadata(df: DataFrame, source_file: str, rep: str) -> DataFrame:
    return (
        df.withColumn("source_file", lit(source_file))
        .withColumn("domain", lit(analytics))
        .withColumn("representation", lit(rep))
        .withColumn("attack_type", lit(detect_attack(source_file)))
        .withColumn("dump_id", lit(source_file.split(".")[0]))
        .withColumn("dataset_type", lit("xcanids"))
        .withColumn("attack_parameter", lit(""))
        .withColumn("target_aid", lit(""))
        .withColumn("fuzz_rate", lit(None).cast("int"))
        .withColumn("replay_start_sec", lit(None).cast("double"))
        .withColumn("replay_end_sec", lit(None).cast("double"))
        .withColumn("ingested_at", current_timestamp())
    )


def normalize_raw(df: DataFrame) -> DataFrame:
    if "timestamp_us" in df.columns:
        df = df.withColumn("timestamp_us", col("timestamp_us").cast(LongType()))
    elif "timestamp" in df.columns:
        df = df.withColumn("timestamp_us", (col("timestamp").cast(DoubleType()) * lit(1000000)).cast(LongType()))
    if "arbitration_id" in df.columns:
        df = df.withColumn("arbitration_id", col("arbitration_id").cast(IntegerType()))
    if "dlc" in df.columns:
        df = df.withColumn("dlc", col("dlc").cast(IntegerType()))
    if "label" in df.columns:
        df = df.withColumn("label", col("label").cast(IntegerType()))
    return df


def normalize_signal(df: DataFrame) -> DataFrame:
    if "timestamp" in df.columns:
        df = df.withColumn("timestamp", col("timestamp").cast(DoubleType()))
    if "label" in df.columns:
        df = df.withColumn("label", col("label").cast(IntegerType()))
    else:
        df = df.withColumn("label", lit(0).cast(IntegerType()))
    return df


def process_representation(spark: SparkSession, cfg: Dict[str, Any], rep: str) -> None:
    landing = cfg["gcs_paths"]["landing"][rep]
    table_name = "raw_valid" if rep == "raw" else "signal_valid"
    done = processed_sources(cfg, "bronze", analytics, rep)
    files = [f for f in list_parquet_files(landing) if f["source_file"] not in done]
    if not files:
        logger.info("No new landing %s files for bronze %s", rep, analytics)
        return
    for f in files:
        try:
            df = read_parquet_resilient(spark, f["gcs_uri"])
            if df is None:
                continue
            df = normalize_raw(df) if rep == "raw" else normalize_signal(df)
            df = add_metadata(df, f["source_file"], rep)
            write_bq(df, cfg, f"bronze_{analytics}", table_name, "append")
            write_audit_status(cfg, "bronze", analytics, rep, f["source_file"], "SUCCESS")
        except Exception as e:
            write_audit_status(cfg, "bronze", analytics, rep, f["source_file"], "FAILED", error_message=str(e))
            raise


def main():
    parser = argparse.ArgumentParser(description="Bronze analytics ingestion to BigQuery native")
    parser.add_argument("--config_path", required=True)
    args = parser.parse_args()
    cfg = load_yaml(args.config_path)
    spark = create_spark("bronze_analytics_bigquery_native")
    try:
        process_representation(spark, cfg, "raw")
        process_representation(spark, cfg, "signal")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
