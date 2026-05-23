import argparse
import logging
from typing import Any, Dict

import yaml
from google.cloud import bigquery, storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("create_bigquery_native_schema")


def parse_gcs_uri(uri: str):
    return uri.replace("gs://", "", 1).split("/", 1)


def load_yaml(path: str) -> Dict[str, Any]:
    if path.startswith("gs://"):
        b, k = parse_gcs_uri(path)
        return yaml.safe_load(storage.Client().bucket(b).blob(k).download_as_text())
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dataset(client, project, dataset_id, location):
    dataset = bigquery.Dataset(f"{project}.{dataset_id}")
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)
    logger.info("Dataset ready: %s.%s", project, dataset_id)


def ensure_audit_tables(client, project, audit_dataset):
    fps_schema = [
        bigquery.SchemaField("layer", "STRING"),
        bigquery.SchemaField("domain", "STRING"),
        bigquery.SchemaField("representation", "STRING"),
        bigquery.SchemaField("source_file", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("rows_written", "INT64"),
        bigquery.SchemaField("error_message", "STRING"),
        bigquery.SchemaField("processed_at", "TIMESTAMP"),
    ]
    dqr_schema = [
        bigquery.SchemaField("check_id", "STRING"),
        bigquery.SchemaField("table_name", "STRING"),
        bigquery.SchemaField("check_name", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("row_count", "INT64"),
        bigquery.SchemaField("error_message", "STRING"),
        bigquery.SchemaField("checked_at", "TIMESTAMP"),
    ]
    for table_name, schema in [("file_processing_status", fps_schema), ("data_quality_results", dqr_schema)]:
        table_id = f"{project}.{audit_dataset}.{table_name}"
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table, exists_ok=True)
        logger.info("Audit table ready: %s", table_id)


def main():
    parser = argparse.ArgumentParser(description="Create BigQuery native datasets and audit tables")
    parser.add_argument("--config_path", required=True)
    args = parser.parse_args()
    cfg = load_yaml(args.config_path)
    project = cfg["bigquery"]["project_id"]
    location = cfg["bigquery"].get("location", "europe-southwest1")
    datasets = cfg["bigquery"]["datasets"]
    client = bigquery.Client(project=project)
    for dataset_id in datasets.values():
        ensure_dataset(client, project, dataset_id, location)
    ensure_audit_tables(client, project, datasets["audit"])


if __name__ == "__main__":
    main()
