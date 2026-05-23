import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

import yaml
from google.cloud import bigquery, storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("quality_checks_bigquery_native")


def parse_gcs_uri(uri: str): return uri.replace("gs://", "", 1).split("/", 1)

def load_yaml(path: str) -> Dict[str, Any]:
    if path.startswith("gs://"):
        b, k = parse_gcs_uri(path)
        return yaml.safe_load(storage.Client().bucket(b).blob(k).download_as_text())
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def audit_table(cfg):
    return f"{cfg['bigquery']['project_id']}.{cfg['bigquery']['datasets']['audit']}.data_quality_results"


def table_name(cfg, dataset_key, table):
    return f"{cfg['bigquery']['project_id']}.{cfg['bigquery']['datasets'][dataset_key]}.{table}"


def count_rows(client, table):
    return int(list(client.query(f"SELECT COUNT(*) c FROM `{table}`").result())[0]["c"])


def check_table(client, table, required_columns):
    status = "PASS"; error = None; row_count = None
    try:
        row_count = count_rows(client, table)
        if row_count == 0:
            status = "WARN"; error = "empty_table"
        for c in required_columns:
            list(client.query(f"SELECT `{c}` FROM `{table}` LIMIT 1").result())
    except Exception as e:
        status = "FAIL"; error = str(e)
    return {"table_name": table, "check_name": "required_columns_and_count", "status": status, "row_count": row_count, "error_message": error, "checked_at": datetime.now(timezone.utc).isoformat()}


def main():
    parser = argparse.ArgumentParser(description="Quality checks stored in BigQuery audit")
    parser.add_argument("--config_path", required=True)
    args = parser.parse_args()
    cfg = load_yaml(args.config_path)
    client = bigquery.Client(project=cfg["bigquery"]["project_id"])
    checks = [
        (table_name(cfg, "gold_ml", "signal_big_table"), ["event_id", "source_file", "attack_type", "label", "ml_split"]),
        (table_name(cfg, "gold_analytics", "fact_signal_window_5min"), ["window_signal_id", "window_id", "capture_id", "attack_id", "signal_id"]),
        (table_name(cfg, "gold_analytics", "dim_signal"), ["signal_id", "signal_column_name"]),
        (table_name(cfg, "gold_analytics", "dim_attack"), ["attack_id", "attack_type"]),
        (table_name(cfg, "gold_analytics", "dim_capture"), ["capture_id", "source_file"]),
        (table_name(cfg, "gold_analytics", "dim_window"), ["window_id", "window_5min"]),
    ]
    rows = []
    for table, cols in checks:
        r = check_table(client, table, cols)
        r["check_id"] = f"{table}:{r['check_name']}:{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        rows.append(r)
    errors = client.insert_rows_json(audit_table(cfg), rows)
    if errors:
        raise RuntimeError(errors)
    failed = [r for r in rows if r["status"] == "FAIL"]
    if failed:
        raise RuntimeError(f"Quality checks failed: {failed}")


if __name__ == "__main__":
    main()
