from datetime import datetime, timezone
from uuid import uuid4

from ingestion.config_loader import load_config
from ingestion.gcs_reader import list_parquet_files
from ingestion.bq_loader import (
    initialize_bigquery,
    insert_pipeline_run,
    load_gcs_parquet_to_bronze,
)


def run_ingestion():
    config = load_config()

    run_id = f"bronze_ingestion_{uuid4()}"
    pipeline_name = "bronze_ingestion"
    started_at = datetime.now(timezone.utc)

    project_id = config["gcp"]["project_id"]
    location = config["bigquery"]["location"]
    bronze_dataset = config["bigquery"]["datasets"]["bronze"]
    pipeline_table = config["bigquery"]["tables"]["pipeline_runs"]

    pipeline_table_id = f"{project_id}.{bronze_dataset}.{pipeline_table}"

    total_loaded = 0
    total_rejected = 0
    total_skipped = 0
    files_processed = 0

    client = None

    try:
        client = initialize_bigquery(config)

        bucket_name = config["gcp"]["bucket_name"]
        source_prefix = config["storage"]["source_prefix"]

        parquet_files = list_parquet_files(bucket_name, source_prefix)

        if not parquet_files:
            finished_at = datetime.now(timezone.utc)

            insert_pipeline_run(
                client=client,
                pipeline_table_id=pipeline_table_id,
                run_id=run_id,
                pipeline_name=pipeline_name,
                status="SUCCESS",
                started_at=started_at,
                finished_at=finished_at,
                files_processed=0,
                files_skipped=0,
                rows_loaded=0,
                rows_rejected=0,
                error_message=None,
                location=location,
            )

            print("Aucun fichier Parquet trouvé.")
            return

        for parquet_file in parquet_files:
            print(f"Traitement du fichier : {parquet_file}")

            result = load_gcs_parquet_to_bronze(
                bucket_name=bucket_name,
                blob_name=parquet_file,
                config=config,
            )

            if result["status"] == "SKIPPED":
                total_skipped += 1
                print(result["message"])
                continue

            files_processed += 1
            total_loaded += result["rows_loaded"]
            total_rejected += result["rows_rejected"]

            print(result["message"])
            print(f"Chargées : {result['rows_loaded']}")
            print(f"Rejetées : {result['rows_rejected']}")

        finished_at = datetime.now(timezone.utc)

        insert_pipeline_run(
            client=client,
            pipeline_table_id=pipeline_table_id,
            run_id=run_id,
            pipeline_name=pipeline_name,
            status="SUCCESS",
            started_at=started_at,
            finished_at=finished_at,
            files_processed=files_processed,
            files_skipped=total_skipped,
            rows_loaded=total_loaded,
            rows_rejected=total_rejected,
            error_message=None,
            location=location,
        )

        print("Ingestion terminée.")
        print(f"Total lignes chargées : {total_loaded}")
        print(f"Total lignes rejetées : {total_rejected}")
        print(f"Total fichiers traités : {files_processed}")
        print(f"Total fichiers ignorés : {total_skipped}")

    except Exception as error:
        finished_at = datetime.now(timezone.utc)

        if client is not None:
            insert_pipeline_run(
                client=client,
                pipeline_table_id=pipeline_table_id,
                run_id=run_id,
                pipeline_name=pipeline_name,
                status="FAILED",
                started_at=started_at,
                finished_at=finished_at,
                files_processed=files_processed,
                files_skipped=total_skipped,
                rows_loaded=total_loaded,
                rows_rejected=total_rejected,
                error_message=str(error),
                location=location,
            )

        raise


if __name__ == "__main__":
    run_ingestion()