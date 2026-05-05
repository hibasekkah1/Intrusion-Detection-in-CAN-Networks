from google.cloud import dataproc_v1


PROJECT_ID = "project-e6de9b55-41d5-4f13-ae0"
REGION = "europe-southwest1"
CLUSTER_NAME = "can-ids-spark-cluster"

BRONZE_SCRIPT_URI = "gs://can-ids-data/spark_jobs/bronze_ingestion_spark.py"
CONFIG_URI = "gs://can-ids-data/config/config.yml"


def get_job_client():
    return dataproc_v1.JobControllerClient(
        client_options={
            "api_endpoint": f"{REGION}-dataproc.googleapis.com:443"
        }
    )


def submit_bronze_job():
    client = get_job_client()

    job = {
        "reference": {
            "project_id": PROJECT_ID,
        },
        "placement": {
            "cluster_name": CLUSTER_NAME,
        },
        "pyspark_job": {
            "main_python_file_uri": BRONZE_SCRIPT_URI,
            "args": [
                "--config_path",
                CONFIG_URI,
            ],
        },
    }

    operation = client.submit_job_as_operation(
        request={
            "project_id": PROJECT_ID,
            "region": REGION,
            "job": job,
        }
    )

    print("Lancement du job Spark Bronze ingestion...")
    response = operation.result()

    print(f"Job terminé : {response.reference.job_id}")
    print(f"Statut : {response.status.state.name}")

    if response.status.state.name != "DONE":
        raise RuntimeError(
            f"Le job Bronze ingestion n'a pas réussi. "
            f"Statut : {response.status.state.name}"
        )


if __name__ == "__main__":
    submit_bronze_job()