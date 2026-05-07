from google.cloud import dataproc_v1


PROJECT_ID = "project-e6de9b55-41d5-4f13-ae0"
REGION = "europe-southwest1"
CLUSTER_NAME = "can-ids-spark-cluster"

SCRIPT_URI = "gs://can-ids-data/spark_jobs/silver_iat_spark.py"
CONFIG_URI = "gs://can-ids-data/config/config.yml"

STEP_LABEL = "silver-iat-spark"


def get_job_client():
    return dataproc_v1.JobControllerClient(
        client_options={
            "api_endpoint": f"{REGION}-dataproc.googleapis.com:443"
        }
    )


def submit_job():
    client = get_job_client()

    job = {
        "reference": {
            "project_id": PROJECT_ID,
        },
        "placement": {
            "cluster_name": CLUSTER_NAME,
        },
        "labels": {
            "pipeline": "can-ids",
            "step": STEP_LABEL,
        },
        "pyspark_job": {
            "main_python_file_uri": SCRIPT_URI,
            "args": [
                "--config_path",
                CONFIG_URI,
            ],
        },
    }

    submitted_job = client.submit_job(
        request={
            "project_id": PROJECT_ID,
            "region": REGION,
            "job": job,
        }
    )

    job_id = submitted_job.reference.job_id
    state = submitted_job.status.state.name

    print("Job Silver IAT soumis.")
    print(f"Job ID : {job_id}")
    print(f"Statut initial : {state}")
    print("")
    print("Pour suivre le job :")
    print(
        f"gcloud dataproc jobs wait {job_id} "
        f"--region={REGION} "
        f"--project={PROJECT_ID}"
    )


if __name__ == "__main__":
    submit_job()