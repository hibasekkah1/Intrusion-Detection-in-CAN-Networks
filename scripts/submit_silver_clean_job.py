from google.cloud import dataproc_v1


PROJECT_ID = "project-e6de9b55-41d5-4f13-ae0"
REGION = "europe-southwest1"
CLUSTER_NAME = "can-ids-spark-cluster"

SILVER_SCRIPT_URI = "gs://can-ids-data/spark_jobs/silver_clean_spark.py"
CONFIG_URI = "gs://can-ids-data/config/config.yml"

STEP_LABEL = "silver-clean-spark"


def get_job_client():
    return dataproc_v1.JobControllerClient(
        client_options={
            "api_endpoint": f"{REGION}-dataproc.googleapis.com:443"
        }
    )


def get_running_jobs_for_step(client):
    request = dataproc_v1.ListJobsRequest(
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    running_jobs = []

    for job in client.list_jobs(request=request):
        labels = dict(job.labels)
        state = job.status.state.name

        if labels.get("step") == STEP_LABEL and state in {
            "PENDING",
            "SETUP_DONE",
            "RUNNING",
            "CANCEL_PENDING",
            "CANCEL_STARTED",
        }:
            running_jobs.append(job)

    return running_jobs


def submit_silver_clean_job():
    client = get_job_client()

    running_jobs = get_running_jobs_for_step(client)

    if running_jobs:
        print("Un job Silver Clean est déjà en cours. Aucun nouveau job n'a été soumis.")
        print("")

        for job in running_jobs:
            job_id = job.reference.job_id
            state = job.status.state.name

            print(f"Job ID existant : {job_id}")
            print(f"Statut : {state}")
            print("")
            print("Pour suivre ce job :")
            print(
                f"gcloud dataproc jobs wait {job_id} "
                f"--region={REGION} "
                f"--project={PROJECT_ID}"
            )
            print("")

        return

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
            "main_python_file_uri": SILVER_SCRIPT_URI,
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

    print("Job Silver Clean soumis avec succès.")
    print(f"Job ID : {job_id}")
    print(f"Statut initial : {state}")
    print("")
    print("Pour suivre le job :")
    print(
        f"gcloud dataproc jobs wait {job_id} "
        f"--region={REGION} "
        f"--project={PROJECT_ID}"
    )
    print("")
    print("Pour voir le détail :")
    print(
        f"gcloud dataproc jobs describe {job_id} "
        f"--region={REGION} "
        f"--project={PROJECT_ID}"
    )


if __name__ == "__main__":
    submit_silver_clean_job()