from google.api_core.exceptions import AlreadyExists
from google.cloud import dataproc_v1


PROJECT_ID = "project-e6de9b55-41d5-4f13-ae0"
REGION = "europe-southwest1"
ZONE = "europe-southwest1-a"
CLUSTER_NAME = "can-ids-spark-cluster"

INIT_ACTION_URI = "gs://can-ids-data/init/install_python_packages.sh"


def get_cluster_client():
    return dataproc_v1.ClusterControllerClient(
        client_options={
            "api_endpoint": f"{REGION}-dataproc.googleapis.com:443"
        }
    )


def create_cluster():
    client = get_cluster_client()

    cluster = {
        "project_id": PROJECT_ID,
        "cluster_name": CLUSTER_NAME,
        "config": {
            "gce_cluster_config": {
                "zone_uri": ZONE,
            },
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n2-standard-2",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 100,
                },
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n2-standard-2",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 100,
                },
            },
            "software_config": {
                "image_version": "2.2-debian12",
                "properties": {
                    "spark:spark.sql.shuffle.partitions": "200",
                    "spark:spark.executor.memory": "4g",
                    "spark:spark.driver.memory": "4g",
                },
            },
            "initialization_actions": [
                {
                    "executable_file": INIT_ACTION_URI,
                    "execution_timeout": {
                        "seconds": 1800
                    },
                }
            ],
        },
    }

    try:
        operation = client.create_cluster(
            request={
                "project_id": PROJECT_ID,
                "region": REGION,
                "cluster": cluster,
            }
        )

        print(f"Création du cluster Dataproc : {CLUSTER_NAME}")
        result = operation.result()
        print(f"Cluster créé avec succès : {result.cluster_name}")

    except AlreadyExists:
        print(f"Cluster déjà existant : {CLUSTER_NAME}")


if __name__ == "__main__":
    create_cluster()