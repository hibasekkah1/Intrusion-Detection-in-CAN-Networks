from google.api_core.exceptions import NotFound
from google.cloud import dataproc_v1


PROJECT_ID = "project-e6de9b55-41d5-4f13-ae0"
REGION = "europe-southwest1"
CLUSTER_NAME = "can-ids-spark-cluster"


def get_cluster_client():
    return dataproc_v1.ClusterControllerClient(
        client_options={
            "api_endpoint": f"{REGION}-dataproc.googleapis.com:443"
        }
    )


def delete_cluster():
    client = get_cluster_client()

    try:
        operation = client.delete_cluster(
            request={
                "project_id": PROJECT_ID,
                "region": REGION,
                "cluster_name": CLUSTER_NAME,
            }
        )

        print(f"Suppression du cluster : {CLUSTER_NAME}")
        operation.result()
        print("Cluster supprimé avec succès.")

    except NotFound:
        print(f"Cluster introuvable ou déjà supprimé : {CLUSTER_NAME}")


if __name__ == "__main__":
    delete_cluster()