from google.cloud import bigquery
from google.api_core.exceptions import Conflict


PROJECT_ID = "project-e6de9b55-41d5-4f13-ae0"
LOCATION = "europe-southwest1"

DATASETS = [
    "can_ids_bronze",
    "can_ids_silver",
    "can_ids_gold",
    "can_ids_audit",
]


def create_dataset(client, dataset_id):
    full_dataset_id = f"{PROJECT_ID}.{dataset_id}"

    dataset = bigquery.Dataset(full_dataset_id)
    dataset.location = LOCATION

    try:
        client.create_dataset(dataset)
        print(f"Dataset créé : {full_dataset_id}")
    except Conflict:
        print(f"Dataset existe déjà : {full_dataset_id}")


def main():
    client = bigquery.Client(project=PROJECT_ID)

    for dataset_id in DATASETS:
        create_dataset(client, dataset_id)


if __name__ == "__main__":
    main()