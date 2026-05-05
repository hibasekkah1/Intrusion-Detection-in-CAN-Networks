from google.cloud import storage


def list_parquet_files(bucket_name, prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)

    parquet_files = [
        blob.name
        for blob in blobs
        if blob.name.endswith(".parquet")
    ]

    return parquet_files