import boto3

from ingestion.config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
)


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url="http://" + MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def upload_file(local_file, object_name):
    client = get_minio_client()

    client.upload_file(
        str(local_file),
        MINIO_BUCKET,
        object_name,
    )

    print(
        f"Uploaded {local_file} "
        f"to {MINIO_BUCKET}/{object_name}"
    )