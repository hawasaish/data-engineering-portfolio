import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


load_dotenv()


MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://localhost:9000"
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "minioadmin"
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "minioadmin123"
)

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "banking-risk"
)


def get_s3_client():

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def upload_directory(
        directory: str,
        prefix: str
):

    s3 = get_s3_client()

    directory_path = Path(directory)

    if not directory_path.exists():
        raise FileNotFoundError(
            directory
        )

    for file_path in directory_path.rglob("*"):

        if not file_path.is_file():
            continue

        relative_path = (
            file_path.relative_to(directory_path)
        )

        object_key = (
            f"{prefix}/{relative_path}"
        )

        print(
            f"Uploading {file_path} "
            f"-> s3://{MINIO_BUCKET}/{object_key}"
        )

        s3.upload_file(
            str(file_path),
            MINIO_BUCKET,
            object_key
        )


if __name__ == "__main__":

    upload_directory(
        "data/raw/macro_api",
        "bronze/macro_api"
    )