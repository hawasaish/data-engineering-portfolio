import os
from minio import Minio
from minio.error import S3Error


# Configuration
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw"

FILE_PATH = "marketing/marketing_data.csv"
OBJECT_NAME = "marketing/marketing_data.csv"


def upload_marketing_data():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    try:
        # Check whether the bucket exists
        if not client.bucket_exists(BUCKET_NAME):
            print(f"Bucket '{BUCKET_NAME}' does not exist.")
            print("Please create the bucket in MinIO first.")
            return

        # Check whether the local file exists
        if not os.path.exists(FILE_PATH):
            print(f"File not found: {FILE_PATH}")
            return

        # Upload file
        client.fput_object(
            BUCKET_NAME,
            OBJECT_NAME,
            FILE_PATH,
            content_type="text/csv"
        )

        print("Marketing data uploaded successfully!")
        print(f"Source      : {FILE_PATH}")
        print(f"Destination : s3://{BUCKET_NAME}/{OBJECT_NAME}")

    except S3Error as e:
        print(f"MinIO error: {e}")


if __name__ == "__main__":
    upload_marketing_data()