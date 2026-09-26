from ingestion.extract import extract_table
from ingestion.parquet import save_parquet
from ingestion.minio_client import upload_file
from ingestion.config import MINIO_BUCKET


TABLES = [
    "customers_dataset",
    "orders_dataset",
    "order_items_dataset",
    "order_payments_dataset",
    "products_dataset",
    "sellers_dataset",
]


def run_pipeline():

    for table_name in TABLES:

        print(f"Starting ingestion for {table_name}")

        # Extract
        df = extract_table(table_name)

        # Convert to Parquet
        file_path = save_parquet(
            df,
            table_name
        )

        # Upload to MinIO
        object_name = (
            f"transactions/{table_name}.parquet"
        )

        upload_file(
            file_path,
            object_name
        )

        print(
            f"Completed ingestion for {table_name}"
        )


def upload_marketing_data():

    file_path = "marketing/marketing_data.csv"

    upload_file(
        file_path,
        "marketing/marketing_data.csv"
    )

    print("Marketing data uploaded")


if __name__ == "__main__":
    run_pipeline()
    upload_marketing_data()