from ingestion.watermark import (
    get_watermark,
    update_watermark,
)

from ingestion.incremental_extract import (
    extract_orders_incrementally,
)

from ingestion.parquet import save_parquet

from ingestion.minio_client import upload_file

from datetime import datetime


PIPELINE_NAME = "orders_incremental"

execution_date = datetime.now().strftime(
    "%Y-%m-%d"
)


def run_incremental_pipeline():

    # 1. Get watermark
    watermark = get_watermark(
        PIPELINE_NAME
    )

    print(
        f"Current watermark: {watermark}"
    )

    # 2. Extract new records
    df = extract_orders_incrementally(
        watermark
    )

    if df.empty:

        print("No new records found")

        return

    # 3. Determine new watermark
    new_watermark = df[
        "order_purchase_timestamp"
    ].max()

    # 4. Save Parquet
    file_path = save_parquet(
        df,
        "orders_incremental"
    )

    # 5. Upload to MinIO
    object_name = (
        f"transactions/orders/"
        f"{execution_date}/"
        f"orders.parquet"
    )

    upload_file(
        file_path,
        object_name
    )

    # 6. Update watermark
    update_watermark(
        PIPELINE_NAME,
        new_watermark
    )

    print(
        f"Pipeline completed. "
        f"New watermark: {new_watermark}"
    )


if __name__ == "__main__":
    run_incremental_pipeline()