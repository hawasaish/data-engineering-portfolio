import pandas as pd

from ingestion.database import get_engine


def extract_orders_incrementally(
        last_processed_timestamp
):

    print("Starting incremental extraction")

    engine = get_engine()

    query = """
        SELECT *
        FROM orders_dataset
        WHERE order_purchase_timestamp::timestamp >
        %(last_processed_timestamp)s
        ORDER BY order_purchase_timestamp::timestamp
    """

    df = pd.read_sql(
        query,
        engine,
        params={
            "last_processed_timestamp":
                last_processed_timestamp
        }
    )

    print(f"Extracted {len(df)} incremental orders")

    return df