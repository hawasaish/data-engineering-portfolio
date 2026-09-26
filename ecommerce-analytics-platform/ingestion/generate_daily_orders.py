import random
from faker import Faker
from datetime import datetime, timedelta
import uuid
from sqlalchemy import text
import pandas as pd

from ingestion.database import get_engine

fake = Faker()

def get_existing_customers():

    engine = get_engine()

    query = """
        SELECT customer_id
        FROM customers_dataset
        LIMIT 10000
    """

    return pd.read_sql(query, engine)


def generate_orders(number_of_orders=100):

    customers = get_existing_customers()

    orders = []

    for _ in range(number_of_orders):

        purchase_time = datetime.now()

        customer_id = random.choice(
            customers["customer_id"].tolist()
        )

        orders.append({
            "order_id": str(uuid.uuid4()),
            "customer_id": customer_id,
            "order_status": "delivered",
            "order_purchase_timestamp": purchase_time,
            "order_approved_at": purchase_time + timedelta(minutes=10),
            "order_delivered_carrier_date": purchase_time + timedelta(days=1),
            "order_delivered_customer_date": purchase_time + timedelta(days=5),
            "order_estimated_delivery_date": purchase_time + timedelta(days=7),
        })

    return pd.DataFrame(orders)


def insert_orders(df):

    engine = get_engine()

    df.to_sql(
        "orders_dataset",
        engine,
        if_exists="append",
        index=False
    )

    print(f"Inserted {len(df)} synthetic orders")


if __name__ == "__main__":

    df = generate_orders(100)

    print(df.head())

    insert_orders(df)