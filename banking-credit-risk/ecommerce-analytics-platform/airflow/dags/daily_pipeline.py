from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def generate_orders():

    from ingestion.generate_daily_orders import (
        generate_orders as generate
    )

    from ingestion.generate_daily_orders import (
        insert_orders
    )

    df = generate(100)

    insert_orders(df)


def incremental_ingestion():

    from ingestion.incremental_pipeline import (
        run_incremental_pipeline
    )

    run_incremental_pipeline()

default_args = {
    "owner": "Saish",
    "retries": 2,
}


with DAG(
        dag_id="ecommerce_daily_incremental",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        default_args=default_args,
) as dag:

    generate_task = PythonOperator(
        task_id="generate_daily_orders",
        python_callable=generate_orders,
    )

    ingestion_task = PythonOperator(
        task_id="incremental_ingestion",
        python_callable=incremental_ingestion,
    )

