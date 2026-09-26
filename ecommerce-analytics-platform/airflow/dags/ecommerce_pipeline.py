from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "ecommerce-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with (DAG(
        dag_id="ecommerce_pipeline",
        description="End-to-end E-Commerce ELT pipeline",
        start_date=datetime(2026, 8, 1),
        schedule="@daily",
        catchup=False,
        max_active_runs=1,
        tags=["ecommerce", "elt", "data-engineering"],
        default_args=default_args,
) as dag):

    # --------------------------------------------------
    # START
    # --------------------------------------------------

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Pipeline started"),
    )

    # --------------------------------------------------
    # DAILY DATA GENERATION
    # --------------------------------------------------

    generate_daily_data = BashOperator(
        task_id="generate_daily_data",
        bash_command=(
            "cd /opt/airflow/project && "
            "python ingestion/generate_daily_orders.py"
        ),
    )

    # --------------------------------------------------
    # POSTGRESQL EXTRACTION
    # --------------------------------------------------

    extract_postgres_data = BashOperator(
        task_id="extract_postgres_data",
        bash_command=(
            "cd /opt/airflow/project && "
            "python ingestion/extract.py"
        ),
    )

    # --------------------------------------------------
    # PARQUET CONVERSION
    # --------------------------------------------------

    convert_to_parquet = BashOperator(
        task_id="convert_to_parquet",
        bash_command=(
            "cd /opt/airflow/project && "
            "python ingestion/parquet.py"
        ),
    )

    # --------------------------------------------------
    # MINIO UPLOAD
    # --------------------------------------------------

    upload_to_minio = BashOperator(
        task_id="upload_to_minio",
        bash_command=(
            "cd /opt/airflow/project && "
            "python ingestion/minio_client.py"
        ),
    )

    # --------------------------------------------------
    # MARKETING DATA INGESTION
    # --------------------------------------------------

    upload_marketing_data = BashOperator(
        task_id="upload_marketing_data",
        bash_command=(
            "cd /opt/airflow/project && "
            "python ingestion/upload_marketing.py"
        ),
    )

    # --------------------------------------------------
    # DUCKDB REFRESH
    # --------------------------------------------------

    refresh_duckdb = BashOperator(
        task_id="refresh_duckdb",
        bash_command=(
            "cd /opt/airflow/project && "
            "python ingestion/setup_duckdb.py"
        ),
    )

    # --------------------------------------------------
    # DBT TRANSFORMATIONS
    # --------------------------------------------------

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/airflow/project/dbt/ecommerce_dbt && "
            "dbt build --profiles-dir ."
        ),
    )

    # --------------------------------------------------
    # DATA QUALITY GATE
    # --------------------------------------------------

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/project/dbt/ecommerce_dbt && "
            "dbt test"
        ),
    )

    # --------------------------------------------------
    # AUDIT / OBSERVABILITY
    # --------------------------------------------------

    update_audit_log = BashOperator(
        task_id="update_audit_log",
        bash_command=(
            "cd /opt/airflow/project && "
            "python ingestion/pipeline_audit.py"
        ),
    )

    # --------------------------------------------------
    # END
    # --------------------------------------------------

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Pipeline completed"),
    )

    # ==================================================
    # DAG DEPENDENCIES
    # ==================================================

    start >> generate_daily_data
    start >> upload_marketing_data

    generate_daily_data >> extract_postgres_data
    extract_postgres_data >> convert_to_parquet
    convert_to_parquet >> upload_to_minio

    [upload_to_minio, upload_marketing_data] >> refresh_duckdb

    refresh_duckdb >> dbt_build
    dbt_build >> dbt_test
    dbt_test >> update_audit_log
    update_audit_log >> end