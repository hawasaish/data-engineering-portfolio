from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
        dag_id="banking_credit_risk_pipeline",

        start_date=datetime(
            2026,
            1,
            1
        ),

        schedule="@daily",

        catchup=False,

        tags=[
            "banking",
            "credit-risk",
            "spark",
            "api",
            "etl",
        ],
) as dag:

    # --------------------------------------------------
    # API extraction
    # --------------------------------------------------

    extract_macro_api = BashOperator(
        task_id="extract_macro_api",

        bash_command=(
            "python "
            "/opt/airflow/ingestion/"
            "extract_world_bank.py"
        ),
    )

    # --------------------------------------------------
    # Upload API raw data
    # --------------------------------------------------

    upload_macro = BashOperator(
        task_id="upload_macro",

        bash_command=(
            "python "
            "/opt/airflow/ingestion/"
            "upload_to_minio.py"
        ),
    )

    # --------------------------------------------------
    # Transform credit
    # --------------------------------------------------

    transform_credit = BashOperator(
        task_id="transform_credit",

        bash_command=(
            "python "
            "/opt/airflow/spark/"
            "transform_credit.py"
        ),
    )

    # --------------------------------------------------
    # Transform macro
    # --------------------------------------------------

    transform_macro = BashOperator(
        task_id="transform_macro",

        bash_command=(
            "python "
            "/opt/airflow/spark/"
            "transform_macro.py"
        ),
    )

    # --------------------------------------------------
    # Risk features
    # --------------------------------------------------

    create_risk_features = BashOperator(
        task_id="create_risk_features",

        bash_command=(
            "python "
            "/opt/airflow/spark/"
            "create_risk_features.py"
        ),
    )

    # --------------------------------------------------
    # Portfolio summary
    # --------------------------------------------------

    create_portfolio_risk = BashOperator(
        task_id="create_portfolio_risk",

        bash_command=(
            "python "
            "/opt/airflow/spark/"
            "create_portfolio_risk.py"
        ),
    )

    # --------------------------------------------------
    # Credit quality
    # --------------------------------------------------

    validate_credit = BashOperator(
        task_id="validate_credit",

        bash_command=(
            "python "
            "/opt/airflow/quality/"
            "validate_credit.py"
        ),
    )

    # --------------------------------------------------
    # Macro quality
    # --------------------------------------------------

    validate_macro = BashOperator(
        task_id="validate_macro",

        bash_command=(
            "python "
            "/opt/airflow/quality/"
            "validate_macro.py"
        ),
    )

    # --------------------------------------------------
    # PostgreSQL
    # --------------------------------------------------

    load_postgres = BashOperator(
        task_id="load_postgres",

        bash_command=(
            "python "
            "/opt/airflow/spark/"
            "load_postgres.py"
        ),
    )

    # --------------------------------------------------
    # Dependencies
    # --------------------------------------------------

    extract_macro_api >> upload_macro

    upload_macro >> transform_macro

    transform_credit >> create_risk_features

    [
        transform_macro,
        create_risk_features,
    ] >> create_portfolio_risk

    create_portfolio_risk >> [
        validate_credit,
        validate_macro,
    ]

    [
        validate_credit,
        validate_macro,
    ] >> load_postgres