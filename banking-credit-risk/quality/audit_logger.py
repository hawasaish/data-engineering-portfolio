import os
from datetime import datetime

import psycopg2
from dotenv import load_dotenv


load_dotenv()


def get_connection():

    return psycopg2.connect(
        host=os.getenv(
            "POSTGRES_HOST",
            "localhost"
        ),

        port=os.getenv(
            "POSTGRES_PORT",
            "5432"
        ),

        database=os.getenv(
            "POSTGRES_DB",
            "banking_risk"
        ),

        user=os.getenv(
            "POSTGRES_USER",
            "banking"
        ),

        password=os.getenv(
            "POSTGRES_PASSWORD",
            "banking"
        ),
    )


def log_pipeline_event(
        run_id,
        pipeline_name,
        task_name,
        start_time,
        end_time,
        status,
        records_processed=None,
        error_message=None,
):

    connection = get_connection()

    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO risk.pipeline_audit
        (
            run_id,
            pipeline_name,
            task_name,
            start_time,
            end_time,
            status,
            records_processed,
            error_message
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        """,
        (
            run_id,
            pipeline_name,
            task_name,
            start_time,
            end_time,
            status,
            records_processed,
            error_message,
        )
    )

    connection.commit()

    cursor.close()
    connection.close()