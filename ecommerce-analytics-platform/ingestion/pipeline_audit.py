import os
import duckdb
from datetime import datetime, timezone


# --------------------------------------------------
# Configuration
# --------------------------------------------------

DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    "/opt/airflow/project/data/ecommerce.duckdb"
)


# --------------------------------------------------
# Create audit table
# --------------------------------------------------

def create_audit_table():
    """
    Create the pipeline audit schema and table if they
    do not already exist.
    """

    con = duckdb.connect(DUCKDB_PATH)

    try:
        con.execute("""
            CREATE SCHEMA IF NOT EXISTS audit;
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
                audit_id BIGINT PRIMARY KEY,
                pipeline_name VARCHAR NOT NULL,
                task_name VARCHAR NOT NULL,
                run_id VARCHAR,
                status VARCHAR NOT NULL,
                rows_processed BIGINT,
                error_message VARCHAR,
                execution_timestamp TIMESTAMP NOT NULL
            );
        """)

        print("Audit table is ready.")

    finally:
        con.close()


# --------------------------------------------------
# Generate next audit ID
# --------------------------------------------------

def get_next_audit_id(con):
    """
    Generate the next audit ID.
    """

    result = con.execute("""
        SELECT COALESCE(MAX(audit_id), 0) + 1
        FROM audit.pipeline_runs
    """).fetchone()

    return result[0]


# --------------------------------------------------
# Log pipeline execution
# --------------------------------------------------

def log_pipeline_run(
        pipeline_name,
        task_name,
        status,
        run_id=None,
        rows_processed=None,
        error_message=None
):
    """
    Insert a pipeline/task execution record into DuckDB.

    Example:
        log_pipeline_run(
            pipeline_name="ecommerce_pipeline",
            task_name="upload_marketing",
            status="SUCCESS",
            run_id="manual__2026-08-20",
            rows_processed=1000
        )
    """

    con = duckdb.connect(DUCKDB_PATH)

    try:
        audit_id = get_next_audit_id(con)

        execution_timestamp = datetime.now(timezone.utc).replace(
            tzinfo=None
        )

        con.execute("""
            INSERT INTO audit.pipeline_runs (
                audit_id,
                pipeline_name,
                task_name,
                run_id,
                status,
                rows_processed,
                error_message,
                execution_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            audit_id,
            pipeline_name,
            task_name,
            run_id,
            status,
            rows_processed,
            error_message,
            execution_timestamp
        ])

        print(
            f"Audit record created. "
            f"Pipeline={pipeline_name}, "
            f"Task={task_name}, "
            f"Status={status}"
        )

    finally:
        con.close()


# --------------------------------------------------
# Standalone execution
# --------------------------------------------------

if __name__ == "__main__":

    create_audit_table()

    print("Pipeline audit setup completed.")