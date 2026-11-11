from sqlalchemy import text

from ingestion.database import get_engine


def get_watermark(pipeline_name):

    engine = get_engine()

    query = text("""
        SELECT last_processed_timestamp
        FROM ingestion_watermark
        WHERE pipeline_name = :pipeline_name
    """)

    with engine.connect() as connection:

        result = connection.execute(
            query,
            {
                "pipeline_name": pipeline_name
            }
        )

        row = result.fetchone()

    if row:
        return row[0]

    return None


def update_watermark(
        pipeline_name,
        timestamp
):

    engine = get_engine()

    query = text("""
        UPDATE ingestion_watermark
        SET last_processed_timestamp = :timestamp
        WHERE pipeline_name = :pipeline_name
    """)

    with engine.begin() as connection:

        connection.execute(
            query,
            {
                "pipeline_name": pipeline_name,
                "timestamp": timestamp,
            }
        )