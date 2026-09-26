import pandas as pd

from ingestion.database import get_engine
from datetime import datetime, timezone


def extract_table(table_name):
    engine = get_engine()

    query = f'SELECT * FROM "{table_name}"'

    print(f"Extracting table: {table_name}")

    df = pd.read_sql(query, engine)

    df["ingested_at"] = datetime.now(timezone.utc)

    print(
        f"Extracted {len(df)} rows "
        f"and {len(df.columns)} columns "
        f"from {table_name}"
    )

    return df