import os
import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ecommerce"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

DATA_FOLDER = "data"

for file in os.listdir(DATA_FOLDER):
    if file.endswith(".csv"):
        table_name = file.replace(".csv", "").replace("olist_", "")
        file_path = os.path.join(DATA_FOLDER, file)

        print(f"Loading {file}...")

        df = pd.read_csv(file_path)

        df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False
        )

        print(f"Loaded {len(df)} rows into {table_name}")

print("All datasets loaded successfully!")