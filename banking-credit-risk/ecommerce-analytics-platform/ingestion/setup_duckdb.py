import duckdb
import os


# --------------------------------------------------
# Configuration
# --------------------------------------------------

DUCKDB_PATH = "data/ecommerce.duckdb"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


# --------------------------------------------------
# Main
# --------------------------------------------------

def setup_duckdb():

    print("Opening DuckDB...")

    con = duckdb.connect(DUCKDB_PATH)

    try:
        # --------------------------------------------------
        # 1. Load httpfs extension
        # --------------------------------------------------

        print("Loading httpfs extension...")

        con.execute("""
            INSTALL httpfs;
        """)

        con.execute("""
            LOAD httpfs;
        """)

        print("httpfs loaded successfully.")

        # --------------------------------------------------
        # 2. Configure MinIO
        # --------------------------------------------------

        print("Configuring MinIO...")

        con.execute(f"""
            CREATE OR REPLACE SECRET minio_secret (
                TYPE S3,
                KEY_ID '{MINIO_ACCESS_KEY}',
                SECRET '{MINIO_SECRET_KEY}',
                ENDPOINT '{MINIO_ENDPOINT}',
                USE_SSL false,
                URL_STYLE 'path'
            );
        """)

        print("MinIO configured successfully.")

        # --------------------------------------------------
        # 3. Create raw schema
        # --------------------------------------------------

        con.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
        """)

        print("Raw schema created.")

        # --------------------------------------------------
        # 4. Create raw views
        # --------------------------------------------------

        print("Creating raw views...")

        con.execute("""
            CREATE OR REPLACE VIEW raw.orders AS SELECT * FROM read_parquet('s3://raw/transactions/orders_dataset.parquet');
        """)

        con.execute("""
            CREATE OR REPLACE VIEW raw.customers AS
            SELECT *
            FROM read_parquet(
                's3://raw/transactions/customers_dataset.parquet'
            );
        """)

        con.execute("""
            CREATE OR REPLACE VIEW raw.products AS
            SELECT *
            FROM read_parquet(
                's3://raw/transactions/products_dataset.parquet'
            );
        """)

        con.execute("""
            CREATE OR REPLACE VIEW raw.order_items AS
            SELECT *
            FROM read_parquet(
                's3://raw/transactions/order_items_dataset.parquet'
            );
        """)

        con.execute("""
            CREATE OR REPLACE VIEW raw.order_payments AS
            SELECT *
            FROM read_parquet(
                's3://raw/transactions/order_payments_dataset.parquet'
            );
        """)

        con.execute("""
            CREATE OR REPLACE VIEW raw.sellers AS 
            SELECT *
            FROM read_parquet(
                's3://raw/transactions/sellers_dataset.parquet'
            );
        """)

        con.execute("""
            CREATE OR REPLACE VIEW raw.marketing AS
            SELECT *
            FROM read_csv_auto(
                's3://raw/marketing/marketing_data.csv'
            );
        """)

        print("Raw views created successfully.")

        # --------------------------------------------------
        # 5. Verify
        # --------------------------------------------------

        print("\nRaw tables/views:")

        result = con.execute("""
            SELECT
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema = 'raw'
            ORDER BY table_name;
        """).fetchall()

        for row in result:
            print(row)

        print("\nDuckDB setup completed successfully.")

    finally:
        con.close()


if __name__ == "__main__":
    setup_duckdb()