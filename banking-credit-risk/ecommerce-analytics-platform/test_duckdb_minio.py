import duckdb

con = duckdb.connect()

con.execute("""
SELECT *
FROM glob(
    's3://raw/transactions/*.parquet'
);
""")

print(con.sql("FROM duckdb_secrets()"))