import duckdb

connection = duckdb.connect(
    "data/ecommerce.duckdb"
)

connection.execute(
    "SELECT 1"
)

print("DuckDB database created successfully")

connection.close()