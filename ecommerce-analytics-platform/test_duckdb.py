import duckdb

con = duckdb.connect(
    "duckdb/ecommerce.duckdb"
)

con.execute(
    "INSTALL httpfs"
)

con.execute(
    "LOAD httpfs"
)