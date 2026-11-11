import duckdb

con = duckdb.connect(
    "data/ecommerce.duckdb"
)

with open("duckdb/setup.sql") as file:
    sql = file.read()

con.execute(sql)

con.close()