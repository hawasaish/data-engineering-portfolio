import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()


def get_engine():

    host = os.getenv("ECOMMERCE_DB_HOST")
    port = os.getenv("ECOMMERCE_DB_PORT")
    database = os.getenv("ECOMMERCE_DB_NAME")
    user = os.getenv("ECOMMERCE_DB_USER")
    password = os.getenv("ECOMMERCE_DB_PASSWORD")

    connection_string = (
        f"postgresql+psycopg2://{user}:{password}"
        f"@{host}:{port}/{database}"
    )

    return create_engine(connection_string)