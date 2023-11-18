from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from os import environ
from sys import path

environ["DB_DRIVERNAME"] = "postgresql"
environ["DB_USERNAME"] = "postgres"
environ["DB_PASSWORD"] = "0000"
environ["DB_HOST"] = "localhost"
environ["DB_PORT"] = "5432"
environ["DB_NAME"] = "cnpjs"

path.insert(0, "./etl")

engine = create_engine(URL.create(drivername=environ["DB_DRIVERNAME"],
                                  username=environ["DB_USERNAME"],
                                  password=environ["DB_PASSWORD"],
                                  host=environ["DB_HOST"],
                                  port=environ["DB_PORT"],
                                  database=environ["DB_NAME"]))
from TestClass import TestClass
obj = TestClass()

def test_connection() -> None:
    with engine.connect() as conn:
        res = conn.execute(text("SELECT 1;")).fetchone()[0]
        assert res == 1

def test_database_exists() -> None:
    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT datname FROM pg_database WHERE datname='{environ["DB_NAME"]}';")).fetchone()[0]
        assert res == environ["DB_NAME"]