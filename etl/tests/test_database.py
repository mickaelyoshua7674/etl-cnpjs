from etl_small_tables import create_insert_aditional_tables
from sqlalchemy import text
from os import environ
import pytest

def test_connection(my_engine) -> None:
    with my_engine.connect() as conn:
        res = conn.execute(text("SELECT 1;")).fetchone()[0]
        assert res == 1, "Result of query 'SELECT 1;' was different of 1"

def test_database_exists(my_engine) -> None:
    with my_engine.connect() as conn:
        res = conn.execute(text(f"SELECT datname FROM pg_database WHERE datname='{environ["DB_NAME"]}';")).fetchone()[0]
        assert res == environ["DB_NAME"], "Database not found"

def test_create_insert_aditional_tables(my_engine):
    data = (1, "desc")
    pk = "c3"
    with my_engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS id_{pk} CASCADE;"))
        create_insert_aditional_tables(pk=pk, data=(data,), conn=conn)
        conn.commit()
        assert conn.execute(text(f"SELECT * FROM id_{pk};")).fetchall() == [data]