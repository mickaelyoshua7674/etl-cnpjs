from etl_small_tables import creat_insert_aditional_tables
from sqlalchemy import text
from os import environ
import pytest

@pytest.mark.dependency()
def test_connection(my_engine) -> None:
    with my_engine.connect() as conn:
        res = conn.execute(text("SELECT 1;")).fetchone()[0]
        assert res == 1, "Result of query 'SELECT 1;' was different of 1"

@pytest.mark.dependency(depends=["test_connection"])
def test_database_exists(my_engine) -> None:
    with my_engine.connect() as conn:
        res = conn.execute(text(f"SELECT datname FROM pg_database WHERE datname='{environ["DB_NAME"]}';")).fetchone()[0]
        assert res == environ["DB_NAME"], "Database not found"

@pytest.mark.dependency(depends=["test_database_exists"])
def test_creat_insert_aditional_tables(my_engine):
    data = (1, "desc")
    pk = "c3"
    with my_engine.connect() as conn:
        creat_insert_aditional_tables(pk=pk, data=(data,), conn=conn)
        assert conn.execute(text(f"SELECT * FROM id_{pk};")).fetchall() == [data]