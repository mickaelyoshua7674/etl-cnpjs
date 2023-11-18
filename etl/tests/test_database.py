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
def test_create_fk_table(my_engine):
    with my_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS id_c3;CREATE TABLE id_c3 (c3 INTEGER PRIMARY KEY, descricao VARCHAR(4));INSERT INTO id_c3 VALUES (1,'desc');"))
        conn.commit()
        assert conn.execute(text("SELECT * FROM id_c3;")).fetchall() == [(1, "desc")]