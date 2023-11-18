from sqlalchemy import text
from os import environ

def test_connection(my_engine) -> None:
    with my_engine.connect() as conn:
        res = conn.execute(text("SELECT 1;")).fetchone()[0]
        assert res == 1, "Result of query 'SELECT 1;' was different of 1"

def test_database_exists(my_engine) -> None:
    with my_engine.connect() as conn:
        res = conn.execute(text(f"SELECT datname FROM pg_database WHERE datname='{environ["DB_NAME"]}';")).fetchone()[0]
        assert res == environ["DB_NAME"], "Database not found"