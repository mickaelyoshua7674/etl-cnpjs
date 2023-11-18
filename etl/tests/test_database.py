from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from os import environ

environ["DB_DRIVERNAME"] = "postgresql"
environ["DB_USERNAME"] = "postgres"
environ["DB_PASSWORD"] = "0000"
environ["DB_HOST"] = "localhost"
environ["DB_PORT"] = "5432"
environ["DB_NAME"] = "cnpjs"

engine = create_engine(URL.create(drivername=environ["DB_DRIVERNAME"],
                                  username=environ["DB_USERNAME"],
                                  password=environ["DB_PASSWORD"],
                                  host=environ["DB_HOST"],
                                  port=environ["DB_PORT"],
                                  database=environ["DB_NAME"]))

def test_connection() -> None:
    """
    Test if connection was successful
    """
    with engine.connect() as conn:
        res = conn.execute(text("SELECT 1;")).fetchone()[0]
        assert res == 1, "Result of query 'SELECT 1;' was different of 1"

def test_database_exists() -> None:
    """
    Test if Database was created
    """
    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT datname FROM pg_database WHERE datname='{environ["DB_NAME"]}';")).fetchone()[0]
        assert res == environ["DB_NAME"], "Database not found"

from sys import path
path.insert(0, "./etl")
from TestClass import TestClass
obj = TestClass()

def test_get_columns() -> None:
    assert obj.get_columns() == ("column1","column2","column3","column4"), "Columns defined in TestClass do not match"

def test_get_dtypes() -> None:
    assert obj.get_dtypes() == {"column1":str, "column2":float, "column3":int, "column4":str}, "Datatypes defined in TestClass do not match"

def test_date_format() -> None:
    assert obj.date_format("20210303") == "2021-03-03", "Conversion to date formate is not correct"
    assert obj.date_format("00000000") == "1900-01-01", "8 0s is supose to return '1900-01-01'"
    assert obj.date_format("202103030") == "1900-01-01", "more then 8 digits is supose to return '1900-01-01'"
    assert obj.date_format("2021030") == "1900-01-01", "less then 8 digits is supose to return '1900-01-01'"

def test_get_insert_script() -> None:
    assert obj.get_insert_script().text == "INSERT INTO test VALUES (:column1,:column2,:column3,:column4);", "text of insert values do not match"

