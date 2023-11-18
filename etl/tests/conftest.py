# 'conftest.py' will share the fixtures across all files

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from os import environ
import pytest

environ["DB_DRIVERNAME"] = "postgresql"
environ["DB_USERNAME"] = "postgres"
environ["DB_PASSWORD"] = "0000"
environ["DB_HOST"] = "localhost"
environ["DB_PORT"] = "5432"
environ["DB_NAME"] = "cnpjs"

@pytest.fixture
def my_engine():
    return create_engine(URL.create(drivername=environ["DB_DRIVERNAME"],
                                    username=environ["DB_USERNAME"],
                                    password=environ["DB_PASSWORD"],
                                    host=environ["DB_HOST"],
                                    port=environ["DB_PORT"],
                                    database=environ["DB_NAME"]))

from sys import path
path.insert(0, "./etl")
from tests.TestClass import TestClass

@pytest.fixture
def my_testclass():
    return TestClass()