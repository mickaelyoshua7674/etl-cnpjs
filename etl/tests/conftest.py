# 'conftest.py' will share the fixtures across all files
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from os import environ
import pytest

@pytest.fixture(scope="session")
def my_engine():
    return create_engine(URL.create(drivername=environ["DB_DRIVERNAME"],
                                    username=environ["DB_USERNAME"],
                                    password=environ["DB_PASSWORD"],
                                    host=environ["DB_HOST"],
                                    port=environ["DB_PORT"],
                                    database=environ["DB_NAME"]))