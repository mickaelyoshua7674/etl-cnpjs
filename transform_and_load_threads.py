from sqlalchemy.types import Integer, VARCHAR
from sqlalchemy import text, create_engine
from sqlalchemy.engine import URL
import pandas as pd
import os

def get_engine_database(driver:str, username:str, password:str, host:str, port:str, database:str):
    """Create and return connection engine to DataBase"""
    return create_engine(URL.create(drivername=driver, username=username, password=password, host=host, port=port, database=database))

with open("./secrets.txt", "r") as f:
    engine = get_engine_database(*f.read().split(","))


