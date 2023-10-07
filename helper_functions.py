from sqlalchemy import create_engine
from sqlalchemy.engine import URL

def get_engine_database():
    """Get secrets from '.secrets.txt' file and create and return connection engine to DataBase"""
    with open("./secrets.txt", "r") as f:
        driver, username, password, host, port, database = f.read().split(",")
        return create_engine(URL.create(drivername=driver, username=username, password=password, host=host, port=port, database=database))