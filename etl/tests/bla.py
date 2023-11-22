from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from os import environ

environ["DB_DRIVERNAME"] = "postgresql"
environ["DB_USERNAME"] = "postgres"
environ["DB_PASSWORD"] = "0000"
environ["DB_HOST"] = "localhost"
environ["DB_PORT"] = "5432"
environ["DB_NAME"] = "cnpjs"

engine =  create_engine(URL.create(drivername=environ["DB_DRIVERNAME"],
                                   username=environ["DB_USERNAME"],
                                   password=environ["DB_PASSWORD"],
                                   host=environ["DB_HOST"],
                                   port=environ["DB_PORT"],
                                   database=environ["DB_NAME"]))

with engine.connect() as conn:
    #print(conn.execute(text("SELECT * FROM test;")).fetchall())
    print(conn.execute(text("SELECT * FROM id_c3;")).fetchall())