from typing import List, Union, Iterable, Tuple
from sqlalchemy import text, create_engine
from sqlalchemy.engine import URL
from math import isnan
import pickle as pk
import os

INDEX_PATH = "last_inserted_index.pkl"

def get_files_list(files_dir: str) -> List[str]:
    return sorted(os.listdir(files_dir))

def get_engine_database(driver: str, username: str, password: str, host: str, port: str, database: str):
    """Create and return connection engine to DataBase"""
    return create_engine(URL.create(drivername=driver, username=username, password=password, host=host, port=port, database=database))

def insert_into_table_script(values: List[Union[int, float, str]] | Tuple[Union[int, float, str]], table_name: str) -> str:
    """
    Create SQL script in string format to insert a list of values on a table.
    """
    value = "("
    for v in values:
        if type(v) == str:
            value += f"'{v}', " # if is a string then insert the '' 
        elif isnan(v):
            value += f"Null, " # if is a NaN values, then insert Null
        else:
            value += f"{v}, "
    value = value[:-2] + ");" # ignore last two characters because it is ', '
    return f"INSERT INTO {table_name}\nVALUES {value}"

def get_last_inserted_index() -> int:
    """If exists, read the .pkl file with the last inserted index"""
    if os.path.exists(INDEX_PATH):
        with open(INDEX_PATH, "rb") as f:
            i = pk.load(f)
            print(f"Last index {i}")
            return i
    return -1

def save_last_inserted_index(i: int) -> None:
    """Save the last inserted index"""
    with open(INDEX_PATH, "wb") as f:
        pk.dump(i, f)

def remove_last_inserted_index() -> None:
    """Delete saved index"""
    os.remove(INDEX_PATH)

def insert_into_table(engine, df: Iterable, table_name: str) -> None:
    """Insert values from DataFrame into table"""
    last_inserted_index = get_last_inserted_index()
    
    with engine.connect() as connection:
        print(f"Inserting into public.{table_name}...")
        for chunk in df:
            for row in chunk.itertuples():
                i = row[0]
                if i > last_inserted_index:
                    insert_into = insert_into_table_script(row[1:], table_name) # get script for insert into
                    connection.execute(text(insert_into))
                    connection.commit()
                    save_last_inserted_index(i)
    remove_last_inserted_index()