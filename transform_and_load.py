from sqlalchemy import text, create_engine
from sqlalchemy.engine import URL
import dask.dataframe as dd
import os, json

FILES_DIR = "Files"
FILES_LIST = os.listdir(FILES_DIR)

with open("all_dtypes.json", "r") as f:
    all_dtypes = json.load(f)
with open("file_table.json", "r") as f:
    file_table = json.load(f)

def get_database_url(driver: str, username: str, password: str, host: str, port: str, database: str):
    return URL.create(drivername=driver, username=username, password=password, host=host, port=port, database=database)

# def get_engine_database(url):
#     """Create and return connection engine to DataBase"""
#     return create_engine(url)

def get_table_name(file_name:str) -> str:
    return file_table[file_name.split(".")[0]]

def get_dtypes(file_name:str) -> dict[str]:
    return all_dtypes[get_table_name(file_name)]

def get_columns(file_name:str) -> list[str]:
    return [i for i, _ in get_dtypes(file_name).items()]

with open("./secrets.txt", "r") as f:
    database_url = get_database_url(*f.read().split(","))

file_name = FILES_LIST[0]
cnaes = dd.read_csv(os.path.join(FILES_DIR,file_name), header=None, encoding="latin-1", sep=";", dtype=get_dtypes(file_name))
cnaes.columns = get_columns(file_name)
print(cnaes.dtypes)
print(cnaes.head())

dd.to_sql(df=cnaes, name=get_table_name(file_name), uri=str(database_url), if_exists="replace", index=False, parallel=True)
