from sqlalchemy import text, create_engine
from sqlalchemy.engine import URL
from sqlalchemy.types import *
import pandas as pd
import os, json

FILES_DIR = "Files"
FILES_LIST = os.listdir(FILES_DIR)

with open("all_dtypes.json", "r") as f:
    all_dtypes = json.load(f)
with open("file_table.json", "r") as f:
    file_table = json.load(f)

def get_engine_database(driver:str, username:str, password:str, host:str, port:str, database:str):
    """Create and return connection engine to DataBase"""
    return create_engine(URL.create(drivername=driver, username=username, password=password, host=host, port=port, database=database))

def get_table_name(file_name:str) -> str:
    return file_table[file_name.split(".")[0]]

def get_dtypes(file_name:str) -> dict[str]:
    return all_dtypes[get_table_name(file_name)]

def get_columns(file_name:str) -> list[str]:
    return [i for i, _ in get_dtypes(file_name).items()]

with open("./secrets.txt", "r") as f:
    engine = get_engine_database(*f.read().split(","))

file_name = FILES_LIST[0]
cnaes = pd.read_csv(os.path.join(FILES_DIR,file_name), header=None, encoding="latin-1", sep=";", dtype=get_dtypes(file_name))
cnaes.columns = get_columns(file_name)

with engine.connect() as conn:
    cnaes.to_sql(name=get_table_name(file_name), con=conn, if_exists="replace", index=False, dtype={"cnae_fiscal_principal":Integer(),
                                                                                                    "descricao":VARCHAR(int(cnaes["descricao"].str.len().max()*1.2))})
    conn.execute(text(f"ALTER TABLE public.id_cnae_fiscal_principal ADD PRIMARY KEY (cnae_fiscal_principal);"))
    conn.commit()
