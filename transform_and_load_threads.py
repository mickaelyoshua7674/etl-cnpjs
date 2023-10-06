from sqlalchemy import text, create_engine
from sqlalchemy.engine import URL
from sqlalchemy.types import *
import pandas as pd
import os

def get_engine_database(driver:str, username:str, password:str, host:str, port:str, database:str):
    """Create and return connection engine to DataBase"""
    return create_engine(URL.create(drivername=driver, username=username, password=password, host=host, port=port, database=database))

with open("./secrets.txt", "r") as f:
    engine = get_engine_database(*f.read().split(","))

def create_insert(file_name:str, table_name:str, pk:str) -> None:
    print(f"Creating and inserting on table {table_name}...")
    df = pd.read_csv(os.path.join("Files",file_name), header=None, encoding="latin-1", sep=";", dtype={0:int,1:str})
    df.columns = (pk, "descricao")
    len_descricao = int(df["descricao"].str.len().max()*1.2)
    with engine.connect() as conn:
        df.to_sql(name=table_name, con=conn, if_exists="replace", index=False, dtype={pk:Integer(), "descricao":VARCHAR(len_descricao)})
        conn.execute(text(f"ALTER TABLE public.{table_name} ADD PRIMARY KEY ({pk});"))
        conn.commit()

create_insert(file_name="Cnaes.csv", table_name="id_cnae", pk="cnae")
create_insert(file_name="Motivos.csv", table_name="id_motivo_situacao_cadastral", pk="motivo_situacao_cadastral")
create_insert(file_name="Municipios.csv", table_name="id_municipio", pk="municipio")
create_insert(file_name="Naturezas.csv", table_name="id_natureza_juridica", pk="natureza_juridica")
create_insert(file_name="Paises.csv", table_name="id_pais", pk="pais")
create_insert(file_name="Qualificacoes.csv", table_name="id_qualificacao", pk="qualificacao")
