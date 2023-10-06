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

def create_insert_files(file_name:str, table_name:str, pk:str, conn) -> None:
    print(f"Creating and inserting on table {table_name}...")
    df = pd.read_csv(os.path.join("Files",file_name), header=None, encoding="latin-1", sep=";", dtype={0:int,1:str})
    df.columns = (pk, "descricao")
    len_descricao = int(df["descricao"].str.len().max()*1.2) # add a 20% margin
    df.to_sql(name=table_name, con=conn, if_exists="replace", index=False, dtype={pk:Integer(), "descricao":VARCHAR(len_descricao)})
    conn.execute(text(f"ALTER TABLE public.{table_name} ADD PRIMARY KEY ({pk});"))

def creat_insert_aditional_tables(table_name:str, pk:str, data:tuple[tuple], conn) -> None:
    print(f"Creating and inserting on table {table_name}...")
    df = pd.DataFrame(columns=(pk,"descricao"), data=data)
    len_descricao = int(df["descricao"].str.len().max()*1.2) # add a 20% margin
    df.to_sql(name=table_name, con=conn, if_exists="replace", index=False, dtype={pk:Integer(), "descricao":VARCHAR(len_descricao)})
    conn.execute(text(f"ALTER TABLE public.{table_name} ADD PRIMARY KEY ({pk});"))

with engine.connect() as conn:
    create_insert_files(file_name="Cnaes.csv", table_name="id_cnae", pk="cnae", conn=conn) # null -> 8888888
    create_insert_files(file_name="Motivos.csv", table_name="id_motivo_situacao_cadastral", pk="motivo_situacao_cadastral", conn=conn) # null -> 0
    create_insert_files(file_name="Municipios.csv", table_name="id_municipio", pk="municipio", conn=conn) # null -> a inserir
    create_insert_files(file_name="Naturezas.csv", table_name="id_natureza_juridica", pk="natureza_juridica", conn=conn) # null -> 0
    create_insert_files(file_name="Paises.csv", table_name="id_pais", pk="pais", conn=conn) # null -> 999
    create_insert_files(file_name="Qualificacoes.csv", table_name="id_qualificacao", pk="qualificacao", conn=conn) # null -> 0

    creat_insert_aditional_tables(table_name="id_porte_empresa", pk="porte_empresa", data=((0, 'NAO INFORMADO'),
                                                                                           (1, 'MICRO EMPRESA'),
                                                                                           (3, 'EMPRESA DE PEQUENO PORTE'),
                                                                                           (5, 'DEMAIS')), conn=conn)
    creat_insert_aditional_tables(table_name="id_identificador", pk="identificador", data=((0, 'VAZIO'),
                                                                                           (1, 'MATRIZ'),
                                                                                           (2, 'FILIAL')), conn=conn)
    creat_insert_aditional_tables(table_name="id_situacao_cadastral", pk="situacao_cadastral", data=((1, 'NULA'),
                                                                                                     (2, 'ATIVA'),
                                                                                                     (3, 'SUSPENSA'),
                                                                                                     (4, 'INAPTA'),
                                                                                                     (8, 'BAIXADA')), conn=conn)
    creat_insert_aditional_tables(table_name="id_opcao_simples", pk="opcao_simples", data=((1, 'SIM'),
                                                                                           (0, 'NAO'),
                                                                                           (2, 'OUTROS')), conn=conn)
    creat_insert_aditional_tables(table_name="id_opcao_mei", pk="opcao_mei", data=((1, 'SIM'),
                                                                                   (0, 'NAO'),
                                                                                   (2, 'OUTROS')), conn=conn)
    creat_insert_aditional_tables(table_name="id_identificador_socio", pk="identificador_socio", data=((0, 'NENHUM'),
                                                                                                       (1, 'PESSOA JURIDICA'),
                                                                                                       (2, 'PESSOA FISICA'),
                                                                                                       (3, 'ESTRANGEIRO')), conn=conn)
    conn.commit()