import pandas as pd
import os

from sqlalchemy.types import INTEGER, VARCHAR
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

FILES_FOLDER = os.environ["FILES_FOLDER"]

engine = create_engine(URL.create(drivername=os.environ["DB_DRIVERNAME"],
                                    username=os.environ["DB_USERNAME"],
                                    password=os.environ["DB_PASSWORD"],
                                    host=os.environ["DB_HOST"],
                                    port=os.environ["DB_PORT"],
                                    database=os.environ["DB_NAME"]))

def create_insert_files(path:str, pk:str, conn) -> None:
    """
    Create table and insert data of small files.
    All tables have the following pattern:
                                        * table name -> id_<pk>
                                        * first column -> <pk> (Primary Key)
                                        * second column -> descricao
    """
    print(f"Creating and inserting on table id_{pk}...")
    df = pd.read_csv(path, compression="zip", header=None, encoding="latin-1", sep=";", dtype={0:int,1:str})
    df.columns = (pk, "descricao")
    len_descricao = int(df["descricao"].str.len().max()*1.2) # add a 20% margin
    df.to_sql(name=f"id_{pk}", con=conn, if_exists="replace", index=False, dtype={pk:INTEGER(), "descricao":VARCHAR(len_descricao)})
    conn.execute(text(f"ALTER TABLE id_{pk} ADD PRIMARY KEY ({pk});"))

def create_insert_aditional_tables(pk:str, data:tuple[tuple], conn) -> None:
    """
    Create table and insert data of tables that are not among the downloaded files.
    All tables have the following pattern:
                                        * table name -> id_<pk>
                                        * first column -> <pk> (Primary Key)
                                        * second column -> descricao
    """
    print(f"Creating and inserting on table id_{pk}...")
    df = pd.DataFrame(columns=(pk,"descricao"), data=data)
    len_descricao = int(df["descricao"].str.len().max()*1.2) # add a 20% margin
    df.to_sql(name=f"id_{pk}", con=conn, if_exists="replace", index=False, dtype={pk:INTEGER(), "descricao":VARCHAR(len_descricao)})
    conn.execute(text(f"ALTER TABLE id_{pk} ADD PRIMARY KEY ({pk});"))

with engine.connect() as conn:
    create_insert_files(path=os.path.join(FILES_FOLDER,"Cnaes.zip"), pk="cnae", conn=conn) # null -> 8888888
    create_insert_files(path=os.path.join(FILES_FOLDER,"Motivos.zip"), pk="motivo_situacao_cadastral", conn=conn) # null -> 0
    create_insert_files(path=os.path.join(FILES_FOLDER,"Municipios.zip"), pk="municipio", conn=conn) # null -> inserted next (9999)
    conn.execute(text("INSERT INTO id_municipio VALUES (9999,'NÃO INFORMADO')")) # null -> 9999
    create_insert_files(path=os.path.join(FILES_FOLDER,"Naturezas.zip"), pk="natureza_juridica", conn=conn) # null -> 0
    create_insert_files(path=os.path.join(FILES_FOLDER,"Paises.zip"), pk="pais", conn=conn) # null -> 999
    create_insert_files(path=os.path.join(FILES_FOLDER,"Qualificacoes.zip"), pk="qualificacao", conn=conn) # null -> 0

    create_insert_aditional_tables(pk="porte_empresa", data=((0, "NAO INFORMADO"),
                                                            (1, "MICRO EMPRESA"),
                                                            (3, "EMPRESA DE PEQUENO PORTE"),
                                                            (5, "DEMAIS")), conn=conn)
    create_insert_aditional_tables(pk="identificador", data=((0, "VAZIO"),
                                                            (1, "MATRIZ"),
                                                            (2, "FILIAL")), conn=conn)
    create_insert_aditional_tables(pk="situacao_cadastral", data=((1, "NULA"),
                                                                 (2, "ATIVA"),
                                                                 (3, "SUSPENSA"),
                                                                 (4, "INAPTA"),
                                                                 (8, "BAIXADA")), conn=conn)
    create_insert_aditional_tables(pk="opcao_simples", data=((1, "SIM"),
                                                            (0, "NAO"),
                                                            (2, "OUTROS")), conn=conn)
    create_insert_aditional_tables(pk="opcao_mei", data=((1, "SIM"),
                                                        (0, "NAO"),
                                                        (2, "OUTROS")), conn=conn)
    create_insert_aditional_tables(pk="identificador_socio", data=((0, "NENHUM"),
                                                                  (1, "PESSOA JURIDICA"),
                                                                  (2, "PESSOA FISICA"),
                                                                  (3, "ESTRANGEIRO")), conn=conn)
    conn.commit()