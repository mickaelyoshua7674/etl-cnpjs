from sqlalchemy.types import INTEGER, VARCHAR
from models.BaseModel import BaseModel
from sqlalchemy import text
import pandas as pd

URL = "https://dadosabertos.rfb.gov.br/CNPJ/"

def create_insert_files(url:str, pk:str, conn) -> None:
    """
    Create table and insert data of small files.
    All tables have the following pattern:
                                        * table name -> id_<pk>
                                        * first column -> <pk> (Primary Key)
                                        * second column -> descricao
    """
    print(f"Creating and inserting on table id_{pk}...")
    df = pd.read_csv(url, header=None, encoding="latin-1", sep=";", dtype={0:int,1:str})
    df.columns = (pk, "descricao")
    len_descricao = int(df["descricao"].str.len().max()*1.2) # add a 20% margin
    df.to_sql(name=f"id_{pk}", con=conn, if_exists="replace", index=False, dtype={pk:INTEGER(), "descricao":VARCHAR(len_descricao)})
    conn.execute(text(f"ALTER TABLE id_{pk} ADD PRIMARY KEY ({pk});"))

def creat_insert_aditional_tables(pk:str, data:tuple[tuple], conn) -> None:
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

engine = BaseModel().engine
with engine.connect() as conn:
    create_insert_files(url=URL+"Cnaes.zip", pk="cnae", conn=conn) # null -> 8888888
    create_insert_files(url=URL+"Motivos.zip", pk="motivo_situacao_cadastral", conn=conn) # null -> 0
    create_insert_files(url=URL+"Municipios.zip", pk="municipio", conn=conn) # null -> inserted next (9999)
    conn.execute(text("INSERT INTO id_municipio VALUES (9999,'NÃO INFORMADO')")) # null -> 9999
    create_insert_files(url=URL+"Naturezas.zip", pk="natureza_juridica", conn=conn) # null -> 0
    create_insert_files(url=URL+"Paises.zip", pk="pais", conn=conn) # null -> 999
    create_insert_files(url=URL+"Qualificacoes.zip", pk="qualificacao", conn=conn) # null -> 0

    creat_insert_aditional_tables(pk="porte_empresa", data=((0, 'NAO INFORMADO'),
                                                            (1, 'MICRO EMPRESA'),
                                                            (3, 'EMPRESA DE PEQUENO PORTE'),
                                                            (5, 'DEMAIS')), conn=conn)
    creat_insert_aditional_tables(pk="identificador", data=((0, 'VAZIO'),
                                                            (1, 'MATRIZ'),
                                                            (2, 'FILIAL')), conn=conn)
    creat_insert_aditional_tables(pk="situacao_cadastral", data=((1, 'NULA'),
                                                                 (2, 'ATIVA'),
                                                                 (3, 'SUSPENSA'),
                                                                 (4, 'INAPTA'),
                                                                 (8, 'BAIXADA')), conn=conn)
    creat_insert_aditional_tables(pk="opcao_simples", data=((1, 'SIM'),
                                                            (0, 'NAO'),
                                                            (2, 'OUTROS')), conn=conn)
    creat_insert_aditional_tables(pk="opcao_mei", data=((1, 'SIM'),
                                                        (0, 'NAO'),
                                                        (2, 'OUTROS')), conn=conn)
    creat_insert_aditional_tables(pk="identificador_socio", data=((0, 'NENHUM'),
                                                                  (1, 'PESSOA JURIDICA'),
                                                                  (2, 'PESSOA FISICA'),
                                                                  (3, 'ESTRANGEIRO')), conn=conn)
    conn.commit()