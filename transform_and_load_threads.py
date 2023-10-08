from models.estabelecimento import Estabelecimento
from sqlalchemy import text
import pandas as pd
import os

estab = Estabelecimento()
file_name = "Estabelecimentos0.csv"
df = pd.read_csv(filepath_or_buffer=os.path.join("Files",file_name),
                 sep=";",
                 header=None,
                 names=estab.get_columns(),
                 dtype=str,
                 encoding="IBM860", # encoding for Portuguese Language
                 nrows=100_000)

df["identificador"] = df["identificador"].fillna(0)
df["situacao_cadastral"] = df["situacao_cadastral"].fillna(1)
df["motivo_situacao_cadastral"] = df["motivo_situacao_cadastral"].fillna(0)
df["pais"] = df["pais"].fillna(999)
df["cnae"] = df["cnae"].fillna(8888888)
df["municipio"] = df["municipio"].fillna(9999)

df["data_situacao_cadastral"] = df["data_situacao_cadastral"].fillna("19000101")
df["data_inicio_atividade"] = df["data_inicio_atividade"].fillna("19000101")
df["data_situacao_especial"] = df["data_situacao_especial"].fillna("19000101")

df = df.astype(estab.get_dtypes())

constraint_identificador = estab.get_constraint_values("identificador")
constraint_situacao_cadastral = estab.get_constraint_values("situacao_cadastral")
constraint_motivo_situacao_cadastral = estab.get_constraint_values("motivo_situacao_cadastral")
constraint_pais = estab.get_constraint_values("pais")
constraint_cnae = estab.get_constraint_values("cnae")
constraint_municipio = estab.get_constraint_values("municipio")

df["identificador"] = df["identificador"].apply(lambda x: 0 if x not in constraint_identificador else x)
df["situacao_cadastral"] = df["situacao_cadastral"].apply(lambda x: 1 if x not in constraint_situacao_cadastral else x)
df["motivo_situacao_cadastral"] = df["motivo_situacao_cadastral"].apply(lambda x: 0 if x not in constraint_motivo_situacao_cadastral else x)
df["pais"] = df["pais"].apply(lambda x: 999 if x not in constraint_pais else x)
df["cnae"] = df["cnae"].apply(lambda x: 8888888 if x not in constraint_cnae else x)
df["municipio"] = df["municipio"].apply(lambda x: 9999 if x not in constraint_municipio else x)

df["data_situacao_cadastral"] = df["data_situacao_cadastral"].apply(estab.date_format)
df["data_inicio_atividade"] = df["data_inicio_atividade"].apply(estab.date_format)
df["data_situacao_especial"] = df["data_situacao_especial"].apply(estab.date_format)

with estab.engine.connect() as conn:
    df.to_sql(name=estab.table_name, con=conn, if_exists="replace", index=False, dtype=estab.schema)
    conn.execute(text(estab.get_add_constraints_script()))
    conn.commit()