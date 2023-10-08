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

for k in estab.fk:
    fk_values = estab.get_fk_values(k)
    match k:
        case "identificador":
            df[k] = df[k].apply(estab.check_fk, args=(0,fk_values))
        case "situacao_cadastral":
            df[k] = df[k].apply(estab.check_fk, args=(1,fk_values))
        case "motivo_situacao_cadastral":
            df[k] = df[k].apply(estab.check_fk, args=(0,fk_values))
        case "pais":
            df[k] = df[k].apply(estab.check_fk, args=(999,fk_values))
        case "cnae":
            df[k] = df[k].apply(estab.check_fk, args=(8888888,fk_values))
        case "municipio":
            df[k] = df[k].apply(estab.check_fk, args=(9999,fk_values))

df["data_situacao_cadastral"] = df["data_situacao_cadastral"].apply(estab.date_format)
df["data_inicio_atividade"] = df["data_inicio_atividade"].apply(estab.date_format)
df["data_situacao_especial"] = df["data_situacao_especial"].apply(estab.date_format)

with estab.engine.connect() as conn:
    df.to_sql(name=estab.table_name, con=conn, if_exists="replace", index=False, dtype=estab.schema)
    conn.execute(text(estab.get_add_constraints_script()))
    conn.commit()