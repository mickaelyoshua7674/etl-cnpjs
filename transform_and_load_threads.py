from models.estabelecimento import Estabelecimento
import pandas as pd
import os

estab = Estabelecimento()
columns_to_constraint = ("identificador","situacao_cadastral","motivo_situacao_cadastral","pais","cnae","municipio")
file_name = "Estabelecimentos0.csv"
df = pd.read_csv(os.path.join("Files",file_name), header=None, encoding="latin-1", sep=";", dtype=str, nrows=100_000)
df.columns = estab.get_columns()

df["identificador"] = df["identificador"].fillna(0)
df["situacao_cadastral"] = df["situacao_cadastral"].fillna(1)
df["motivo_situacao_cadastral"] = df["motivo_situacao_cadastral"].fillna(0)
df["pais"] = df["pais"].fillna(999)
df["cnae"] = df["cnae"].fillna(8888888)
df["municipio"] = df["municipio"].fillna(9999)

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

print(df.head())
