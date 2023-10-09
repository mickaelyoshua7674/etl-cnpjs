from models.estabelecimento import Estabelecimento
import pandas as pd
import os

estab = Estabelecimento()
estab.create_table()

substitute_value = int()
dtypes = estab.get_dtypes()
file_name = "Estabelecimentos0.csv"
df = pd.read_csv(filepath_or_buffer=os.path.join("Files",file_name),
                 sep=";",
                 header=None,
                 names=estab.get_columns(),
                 dtype=str,
                 encoding="IBM860", # encoding for Portuguese Language
                 nrows=100_000)

for k in estab.fk:
    fk_values = estab.get_fk_values(k)
    match k:
        case "identificador":
            substitute_value = 0
        case "situacao_cadastral":
            substitute_value = 1
        case "motivo_situacao_cadastral":
            substitute_value = 0
        case "pais":
            substitute_value = 999
        case "cnae":
            substitute_value = 8888888
        case "municipio":
            substitute_value = 9999
    df[k] = df[k].fillna(substitute_value)
    df[k] = df[k].astype(dtypes[k])
    df[k] = df[k].apply(estab.check_fk, args=(substitute_value,fk_values))

for data_field in ("data_situacao_cadastral","data_inicio_atividade","data_situacao_especial"):
    df[data_field] = df[data_field].fillna("19000101")
    df[data_field] = df[data_field].apply(estab.date_format)

df = df.astype(dtypes)

estab.insert_data(df)