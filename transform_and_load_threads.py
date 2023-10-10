from models.estabelecimento import Estabelecimento
from models.MyThread import MyThread
from sqlalchemy import text
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
    df[k].fillna(substitute_value, inplace=True)
    df[k] = df[k].astype(dtypes[k])
    df[k] = df[k].apply(estab.check_fk, args=(substitute_value,fk_values))

for date_field in ("data_situacao_cadastral","data_inicio_atividade","data_situacao_especial"):
    df[date_field].fillna("19000101", inplace=True)
    df[date_field] = df[date_field].apply(estab.date_format)

df = df.astype(dtypes)
print(df.head(2))

thread1 = MyThread(estab, df)
thread2 = MyThread(estab, df.iloc[1:,:])

thread1.start()
thread2.start()

thread1.join()
thread2.join()







print("Done")



