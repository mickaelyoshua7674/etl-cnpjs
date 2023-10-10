from models.estabelecimento import Estabelecimento
from models.MyThread import MyThread
from queue import Queue
import pandas as pd
import os, time

NUMBER_OF_THREADS = 2

estab = Estabelecimento()
dtypes = estab.get_dtypes()
file_name = "Estabelecimentos0.csv"
df = pd.read_csv(filepath_or_buffer=os.path.join("Files",file_name),
                 sep=";",
                 header=None,
                 names=estab.get_columns(),
                 dtype=str,
                 encoding="IBM860", # encoding for Portuguese Language
                 nrows=100_000)

substitute_value = int()
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

start = time.time()
my_queue = Queue()
for d in df.to_dict(orient="records"):
    my_queue.put(d)
del df

threads = [MyThread(estab,my_queue) for _ in range(NUMBER_OF_THREADS)]
estab.create_table()
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()

print(f"\n\nExecution of data insertion {time.time()-start}")



