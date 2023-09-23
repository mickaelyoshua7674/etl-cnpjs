from collections import defaultdict
from helper_load import get_engine_database
import pandas as pd

with open("./secrets.txt", "r") as f:
    driver, username, password, host, port, database = f.read().split(",")
engine = get_engine_database(driver, username, password, host, port, database)

def get_data(nomes: list[str], cnpjs_cpfs: list[str], telefones_coletados: list[str]=[]) -> tuple[dict, set]:
    socio_numero_dict = defaultdict(str)
    for nome, cnpj_cpf in zip(nomes, cnpjs_cpfs):
        with engine.begin() as conn:
            qry = f"""SELECT telefone FROM public.socio_telefone
            WHERE nome_socio = '{nome}' AND cnpj_cpf_socio = '{cnpj_cpf}' AND telefone <> ' ';"""

            socio_numero_dict[f"{nome}-{cnpj_cpf}"] = pd.read_sql(qry, conn).dropna().drop_duplicates()["telefone"].values[0]

    todos_telefones = set()
    for _, t in dict(socio_numero_dict).items():
        todos_telefones.add(t)

    if telefones_coletados:
        return dict(socio_numero_dict), set([t for t in todos_telefones if t not in telefones_coletados])
    
    return dict(socio_numero_dict), todos_telefones

def get_socios_telefones(nomes: list[str],
                         cnpjs_cpfs: list[str],
                         socio_numero_dict: dict[str],
                         todos_telefones: set,
                         socios_telefones: list[tuple] = []) -> tuple[list[tuple], list[tuple]]:
    socios_telefones_replace = []
    for nome, cnpj_cpf in zip(nomes, cnpjs_cpfs):
        ts = socio_numero_dict[f"{nome}-{cnpj_cpf}"]
        if ts in todos_telefones:
            socios_telefones.append((nome, cnpj_cpf, ts))
            todos_telefones.remove(ts)
        else:
            socios_telefones_replace.append((nome, cnpj_cpf))
    return socios_telefones, socios_telefones_replace