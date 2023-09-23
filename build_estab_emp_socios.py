from helper_extract import *
from random import shuffle
import re

with open("extract_empresas.sql", "r") as f:
    estab_query = f.read()

with engine.begin() as conn:
    estab = pd.read_sql(estab_query, conn)

    socios_query = f"""SELECT
    s.cnpj_basico AS "CNPJ Básico",
    isocio.descricao AS "Identificador Sócio",
    s.nome_socio AS "Nome Sócio",
    s.cnpj_cpf_socio AS "CNPJ/CPF Sócio",
    q.descricao AS "Qualificação Sócio",
    CONCAT(RIGHT(s.data_entrada_sociedade,2), '/',
        SUBSTRING(s.data_entrada_sociedade FROM 5 FOR 2), '/',
        LEFT(s.data_entrada_sociedade,4)) AS "Data Entrada Sociedade",
    paises.pais AS "País",
    s.representante_legal AS "CPF Representante Legal",
    s.nome_representante AS "Nome Representante Legal"
FROM public.socios AS s 

LEFT JOIN public.id_identificador_socio AS isocio
USING(identificador_socio)
LEFT JOIN public.id_qualificacoes AS q
USING(qualificacoes)
LEFT JOIN public.paises as paises
USING(cod_pais)
WHERE s.cnpj_basico IN ({",".join(estab["CNPJ Básico"].apply(lambda x: f"'{x}'").unique())});"""

    socios = pd.read_sql(socios_query, conn)


nomes, cnpjs_cpfs = socios["Nome Sócio"], socios["CNPJ/CPF Sócio"]
socio_numero_dict, todos_telefones = get_data(nomes, cnpjs_cpfs)
socios_telefones, socios_telefones_replace = get_socios_telefones(nomes, cnpjs_cpfs, socio_numero_dict, todos_telefones)
while True:
    nomes, cnpjs_cpfs = [i[0] for i in socios_telefones_replace], [i[1] for i in socios_telefones_replace]
    socio_numero_dict, todos_telefones = get_data(nomes, cnpjs_cpfs, [i[-1] for i in socios_telefones])
    novo_socios_telefones, socios_telefones_replace = get_socios_telefones(nomes, cnpjs_cpfs, socio_numero_dict, todos_telefones, socios_telefones)

    if len(socios_telefones) == len(novo_socios_telefones):
        socios_telefones = novo_socios_telefones
        break

    socios_telefones = novo_socios_telefones


with engine.begin() as conn:
    qry = f"""SELECT DISTINCT
                CASE
                    WHEN telefone1 IS NOT NULL THEN CONCAT(ddd1,' ',telefone1)
                    ELSE CONCAT(ddd2,' ',telefone2)
                END AS telefone
            FROM public.estabelecimentos
            WHERE
                ((ddd1 IS NOT NULL AND telefone1 IS NOT NULL) OR
                (ddd2 IS NOT NULL AND telefone2 IS NOT NULL)) AND
                situacao_cadastral = 8;"""
    rand_telefones_chunk = pd.read_sql(qry, conn, chunksize=100_000)

r = []
for chunk in rand_telefones_chunk:
    for value in chunk["telefone"].unique():
        v = value.strip()
        if re.match(r"[0-9]{2,} [0-9]+", v):
            r.append(v)
    break

shuffle(r)
rand_telefones = r[:len(socios_telefones_replace)]
for nc, t in zip(socios_telefones_replace, rand_telefones):
    socios_telefones.append((nc[0], nc[1], t))
st = pd.DataFrame(data=socios_telefones, columns=["Nome Sócio", "CNPJ/CPF Sócio", "Telefone"])
socios_final = pd.merge(socios, st, how="inner", on=["Nome Sócio","CNPJ/CPF Sócio"])

with pd.ExcelWriter("Empresas-Sócios_CNAE-9602501_Cidade-Maringa_test_2.xlsx") as writer:
    estab.to_excel(writer, sheet_name="Empresas", index=False)
    socios_final.to_excel(writer, sheet_name="Sócios", index=False)