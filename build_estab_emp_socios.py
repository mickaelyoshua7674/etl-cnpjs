from helper_load import get_engine_database
import pandas as pd

with open("./secrets.txt", "r") as f:
    driver, username, password, host, port, database = f.read().split(",")
engine = get_engine_database(driver, username, password, host, port, database)

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

with pd.ExcelWriter("Empresas-Sócios_CNAE-9602501_Cidade-Maringa_test_1.xlsx") as writer:
    estab.to_excel(writer, sheet_name="Empresas", index=False)
    socios.to_excel(writer, sheet_name="Sócios", index=False)