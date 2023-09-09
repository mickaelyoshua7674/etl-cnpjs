import pandas as pd
import os, re
import warnings # disable warnings
warnings.filterwarnings("ignore")

from time import time
start = time()

clean_chars_fn = lambda x: re.sub(r"(')|(%)|(\()|(\))", "", x)

MERGED_PATH = "mergedfiles"
TRANSFORMED_PATH = "transformedfiles"
if not os.path.exists(TRANSFORMED_PATH):
    os.mkdir(TRANSFORMED_PATH)

files = sorted(os.listdir(MERGED_PATH))

# EMPRESAS
empresas_filename = files[0]
print(f"Transforming {empresas_filename}...")
empresas_path = os.path.join(MERGED_PATH, empresas_filename)
empresas = pd.read_csv(empresas_path, chunksize=100_000, dtype={"cnpj_basico": "str"})

header = True
save_path_empresas = os.path.join(TRANSFORMED_PATH, empresas_filename)
for chunk in empresas:
    chunk = chunk.loc[chunk["cnpj_basico"] != "cnpj_basico"]
    chunk = chunk.fillna(value={"natureza_juridica": 0,
                                "qualificacoes": 0,
                                "porte_empresa": 0,
                                "razao_social": "Nao informado",
                                "ente_federativo_responsavel": "Nao informado"})
    chunk["qualificacoes"] = chunk["qualificacoes"].replace(36, 0)
    chunk["capital_social"] = chunk["capital_social"].apply(lambda x: x.replace(",", ".")).astype(float)
    chunk["razao_social"] = chunk["razao_social"].apply(clean_chars_fn)
    chunk["ente_federativo_responsavel"] = chunk["ente_federativo_responsavel"].apply(clean_chars_fn)
    chunk.to_csv(save_path_empresas, header=header, index=False, mode="a")
    header = False

# ESTABELECIMENTOS
estabelecimentos_filename = files[1]
print(f"Transforming {estabelecimentos_filename}...")
estabelecimentos_path = os.path.join(MERGED_PATH, estabelecimentos_filename)
estabelecimentos = pd.read_csv(estabelecimentos_path, chunksize=100_000, dtype={"cnpj_basico": "str",
                                                                                "cnpj_ordem": "str",
                                                                                "cnpj_dv": "str",
                                                                                "data_situacao_cadastral": "str",
                                                                                "data_inicio_atividade": "str",
                                                                                "cep": "str",
                                                                                "ddd1": "str",
                                                                                "telefone1": "str",
                                                                                "ddd2": "str",
                                                                                "telefone2": "str",
                                                                                "ddd_fax": "str",
                                                                                "situacao_especial": "str",
                                                                                "data_situacao_especial": "str"})

header = True
save_path_estabelecimentos = os.path.join(TRANSFORMED_PATH, estabelecimentos_filename)
for chunk in estabelecimentos:
    chunk = chunk.loc[chunk["cnpj_basico"] != "cnpj_basico"]
    chunk = chunk.fillna(value={"identificador": 0,
                                "situacao_cadastral": 1,
                                "cod_pais": 999,
                                "cod_municipio": 0,
                                "cnae_fiscal_principal": 0,
                                "motivo_situacao_cadastral": 0,
                                "nome_fantasia": "Nao informado"})
    chunk["cod_pais"] = chunk["cod_pais"].astype(int)
    chunk["cod_pais"] = chunk["cod_pais"].replace((367, 678, 150, 452, 359, 151, 737, 449, 994, 8, 498, 9), 999)
    chunk["nome_fantasia"] = chunk["nome_fantasia"].apply(clean_chars_fn)
    chunk.to_csv(save_path_estabelecimentos, header=header, index=False, mode="a")
    header = False

# # CNAE
# cnae_filename = files[2]
# print(f"Transforming {cnae_filename}...")
# cnae_path = os.path.join(MERGED_PATH, cnae_filename)
# cnae = pd.read_csv(cnae_path)

# save_path_cnae = os.path.join(TRANSFORMED_PATH, cnae_filename)
# cnae.to_csv(save_path_cnae, header=True, index=False)

# # MOTIVO
# motivo_filename = files[3]
# print(f"Transforming {motivo_filename}...")
# motivo_path = os.path.join(MERGED_PATH, motivo_filename)
# motivo = pd.read_csv(motivo_path)

# save_path_motivo = os.path.join(TRANSFORMED_PATH, motivo_filename)
# motivo.to_csv(save_path_motivo, header=True, index=False)

# # NATUREZA JURIDICA
# natjur_filename = files[4]
# print(f"Transforming {natjur_filename}...")
# natjur_path = os.path.join(MERGED_PATH, natjur_filename)
# natjur = pd.read_csv(natjur_path)

# save_path_natjur = os.path.join(TRANSFORMED_PATH, natjur_filename)
# natjur.to_csv(save_path_natjur, header=True, index=False)

# # QUALIFICACOES
# qualificacoes_filename = files[5]
# print(f"Transforming {qualificacoes_filename}...")
# qualificacoes_path = os.path.join(MERGED_PATH, qualificacoes_filename)
# qualificacoes = pd.read_csv(qualificacoes_path)

# save_path_qualificacoes = os.path.join(TRANSFORMED_PATH, qualificacoes_filename)
# qualificacoes.to_csv(save_path_qualificacoes, header=True, index=False)

# # MUNICIPIOS
# municipios_filename = files[6]
# print(f"Transforming {municipios_filename}...")
# municipios_path = os.path.join(MERGED_PATH, municipios_filename)
# municipios = pd.read_csv(municipios_path)
# municipios["descricao"] = municipios["descricao"].apply(lambda x: x.replace("'", ""))

# save_path_municipios = os.path.join(TRANSFORMED_PATH, municipios_filename)
# municipios.to_csv(save_path_municipios, header=True, index=False)

# # PAISES
# paises_filename = files[7]
# print(f"Transforming {paises_filename}...")
# paises_path = os.path.join(MERGED_PATH, paises_filename)
# paises = pd.read_csv(paises_path)

# save_path_paises = os.path.join(TRANSFORMED_PATH, paises_filename)
# paises.to_csv(save_path_paises, header=True, index=False)

# # SIMPLES
# simples_filename = files[8]
# print(f"Transforming {simples_filename}...")
# simples_path = os.path.join(MERGED_PATH, simples_filename)
# simples = pd.read_csv(simples_path, chunksize=100_000)

# header = True
# save_path_simples = os.path.join(TRANSFORMED_PATH, simples_filename)
# for chunk in simples:
#     chunk = chunk.loc[chunk["cnpj_basico"] != "cnpj_basico"]
#     chunk = chunk.fillna(value={"opcao_simples": 2,
#                                 "opcao_mei": 2})
#     chunk["opcao_simples"] = chunk["opcao_simples"].replace(["S", "N"], [1, 0])
#     chunk["opcao_mei"] = chunk["opcao_mei"].replace(["S", "N"], [1, 0])
#     chunk["opcao_simples"] = chunk["opcao_simples"].astype(int)
#     chunk["opcao_mei"] = chunk["opcao_mei"].astype(int)
#     chunk.to_csv(save_path_simples, header=header, index=False, mode="a")
#     header = False

# # SOCIOS
# socios_filename = files[9]
# print(f"Transforming {socios_filename}...")
# socios_path = os.path.join(MERGED_PATH, socios_filename)
# socios = pd.read_csv(socios_path, chunksize=100_000)

# header = True
# save_path_socios = os.path.join(TRANSFORMED_PATH, socios_filename)
# for chunk in socios:
#     chunk = chunk.loc[chunk["cnpj_basico"] != "cnpj_basico"]
#     chunk = chunk.fillna(value={"identificador_socio": 0,
#                                 "cod_pais": 999,
#                                 "qualificacoes": 0,
#                                 "nome_socio": "Não informado"})
#     chunk["cod_pais"] = chunk["cod_pais"].astype(int)
#     chunk["cod_pais"] = chunk["cod_pais"].replace((367, 678, 150, 452, 359, 151, 737, 449, 994, 8, 498, 9), 999)
#     chunk["nome_socio"] = chunk["nome_socio"].apply(lambda x: x.replace("'", ""))
#     chunk.to_csv(save_path_socios, header=header, index=False, mode="a")
#     header = False

print(f"Exec time {round(time()-start, 2)}s")