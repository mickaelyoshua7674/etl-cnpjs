from models.BaseModel import *

class Estabelecimento(BaseModel):
    table_name:str="estabelecimento"

    schema:dict={
        "cnpj_basico":VARCHAR(8),
        "cnpj_ordem":VARCHAR(4),
        "cnpj_dv":VARCHAR(2),
        "identificador":INTEGER(),
        "nome_fantasia":VARCHAR(100),
        "situacao_cadastral":INTEGER(),
        "data_situacao_cadastral":DATE(),
        "motivo_situacao_cadastral":INTEGER(),
        "nome_cidade_exterior":VARCHAR(100),
        "pais":INTEGER(),
        "data_inicio_atividade":DATE(),
        "cnae":INTEGER(),
        "cnae_secundario":VARCHAR(800),
        "tipo_logradouro":VARCHAR(30),
        "logradouro":VARCHAR(70),
        "numero":VARCHAR(10),
        "complemento":VARCHAR(170),
        "bairro":VARCHAR(60),
        "cep":VARCHAR(10),
        "uf":VARCHAR(3),
        "municipio":INTEGER(),
        "ddd1":VARCHAR(5),
        "telefone1":VARCHAR(9),
        "ddd2":VARCHAR(5),
        "telefone2":VARCHAR(9),
        "ddd_fax":VARCHAR(5),
        "fax":VARCHAR(9),
        "email":VARCHAR(130),
        "situacao_especial":VARCHAR(100),
        "data_situacao_especial":DATE()
    }
    
    fk:tuple=("identificador","situacao_cadastral","motivo_situacao_cadastral","pais","cnae","municipio")