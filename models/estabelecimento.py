from sqlalchemy.types import VARCHAR, INT

class Estabelecimento():
    schema:dict={
        "cnpj_basico":VARCHAR(),
        "cnpj_ordem":VARCHAR(),
        "cnpj_dv":VARCHAR(),
        "identificador":INT,
        "nome_fantasia":VARCHAR(),
        "situacao_cadastral":INT,
        "data_situacao_cadastral":VARCHAR(),
        "motivo_situacao_cadastral":INT,
        "nome_cidade_exterior":VARCHAR(),
        "pais":INT,
        "data_inicio_atividade":VARCHAR(),
        "cnae":INT,
        "cnae_secundario":VARCHAR(),
        "tipo_logradouro":VARCHAR(),
        "logradouro":VARCHAR(),
        "numero":VARCHAR(),
        "complemento":VARCHAR(),
        "bairro":VARCHAR(),
        "cep":VARCHAR(),
        "uf":VARCHAR(),
        "municipio":INT,
        "ddd1":VARCHAR(),
        "telefone1":VARCHAR(),
        "ddd2":VARCHAR(),
        "telefone2":VARCHAR(),
        "ddd_fax":VARCHAR(),
        "fax":VARCHAR(),
        "email":VARCHAR(),
        "situacao_especial":VARCHAR(),
        "data_situacao_especial":VARCHAR()
    }
    def get_schema(self) -> dict:
        return self.schema
    
    def get_columns(self) -> tuple[str]:
        return tuple(i for i,_ in self.schema.items())