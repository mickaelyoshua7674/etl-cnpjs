from sqlalchemy.types import VARCHAR, INT, DATE

class Estabelecimento():
    schema:dict={
        "cnpj_basico":VARCHAR(8),
        "cnpj_ordem":VARCHAR(4),
        "cnpj_dv":VARCHAR(2),
        "identificador":INT,
        "nome_fantasia":VARCHAR(100),
        "situacao_cadastral":INT,
        "data_situacao_cadastral":DATE(),
        "motivo_situacao_cadastral":INT,
        "nome_cidade_exterior":VARCHAR(100),
        "pais":INT,
        "data_inicio_atividade":DATE(),
        "cnae":INT,
        "cnae_secundario":VARCHAR(800),
        "tipo_logradouro":VARCHAR(30),
        "logradouro":VARCHAR(70),
        "numero":VARCHAR(10),
        "complemento":VARCHAR(170),
        "bairro":VARCHAR(60),
        "cep":VARCHAR(10),
        "uf":VARCHAR(3),
        "municipio":INT,
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
    
    def get_columns(self) -> tuple:
        return tuple(i for i,_ in self.schema.items())
    
    def get_dtypes(self) -> dict:
        dtypes = {}
        for c, t in self.schema.items():
            match t:
                case VARCHAR():
                    dtypes[c] = str
                case DATE():
                    dtypes[c] = str
                case INT:
                    dtypes[c] = int
        return dtypes