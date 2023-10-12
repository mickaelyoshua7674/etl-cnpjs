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

    def process_file(self, chunk, my_queue) -> None:
        print("Start processing data...")
        dtypes = self.get_dtypes()

        substitute_value = int()
        for k in self.fk:
            fk_values = self.get_fk_values(k)
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
            chunk[k].fillna(substitute_value, inplace=True)
            chunk[k] = chunk[k].astype(dtypes[k])
            chunk[k] = chunk[k].apply(self.check_fk, args=(substitute_value,fk_values))

        for date_field in ("data_situacao_cadastral","data_inicio_atividade","data_situacao_especial"):
            chunk[date_field].fillna("19000101", inplace=True)
            chunk[date_field] = chunk[date_field].apply(self.date_format)

        chunk = chunk.astype(dtypes)
        for d in chunk.to_dict(orient="records"):
            my_queue.put(d)
        print("Finished processing data.\n")