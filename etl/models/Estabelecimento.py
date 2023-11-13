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

    def process_chunk(self, chunk, my_queue, engine) -> None:
        """
        Process the data of each chunk to make a clean insertion into the DataBase.
        """
        dtypes = self.get_dtypes()

        substitute_value = int()
        for k in self.fk:
            fk_values = self.get_fk_values(k, engine)
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
            chunk[k].fillna(substitute_value, inplace=True) # fill null values in Foreign Key field
            chunk[k] = chunk[k].astype(dtypes[k]) # initially the data is all strings, so change now the data type to compare in the next lines
            chunk[k] = chunk[k].apply(self.check_fk, args=(substitute_value,fk_values)) # check if the value will be accepted in FOREIGN KEY CONSTRAINTS
                                                                                        # if not then subtitute the value

        for date_field in ("data_situacao_cadastral","data_inicio_atividade","data_situacao_especial"):
            chunk[date_field].fillna("19000101", inplace=True) # The date to be substitute Nulls is '1900-01-01'
            chunk[date_field] = chunk[date_field].apply(self.date_format) # format field to 'yyyy-mm-dd'

        chunk = chunk.astype(dtypes)
        for d in chunk.to_dict(orient="records"): # 'orient="records"' will return a list with dictionaries
            my_queue.put(d) # insert each dictionary into queue to share the data between threads