from models.BaseModel import *

class Socio(BaseModel):
    table_name:str="socio"

    schema:dict={
        "cnpj_basico":VARCHAR(8),
        "identificador_socio":INTEGER(),
        "nome_socio":VARCHAR(200),
        "cnpj_cpf_socio":VARCHAR(20),
        "qualificacao":INTEGER(),
        "data_entrada_sociedade":DATE(),
        "pais":INTEGER(),
        "representante_legal":VARCHAR(15),
        "nome_representante":VARCHAR(200),
        "qualificacao_representante":INTEGER(),
        "faixa_etaria":VARCHAR(2)
    }
    
    fk:tuple=("identificador_socio","pais","qualificacao")

    def process_chunk(self, chunk, my_queue) -> None:
        """
        Process the data of each chunk to make a clean insertion into the DataBase.
        """
        dtypes = self.get_dtypes()

        substitute_value = int()
        for k in self.fk:
            fk_values = self.get_fk_values(k)
            match k:
                case "identificador_socio":
                    substitute_value = 0
                case "pais":
                    substitute_value = 999
                case "qualificacao":
                    substitute_value = 0
            chunk[k].fillna(substitute_value, inplace=True) # fill null values in Foreign Key field
            chunk[k] = chunk[k].astype(dtypes[k]) # initially the data is all strings, so change now the data type to compare in the next lines
            chunk[k] = chunk[k].apply(self.check_fk, args=(substitute_value,fk_values)) # check if the value will be accepted in FOREIGN KEY CONSTRAINTS
                                                                                        # if not then subtitute the value

        date_field = "data_entrada_sociedade"
        chunk[date_field].fillna("19000101", inplace=True) # The date to be substitute Nulls is '1900-01-01'
        chunk[date_field] = chunk[date_field].apply(self.date_format) # format field to 'yyyy-mm-dd'

        chunk = chunk.astype(dtypes)
        for d in chunk.to_dict(orient="records"): # 'orient="records"' will return a list with dictionaries
            my_queue.put(d) # insert each dictionary into queue to share the data between threads