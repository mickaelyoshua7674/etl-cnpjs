from models.BaseModel import *

class Empresa(BaseModel):
    table_name:str="empresa"

    schema:dict={
        "cnpj_basico":VARCHAR(8),
        "razao_social":VARCHAR(200),
        "natureza_juridica":INTEGER(),
        "qualificacao":INTEGER(),
        "capital_social":FLOAT(),
        "porte_empresa":INTEGER(),
        "ente_federativo_responsavel":VARCHAR(100)
    }
    
    fk:tuple=("natureza_juridica","qualificacao","porte_empresa")

    def process_chunk(self, chunk, my_queue) -> None:
        """
        Process the data of each chunk to make a clean insertion into the DataBase.
        """
        dtypes = self.get_dtypes()

        substitute_value = 0
        for k in self.fk:
            fk_values = self.get_fk_values(k)
            chunk[k].fillna(substitute_value, inplace=True) # fill null values in Foreign Key field
            chunk[k] = chunk[k].astype(dtypes[k]) # initially the data is all strings, so change now the data type to compare in the next lines
            chunk[k] = chunk[k].apply(self.check_fk, args=(substitute_value,fk_values)) # check if the value will be accepted in FOREIGN KEY CONSTRAINTS
                                                                                        # if not then subtitute the value

        chunk["capital_social"] = chunk["capital_social"].apply(lambda x: x.replace(",",".")) # replace to convert field to float
        chunk = chunk.astype(dtypes)
        for d in chunk.to_dict(orient="records"): # 'orient="records"' will return a list with dictionaries
            my_queue.put(d) # insert each dictionary into queue to share the data between threads