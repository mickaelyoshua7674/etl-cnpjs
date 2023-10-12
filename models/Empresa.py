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
        print("Start processing data...")
        dtypes = self.get_dtypes()

        substitute_value = 0
        for k in self.fk:
            fk_values = self.get_fk_values(k)
            chunk[k].fillna(substitute_value, inplace=True)
            chunk[k] = chunk[k].astype(dtypes[k])
            chunk[k] = chunk[k].apply(self.check_fk, args=(substitute_value,fk_values))

        chunk["capital_social"] = chunk["capital_social"].apply(lambda x: x.replace(",","."))
        chunk = chunk.astype(dtypes)
        for d in chunk.to_dict(orient="records"):
            my_queue.put(d)
        print("Finished processing data.\n")