from models.BaseModel import *

class Simples(BaseModel):
    table_name:str="simples"

    schema:dict={
        "cnpj_basico":VARCHAR(8),
        "opcao_simples":INTEGER(),
        "data_opcao_simples":DATE(),
        "data_exclusao_simples":DATE(),
        "opcao_mei":INTEGER(),
        "data_opcao_mei":DATE(),
        "data_exclusao_mei":DATE()
    }
    
    fk:tuple=("opcao_simples","opcao_mei")

    def process_chunk(self, chunk, my_queue) -> None:
        dtypes = self.get_dtypes()

        substitute_value = 2
        for k in self.fk:
            fk_values = self.get_fk_values(k)
            chunk[k].fillna(substitute_value, inplace=True)
            chunk[k].replace("N",substitute_value, inplace=True)
            chunk[k].replace("S",substitute_value, inplace=True)
            chunk[k] = chunk[k].astype(dtypes[k])
            chunk[k] = chunk[k].apply(self.check_fk, args=(substitute_value,fk_values))

        for date_field in ("data_opcao_simples","data_exclusao_simples","data_opcao_mei","data_exclusao_mei"):
            chunk[date_field].fillna("19000101", inplace=True)
            chunk[date_field] = chunk[date_field].apply(self.date_format)

        chunk = chunk.astype(dtypes)
        for d in chunk.to_dict(orient="records"):
            my_queue.put(d)