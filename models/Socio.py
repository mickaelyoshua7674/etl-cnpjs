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
        print("Start processing data...")
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
            chunk[k].fillna(substitute_value, inplace=True)
            chunk[k] = chunk[k].astype(dtypes[k])
            chunk[k] = chunk[k].apply(self.check_fk, args=(substitute_value,fk_values))

        date_field = "data_entrada_sociedade"
        chunk[date_field].fillna("19000101", inplace=True)
        chunk[date_field] = chunk[date_field].apply(self.date_format)

        chunk = chunk.astype(dtypes)
        for d in chunk.to_dict(orient="records"):
            my_queue.put(d)
        print("Finished processing data.\n")