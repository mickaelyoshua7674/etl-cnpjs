from models.BaseModel import *
from queue import Queue

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

    def process_chunk(self, chunk, engine) -> Queue:
        """
        Process the data of each chunk to make a clean insertion into the DataBase.
        """
        dtypes = self.get_dtypes()

        substitute_value = 2
        for k in self.fk:
            fk_values = self.get_fk_values(k, engine)
            chunk[k].fillna(substitute_value, inplace=True) # fill null values in Foreign Key field
            chunk[k].replace("N",substitute_value, inplace=True) # replace to number equivalent in 'opcao_simples'/'opcao_mei'
            chunk[k].replace("S",substitute_value, inplace=True) # replace to number equivalent in 'opcao_simples'/'opcao_mei'
            chunk[k] = chunk[k].astype(dtypes[k]) # initially the data is all strings, so change now the data type to compare in the next lines
            chunk[k] = chunk[k].apply(self.check_fk, args=(substitute_value,fk_values)) # check if the value will be accepted in FOREIGN KEY CONSTRAINTS
                                                                                        # if not then subtitute the value

        for date_field in ("data_opcao_simples","data_exclusao_simples","data_opcao_mei","data_exclusao_mei"):
            chunk[date_field].fillna("19000101", inplace=True) # The date to be substitute Nulls is '1900-01-01'
            chunk[date_field] = chunk[date_field].apply(self.date_format) # format field to 'yyyy-mm-dd'

        chunk = chunk.astype(dtypes)
        my_queue = Queue() # one queue to each process so the threads inside those processes can safely share data
        for d in chunk.to_dict(orient="records"): # 'orient="records"' will return a list with dictionaries
            my_queue.put(d) # insert each dictionary into queue to share the data between threads
        return my_queue