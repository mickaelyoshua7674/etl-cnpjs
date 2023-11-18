from models.BaseModel import *

class TestClass(BaseModel):
    table_name:str="test"

    schema:dict={
        "column1":VARCHAR(8),
        "column2":FLOAT(),
        "column3":VARCHAR(2),
        "column4":DATE()
    }
    
    fk:tuple=("identificador","situacao_cadastral","motivo_situacao_cadastral","pais","cnae","municipio")