from models.BaseModel import *

class TestClass(BaseModel):
    table_name:str="test"

    schema:dict={
        "column1":VARCHAR(),
        "column2":FLOAT(),
        "column3":INTEGER(),
        "column4":DATE()
    }
    
    fk:tuple=("column3")