from sqlalchemy.types import VARCHAR, DATE, INTEGER, FLOAT
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

class BaseModel():

    with open("./secrets.txt", "r") as f:
        driver, username, password, host, port, database = f.read().split(",")
        engine = create_engine(URL.create(drivername=driver, username=username, password=password, host=host, port=port, database=database))
    
    schema:dict

    def get_columns(self) -> tuple:
        return tuple(i for i,_ in self.schema.items())
    
    def get_dtypes(self) -> dict:
        dtypes = {}
        for c, t in self.schema.items():
            match t:
                case VARCHAR():
                    dtypes[c] = str
                case DATE():
                    dtypes[c] = str
                case INTEGER():
                    dtypes[c] = int
                case FLOAT():
                    dtypes[c] = float
        return dtypes
    
    def get_constraint_values(self, column_name:str) -> set:
        with self.engine.begin() as conn:
            res = conn.execute(text(f"SELECT {column_name} FROM public.id_{column_name};"))
            return set(v[0] for v in res.fetchall())
        
    def get_engine_database():
        """Get secrets from '.secrets.txt' file and create and return connection engine to DataBase"""
        with open("./secrets.txt", "r") as f:
            driver, username, password, host, port, database = f.read().split(",")
            return create_engine(URL.create(drivername=driver, username=username, password=password, host=host, port=port, database=database))