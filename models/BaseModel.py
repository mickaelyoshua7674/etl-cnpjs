from sqlalchemy.types import VARCHAR, DATE, INTEGER, FLOAT
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

class BaseModel():

    with open("./secrets.txt", "r") as f:
        driver, username, password, host, port, database = f.read().split(",")
        engine = create_engine(URL.create(drivername=driver, username=username, password=password, host=host, port=port, database=database))

    table_name:str

    schema:dict

    fk:tuple

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
    
    def get_fk_values(self, column_name:str) -> set:
        with self.engine.begin() as conn:
            res = conn.execute(text(f"SELECT {column_name} FROM public.id_{column_name};"))
            return set(v[0] for v in res.fetchall())
        
    def get_engine_database():
        """Get secrets from '.secrets.txt' file and create and return connection engine to DataBase"""
        with open("./secrets.txt", "r") as f:
            driver, username, password, host, port, database = f.read().split(",")
            return create_engine(URL.create(drivername=driver, username=username, password=password, host=host, port=port, database=database))
    
    def check_fk(self, value:int, substitute_value:str, fk_values:set) -> int:
        return substitute_value if value not in fk_values else value

    def date_format(self, value:str) -> str:
        if len(value) == 8:
            return f"{value[:4]}-{value[4:6]}-{value[-2:]}"
        return "1900-01-01"
    
    def get_add_constraints_script(self) -> str:
        head = f"ALTER TABLE public.{self.table_name} "
        return head + ",".join([f"ADD CONSTRAINT {c} FOREIGN KEY ({c}) REFERENCES public.id_{c}({c})" for c in self.fk]) + ";"