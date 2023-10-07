from sqlalchemy.types import VARCHAR, DATE, INTEGER, FLOAT
from helper_functions import get_engine_database
from sqlalchemy import text

engine = get_engine_database()
class BaseModel():
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
        with engine.begin() as conn:
            res = conn.execute(text(f"SELECT {column_name} FROM public.id_{column_name};"))
            return set(v[0] for v in res.fetchall())