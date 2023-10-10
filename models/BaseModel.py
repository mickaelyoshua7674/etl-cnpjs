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
    
    def check_fk(self, value:int, substitute_value:int, fk_values:set) -> int:
        return value if value in fk_values else substitute_value

    def date_format(self, value:str) -> str:
        if len(value) == 8:
            return f"{value[:4]}-{value[4:6]}-{value[-2:]}"
        return "1900-01-01"
    
    def create_table(self) -> None:
        head = f"DROP TABLE IF EXISTS public.{self.table_name};\nCREATE TABLE public.{self.table_name} (\n  "
        columns = [f"{k} {i}"for k, i in self.schema.items()]
        constraints = [f"CONSTRAINT {c} FOREIGN KEY ({c}) REFERENCES public.id_{c}({c})" for c in self.fk]
        script = head + ",\n    ".join(columns+constraints) + "\n);"
        with self.engine.begin() as conn:
            conn.execute(text(script))
    
    def insert_data(self, df) -> None:
        with self.engine.connect() as conn:
            df.to_sql(name=self.table_name, con=conn, if_exists="append", index=False, dtype=self.schema)
            conn.commit()

    def get_insert_script(self) -> str:
        head = f"INSERT INTO public.{self.table_name} VALUES ("
        return text(head + ",".join([f":{k}" for k in self.schema.keys()]) + ");")
    
    def row_to_dict(self, row:tuple) -> dict:
        return {key:value for key, value in zip(self.schema.keys(), row)}

