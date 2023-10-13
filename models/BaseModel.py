from sqlalchemy.types import VARCHAR, DATE, INTEGER, FLOAT
from sqlalchemy import create_engine, text
from models.MyThread import MyThread
from sqlalchemy.engine import URL

class BaseModel():
    """
    Base class to all table classes
    Each table class will define its own 'table_name', 'schema', 'fk' and 'process_chunk' methods.
    """
    with open("./secrets.txt", "r") as f: # creating one engine to all table classes
        driver, username, password, host, port, database = f.read().split(",")
        engine = create_engine(URL.create(drivername=driver, username=username, password=password, host=host, port=port, database=database))

    table_name:str

    schema:dict

    fk:tuple # Foreign Keys

    def get_columns(self) -> tuple:
        """
        From schema implemented in each table class
        return a tuple with all columns names.
        """
        return tuple(i for i,_ in self.schema.items())
    
    def get_dtypes(self) -> dict:
        """
        From schema implemented in each table class
        return a dict with format <column _name>:<column_type>
        where <column_type> is the Python type.
        """
        dtypes = {}
        for c, t in self.schema.items():
            match t: # using the new match/case of Python
                case VARCHAR():
                    dtypes[c] = str
                case DATE():
                    dtypes[c] = str # DATE type will be converted to string so the date values is formated to 'yyyy-mm-dd's
                case INTEGER():
                    dtypes[c] = int
                case FLOAT():
                    dtypes[c] = float
        return dtypes
    
    def get_fk_values(self, column_name:str) -> set:
        """
        Search by Foreign Key (column_name) and get values.
        Return values as a set to faster search using the 'in' keyword in method 'check_fk'.
        """
        with self.engine.begin() as conn:
            res = conn.execute(text(f"SELECT {column_name} FROM public.id_{column_name};"))
            return set(v[0] for v in res.fetchall())
    
    def check_fk(self, value:int, substitute_value:int, fk_values:set) -> int:
        """
        Check if the 'value' is in Foreign Keys set 'fk_values'
        If contains value, then leave the value, if don't them replace with the 'substitute_value'.

        This method is to make the CONSTRAINT of Foreign Keys.
        """
        return value if value in fk_values else substitute_value

    def date_format(self, value:str) -> str:
        """
        
        """
        if len(value) == 8 and value != "00000000":
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

    def get_thread(self, queue) -> MyThread:
        return MyThread(self.engine, queue, self.get_insert_script())
    
    def process_chunk(self) -> None:
        ...