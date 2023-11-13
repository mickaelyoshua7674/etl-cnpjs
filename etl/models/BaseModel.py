from sqlalchemy.types import VARCHAR, DATE, INTEGER, FLOAT
from models.MyThread import MyThread
from sqlalchemy import text
import pandas as pd

class BaseModel():
    """
    Base class to all table classes
    Each table class will define its own 'table_name', 'schema', 'fk' and 'process_chunk' methods.
    """

    table_name:str

    schema:dict

    fk:tuple # Foreign Keys

    def get_reader_file(self, url:str, chunksize:int):
        """
        Return an TextFileReader with given chunksize from given file path.
        Will read all the columns in string format so don't lose left zeros i.e.
        """
        return pd.read_csv(filepath_or_buffer=url,
                           compression="zip",
                           sep=";",
                           header=None, # the files don't have header
                           names=self.get_columns(), # give the header
                           dtype=str, # all string to don't force any unwanted type
                           encoding="IBM860", # encoding for Portuguese Language
                           chunksize=chunksize) # reader in chunks / since this function will run in 8 processes will be 800_000 rows in memory at a time

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
                    dtypes[c] = str # DATE type will be converted to string so the date values is formated to 'yyyy-mm-dd'
                case INTEGER():
                    dtypes[c] = int
                case FLOAT():
                    dtypes[c] = float
        return dtypes
    
    def get_fk_values(self, column_name:str, engine) -> set:
        """
        Search by Foreign Key (column_name) and get values.
        Return values as a set to faster search using the 'in' keyword in method 'check_fk'.
        """
        with engine.begin() as conn:
            res = conn.execute(text(f"SELECT {column_name} FROM id_{column_name};"))
            return set(v[0] for v in res.fetchall())
    
    def check_fk(self, value:int, substitute_value:int, fk_values:set) -> int: # return an int because all Foreign Keys are int
        """
        Check if the 'value' is in Foreign Keys set 'fk_values'
        If contains value, then leave the value, if don't them replace with the 'substitute_value'.

        This method is to make the CONSTRAINT of Foreign Keys.
        """
        return value if value in fk_values else substitute_value

    def date_format(self, value:str) -> str:
        """
        From value in date fields return the date as 'yyyy-mm-dd'.
        The date to be substitute Nulls and not valid values is '1900-01-01'.
        """
        if len(value) == 8 and value != "00000000":
            return f"{value[:4]}-{value[4:6]}-{value[-2:]}"
        return "1900-01-01"
    
    def create_table(self, engine) -> None:
        """
        Drop the table if exists (will always reset the table) then create again.
        This function will use the 'schema' and 'table_name' defined in each table class.
        """
        head = f"DROP TABLE IF EXISTS {self.table_name};\nCREATE TABLE {self.table_name} (\n  "
        columns = [f"{k} {i}"for k, i in self.schema.items()] # all Tables columns
        constraints = [f"CONSTRAINT {c} FOREIGN KEY ({c}) REFERENCES id_{c}({c})" for c in self.fk] # all constraints of Foreign Keys
        script = head + ",\n    ".join(columns+constraints) + "\n);"
        with engine.begin() as conn:
            conn.execute(text(script))

    def get_insert_script(self):
        """
        Return a string that already passed through the 'text()' function from 'sqlalchemy'.

        The String format is: INSERT INTO {table_name} VALUES (:{field1},:{field2},...,:{fieldN});

        With this format, the paramns passed to 'execute()' of sqlalchemy must be dictionaries with '{filed}:{value}'.
        """
        head = f"INSERT INTO {self.table_name} VALUES ("
        return text(head + ",".join([f":{k}" for k in self.schema.keys()]) + ");")

    def get_thread(self, queue, engine) -> MyThread:
        """
        Return an object of MyThread class with the passed 'queue' to share data between threads,
        the script for insert data and the engine to each thread create a connection to the DataBase.
        """
        return MyThread(engine, queue, self.get_insert_script())
    
    def process_chunk(self) -> None:
        """
        Process the data of each chunk to make a clean insertion into the DataBase.
        """
        ...