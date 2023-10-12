from models.Estabelecimento import Estabelecimento
from models.Simples import Simples
from models.Empresa import Empresa
from models.Socio import Socio
from multiprocessing import Pool
from queue import Queue
from glob import glob
from time import time
import pandas as pd

NUMBER_OF_THREADS = 10
# files_paths = glob("Files/Estabelecimentos*.csv") + glob("Files/Socios*.csv") + glob("Files/Empresas*.csv") + ["Files\\Simples.csv"]
files_paths = glob("Files/Estabelecimentos*.csv")
estab = Estabelecimento()
estab.create_table()

files_paths = glob("Files/Socios*.csv")
socio = Socio()
socio.create_table()

files_paths = glob("Files/Empresas*.csv")
empresa = Empresa()
empresa.create_table()

files_paths = ["Files\\Simples.csv"]
simples = Simples()
simples.create_table()

def process_and_insert(file_path) -> None:
    print("Start process...")
    
    obj = None
    if "Estabelecimento" in file_path:
        obj = estab
    elif "Socio" in file_path:
        obj = socio
    elif "Empresa" in file_path:
        obj = empresa
    elif "Simples" in file_path:
        obj = simples

    my_queue = Queue()
    df = pd.read_csv(filepath_or_buffer=file_path,
                        sep=";",
                        header=None,
                        names=obj.get_columns(),
                        dtype=str,
                        encoding="IBM860", # encoding for Portuguese Language
                        chunksize=100_000)
    for chunk in df:
        obj.process_chunk(chunk, my_queue)

        start = time()
        print("Start inserting into database...")
        threads = [obj.get_thread(my_queue) for _ in range(NUMBER_OF_THREADS)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        print(f"\n\nExecution of data insertion {round(time()-start,2)}s")

if __name__ == "__main__":
    with Pool() as pool:
        pool.map(process_and_insert, files_paths)


