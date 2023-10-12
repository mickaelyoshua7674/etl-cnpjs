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

estab = Estabelecimento()
estab.create_table()

socio = Socio()
socio.create_table()

empresa = Empresa()
empresa.create_table()

simples = Simples()
simples.create_table()

def process_and_insert(file_path) -> None:
    print(f"Inserting {file_path}...")
    
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
    start_file = time()
    for chunk in df:
        obj.process_chunk(chunk, my_queue)

        start_chunk = time()
        threads = [obj.get_thread(my_queue) for _ in range(NUMBER_OF_THREADS)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        print(f"\n\nTime execution of chunk in {file_path} {round(time()-start_chunk,2)}s")
    print(f"\n\nTime execution of {file_path} {round(time()-start_file,2)}s")

files_paths = glob("Files/Estabelecimentos*.csv") + glob("Files/Socios*.csv") + glob("Files/Empresas*.csv") + ["Files\\Simples.csv"]
if __name__ == "__main__":
    with Pool() as pool:
        pool.map(process_and_insert, files_paths)


