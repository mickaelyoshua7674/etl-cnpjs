from models.Estabelecimento import Estabelecimento
from models.Simples import Simples
from models.Empresa import Empresa
from models.Socio import Socio
from multiprocessing import Pool
from queue import Queue
from glob import glob
from time import time
import pandas as pd

NUMBER_OF_THREADS = 10 # Warning -> the number of threads will be multiplied by the number of processes

# Create objects of all table classes and create the DataBase table
estab = Estabelecimento()
estab.create_table()

socio = Socio()
socio.create_table()

empresa = Empresa()
empresa.create_table()

simples = Simples()
simples.create_table()

def process_and_insert(file_path) -> None:
    """
    Function to map all files in multiprocessing.Pool making the transformation and insertion of data into the DataBase.
    """
    print(f"\nInserting {file_path}...\n")
    
    # Switch to correct table
    obj = None
    if "Estabelecimento" in file_path:
        obj = estab
    elif "Socio" in file_path:
        obj = socio
    elif "Empresa" in file_path:
        obj = empresa
    elif "Simples" in file_path:
        obj = simples

    my_queue = Queue() # one queue to each process so the threads inside those processes can safely share data
    df = pd.read_csv(filepath_or_buffer=file_path,
                     sep=";",
                     header=None, # the files don't have header
                     names=obj.get_columns(), # give the header
                     dtype=str, # all string to don't force any unwanted type
                     encoding="IBM860", # encoding for Portuguese Language
                     chunksize=100_000) # reader in chunks / since this function will run in 8 processes will be 800_000 rows in memory at a time
    start_file = time()
    for chunk in df:
        obj.process_chunk(chunk, my_queue)
        start_chunk = time()
        # the threads will process one chunck at a time
        threads = [obj.get_thread(my_queue) for _ in range(NUMBER_OF_THREADS)] # create objects of MyThread
        for thread in threads: # Start all threads
            thread.start()
        for thread in threads: # wait all to finish
            thread.join()
        print(f"Time execution of chunk in {file_path} {round(time()-start_chunk,2)}s")
    print(f"Time execution of {file_path} {round(time()-start_file,2)}s")

# getting list of all files
files_paths = glob("Files/Estabelecimentos*.csv") + glob("Files/Socios*.csv") + glob("Files/Empresas*.csv") + ["Files\\Simples.csv"]
if __name__ == "__main__":
    with Pool() as pool: # since 'processes' is not defined will use all available cores (mine is 8)
        start_all = time()
        pool.map(process_and_insert, files_paths)
        print(f"Total time of execution {round(time()-start_all,2)}s")

