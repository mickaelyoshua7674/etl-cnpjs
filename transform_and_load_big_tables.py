from models.Estabelecimento import Estabelecimento
from models.Simples import Simples
from models.Empresa import Empresa
from models.Socio import Socio
from multiprocessing import Pool
from queue import Queue
from os import environ
from glob import glob
from time import time
from tqdm import tqdm

THREADS_NUMBER = int(environ["THREADS_NUMBER"]) # Warning -> the number of threads will be multiplied by the number of processes
CHUNKSIZE = int(environ["CHUNKSIZE"]) # Warning -> the size of chunk will be for each process

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
    df = obj.get_reader_file(file_path, CHUNKSIZE)
    number_of_chunks = sum(1 for _ in obj.get_reader_file(file_path, CHUNKSIZE))
    # getting the file iterator again because can't interate on original 'df'. If iterate the original the iterator state will be at the end
    with tqdm(total=number_of_chunks, desc=file_path, unit="chunk") as pbar: # see progress of chunk insertion in each file
        for chunk in df:
            obj.process_chunk(chunk, my_queue)

            # the threads will process one chunck at a time
            threads = [obj.get_thread(my_queue) for _ in range(THREADS_NUMBER)] # create objects of MyThread
            for thread in threads: # Start all threads
                thread.start()
            for thread in threads: # wait all to finish
                thread.join()
            pbar.update(1)

# getting list of all files
files_paths = glob("Files/Estabelecimentos*.csv") + glob("Files/Socios*.csv") + glob("Files/Empresas*.csv") + ["Files\\Simples.csv"]
if __name__ == "__main__":
    with Pool() as pool: # since 'processes' is not defined will use all available cores (mine is 8)
        start_all = time()
        pool.map(process_and_insert, files_paths)
        print(f"Total time of execution {round(time()-start_all,2)}s")

