from models.Estabelecimento import Estabelecimento
from multiprocessing import Pool, cpu_count
from models.Simples import Simples
from models.Empresa import Empresa
from models.Socio import Socio
from queue import Queue
from os import environ
from time import time

URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
ZIPFILES = ("Empresas0.zip", "Empresas1.zip", "Empresas2.zip", "Empresas3.zip", "Empresas4.zip",
            "Empresas5.zip", "Empresas6.zip", "Empresas7.zip", "Empresas8.zip", "Empresas9.zip",
            "Estabelecimentos0.zip", "Estabelecimentos1.zip", "Estabelecimentos2.zip", "Estabelecimentos3.zip", "Estabelecimentos4.zip",
            "Estabelecimentos5.zip", "Estabelecimentos6.zip", "Estabelecimentos7.zip", "Estabelecimentos8.zip", "Estabelecimentos9.zip",
            "Simples.zip",
            "Socios0.zip", "Socios1.zip", "Socios2.zip", "Socios3.zip", "Socios4.zip",
            "Socios5.zip", "Socios6.zip", "Socios7.zip", "Socios8.zip", "Socios9.zip")
URLS = tuple((URL+zf for zf in ZIPFILES))
THREADS_NUMBER = int(environ["THREADS_NUMBER"]) # Warning -> the number of threads will be multiplied by the number of processes
PROCESSES_NUMBER = cpu_count() # using all cores availables (mine is 8)
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

def process_and_insert(url:str) -> None:
    """
    Function to map all files in multiprocessing.Pool making the transformation and insertion of data into the DataBase.
    """
    print(f"Processing {url.split("/")[-1]}...")
    # Switch to correct table
    obj = None
    if "Estabelecimento" in url:
        obj = estab
    elif "Socio" in url:
        obj = socio
    elif "Empresa" in url:
        obj = empresa
    elif "Simples" in url:
        obj = simples

    my_queue = Queue() # one queue to each process so the threads inside those processes can safely share data
    df = obj.get_reader_file(url, CHUNKSIZE)
    for chunk in df:
        obj.process_chunk(chunk, my_queue)

        # the threads will process one chunck at a time
        threads = [obj.get_thread(my_queue) for _ in range(THREADS_NUMBER)] # create objects of MyThread
        for thread in threads: # Start all threads
            thread.start()
        for thread in threads: # wait all to finish
            thread.join()

if __name__ == "__main__":
    with Pool(processes=PROCESSES_NUMBER) as pool:
        start_all = time()
        pool.map(process_and_insert, URLS)
        print(f"Total time of execution {round(time()-start_all,2)}s")

