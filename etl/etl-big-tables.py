from models.Estabelecimento import Estabelecimento
from models.Simples import Simples
from models.Empresa import Empresa
from multiprocessing import Pool
from models.Socio import Socio

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.engine import URL

from queue import Queue
from os import environ
from time import time

engine = create_engine(URL.create(drivername=environ["DB_DRIVERNAME"],
                                    username=environ["DB_USERNAME"],
                                    password=environ["DB_PASSWORD"],
                                    host=environ["DB_HOST"],
                                    port=environ["DB_PORT"],
                                    database=environ["DB_NAME"]), poolclass=NullPool)

ZIPFILES = ("Empresas0.zip", "Empresas1.zip", "Empresas2.zip", "Empresas3.zip", "Empresas4.zip",
            "Empresas5.zip", "Empresas6.zip", "Empresas7.zip", "Empresas8.zip", "Empresas9.zip",
            "Estabelecimentos0.zip", "Estabelecimentos1.zip", "Estabelecimentos2.zip", "Estabelecimentos3.zip", "Estabelecimentos4.zip",
            "Estabelecimentos5.zip", "Estabelecimentos6.zip", "Estabelecimentos7.zip", "Estabelecimentos8.zip", "Estabelecimentos9.zip",
            "Simples.zip",
            "Socios0.zip", "Socios1.zip", "Socios2.zip", "Socios3.zip", "Socios4.zip",
            "Socios5.zip", "Socios6.zip", "Socios7.zip", "Socios8.zip", "Socios9.zip")
link = "https://dadosabertos.rfb.gov.br/CNPJ/"
URLS = tuple((link+zf for zf in ZIPFILES))
THREADS_NUMBER = int(environ["THREADS_NUMBER"]) # Warning -> the number of threads will be multiplied by the number of processes
CHUNKSIZE = int(environ["CHUNKSIZE"]) # Warning -> the size of chunk will be for each process

# Create objects of all table classes and create the DataBase table
estab = Estabelecimento()
estab.create_table(engine)

socio = Socio()
socio.create_table(engine)

empresa = Empresa()
empresa.create_table(engine)

simples = Simples()
simples.create_table(engine)

def process_and_insert(url:str) -> None:
    """
    Function to map all files in multiprocessing.Pool making the transformation and insertion of data into the DataBase.
    """
    filename = url.split("/")[-1]
    chunk_count = 0
    print(f"Processing {filename}...")
    # Switch to correct table
    obj = None
    if "Estabelecimento" in filename:
        obj = estab
    elif "Socio" in filename:
        obj = socio
    elif "Empresa" in filename:
        obj = empresa
    elif "Simples" in filename:
        obj = simples

    my_queue = Queue() # one queue to each process so the threads inside those processes can safely share data
    df = obj.get_reader_file(url, CHUNKSIZE)
    for chunk in df:
        obj.process_chunk(chunk, my_queue)
        # the threads will process one chunck at a time
        threads = [obj.get_thread(my_queue, engine) for _ in range(THREADS_NUMBER)] # create objects of MyThread
        for thread in threads: # Start all threads
            thread.start()
        for thread in threads: # wait all to finish
            thread.join()
        chunk_count += 1
        print(f"\nChunk number {chunk_count} from {filename}\n")

engine.dispose()
if __name__ == "__main__":
    with Pool() as pool:
        start_all = time()
        pool.map(process_and_insert, URLS)
        exec_time_hr = ((time()-start_all)/60)/60
        print(f"\n\n############ Total time of execution {round(exec_time_hr,2)}hr ############\n\n")