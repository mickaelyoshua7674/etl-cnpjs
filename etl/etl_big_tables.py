from models.Estabelecimento import Estabelecimento
from models.Simples import Simples
from models.Empresa import Empresa
from models.Socio import Socio

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from multiprocessing import Pool
from __init__ import ZIPFILES
from threading import Thread
from queue import Empty
from os import environ
from time import time

link = "https://dadosabertos.rfb.gov.br/CNPJ/"
URLS = tuple((link+zf for zf in ZIPFILES))
THREADS_NUMBER = int(environ["THREADS_NUMBER"]) # Warning -> the number of threads will be multiplied by the number of processes
CHUNKSIZE = int(environ["CHUNKSIZE"]) # Warning -> the size of chunk will be for each process

engine = create_engine(URL.create(drivername=environ["DB_DRIVERNAME"],
                                  username=environ["DB_USERNAME"],
                                  password=environ["DB_PASSWORD"],
                                  host=environ["DB_HOST"],
                                  port=environ["DB_PORT"],
                                  database=environ["DB_NAME"]))

# Create objects of all table classes and create the DataBase table
print("Creating tables...")
estab = Estabelecimento()
estab.create_table(engine)

socio = Socio()
socio.create_table(engine)

empresa = Empresa()
empresa.create_table(engine)

simples = Simples()
simples.create_table(engine)
print("Tables created.")

def insert_data(engine, insert_script, my_queue) -> None:
    """
    When 'Thread.start' is called, also call the 'run()' method.
    Here we are overwriting the method to create a connection to the DataBase and insert values
    """
    with engine.connect() as conn:
        while True:
            try:
                # when '.get()' is called retuns a value and delete that value from the queue
                row = my_queue.get(block=False) # 'block=False' allows to check the Empty exception
            except Empty: # if there is not values left inside the queue
                return # end the thread excecution
            else:
                conn.execute(insert_script, row)
                conn.commit()

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

    df = obj.get_reader_file(url, CHUNKSIZE)
    for chunk in df:
        my_queue = obj.process_chunk(chunk, engine)
        # the threads will insert one chunck at a time
        threads = [Thread(target=insert_data, args=(engine, obj.get_insert_script(), my_queue), daemon=True) for _ in range(THREADS_NUMBER)]
        for thread in threads: # Start all threads
            thread.start()
        for thread in threads: # wait all to finish
            thread.join()
        chunk_count += 1
        print(f"\nChunk number {chunk_count} from {filename}\n")

print(f"Start multiprocessing pool...")
engine.dispose()
if __name__ == "__main__":
    with Pool() as pool:
        start_all = time()
        pool.map(process_and_insert, URLS)
        exec_time_hr = ((time()-start_all)/60)/60
        print(f"\n\n############ Total time of execution {round(exec_time_hr,2)}hr ############\n\n")