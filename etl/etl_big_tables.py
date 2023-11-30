from models.Estabelecimento import Estabelecimento
from models.Simples import Simples
from models.Empresa import Empresa
from models.Socio import Socio

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from __init__ import LARGE_ZIPFILES
from multiprocessing import Pool
from threading import Thread
from queue import Empty
from time import time
import os

def insert_data(engine, insert_script, my_queue) -> None:
    """
    Function to run inside Threads and load data into database
    """
    with engine.begin() as conn:
        while True:
            try:
                # when '.get()' is called retuns a value and delete that value from the queue
                row = my_queue.get(block=False) # 'block=False' allows to check the Empty exception
            except Empty: # if there is not values left inside the queue
                return # end the thread excecution
            else:
                conn.execute(insert_script, row)

def process_and_insert(path:str) -> None:
    """
    Function to map all files in multiprocessing.Pool making the transformation and insertion of data into the DataBase.
    """
    chunk_count = 0
    print(f"Processing {path}...")
    # Switch to correct table
    obj = None
    if "Estabelecimento" in path:
        obj = estab
    elif "Socio" in path:
        obj = socio
    elif "Empresa" in path:
        obj = empresa
    elif "Simples" in path:
        obj = simples

    df = obj.get_reader_file(path, CHUNKSIZE)
    for chunk in df:
        my_queue = obj.process_chunk(chunk, engine)
        # the threads will insert one chunck at a time
        threads = [Thread(target=insert_data, args=(engine, obj.get_insert_script(), my_queue)) for _ in range(THREADS_NUMBER)]
        for thread in threads: # Start all threads
            thread.start()
        for thread in threads: # wait all to finish
            thread.join()
        chunk_count += 1
        print(f"\nChunk number {chunk_count} from {path}\n")
    print(f"\n---Finished file {path}---\n")
    os.remove(path)

if __name__ == "__main__":
    print(f"Start multiprocessing pool...")
    FILES_FOLDER = os.environ["FILES_FOLDER"]
    files_path = [os.path.join(FILES_FOLDER,f) for f in os.listdir(FILES_FOLDER)]
    THREADS_NUMBER = int(os.environ["THREADS_NUMBER"]) # Warning -> the number of threads will be multiplied by the number of processes
    CHUNKSIZE = int(os.environ["CHUNKSIZE"]) # Warning -> the size of chunk will be for each process
    engine = create_engine(URL.create(drivername=os.environ["DB_DRIVERNAME"],
                                      username=os.environ["DB_USERNAME"],
                                      password=os.environ["DB_PASSWORD"],
                                      host=os.environ["DB_HOST"],
                                      port=os.environ["DB_PORT"],
                                      database=os.environ["DB_NAME"]))
    engine.dispose()

    estab = Estabelecimento()
    socio = Socio()
    empresa = Empresa()
    simples = Simples()

    if len(files_path) == len(LARGE_ZIPFILES): # when a file is fully loaded into the DataBase it's deleted,
                                         # so if all files are still there create the tables
        # Create objects of all table classes and create the DataBase table
        print("Creating tables...")
        estab.create_table(engine)
        socio.create_table(engine)
        empresa.create_table(engine)
        simples.create_table(engine)
        print("Tables created.")

    with Pool() as pool:
        start_all = time()
        pool.map(process_and_insert, files_path)
        exec_time_hr = ((time()-start_all)/60)/60
        print(f"\n\n############ Total time of execution {round(exec_time_hr,2)}hr ############\n\n")