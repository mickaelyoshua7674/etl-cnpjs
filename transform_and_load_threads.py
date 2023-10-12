from models.Estabelecimento import Estabelecimento
from multiprocessing import Pool
from queue import Queue
from glob import glob
import pandas as pd
import time

NUMBER_OF_THREADS = 10
# files_paths = glob("Files/Estabelecimentos*.csv") + glob("Files/Socios*.csv") + glob("Files/Empresas*.csv") + ["Files\\Simples.csv"]
files_paths = glob("Files/Estabelecimentos*.csv")
estab = Estabelecimento()
estab.create_table()

def process_and_insert(file_path) -> None:
    print("Start process...")
    print(file_path)
    df = pd.read_csv(filepath_or_buffer=file_path,
                        sep=";",
                        header=None,
                        names=estab.get_columns(),
                        dtype=str,
                        encoding="IBM860", # encoding for Portuguese Language
                        chunksize=100_000)
    for chunk in df:
        my_queue = Queue()
        estab.process_file(chunk, my_queue)

        start = time.time()
        print("Start inserting into database...")
        threads = [estab.get_thread(my_queue) for _ in range(NUMBER_OF_THREADS)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        print(f"\n\nExecution of data insertion {round(time.time()-start,2)}s")
    # print(f"\n\nExecution of data insertion {round(time.time()-start,2)}s")

if __name__ == "__main__":
    with Pool() as pool:
        pool.map(process_and_insert, files_paths)


