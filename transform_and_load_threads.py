from models.estabelecimento import Estabelecimento
import multiprocessing as mp
from queue import Queue
from glob import glob
import time

NUMBER_OF_THREADS = 3
# files_paths = glob("Files/Estabelecimentos*.csv") + glob("Files/Socios*.csv") + glob("Files/Empresas*.csv") + ["Files\\Simples.csv"]
files_paths = glob("Files/Estabelecimentos*.csv")
estab = Estabelecimento()
estab.create_table()

def process_and_insert(file_path) -> None:
    print("Start process...")
    print(file_path)
    my_queue = Queue()
    estab.process_file(file_path, my_queue)

    start = time.time()
    print("Start inserting into database...")
    threads = [estab.get_thread(my_queue) for _ in range(NUMBER_OF_THREADS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    print(f"\n\nExecution of data insertion {round(time.time()-start,2)}s")

if __name__ == "__main__":
    with mp.Pool() as pool:
        pool.map(process_and_insert, files_paths)


