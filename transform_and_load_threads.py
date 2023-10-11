from models.estabelecimento import Estabelecimento
from queue import Queue
import time

NUMBER_OF_THREADS = 10

estab = Estabelecimento()
insert_script = estab.get_insert_script()
my_queue = Queue()
file_name = "Estabelecimentos0.csv"
estab.process_file(file_name, my_queue)

start = time.time()
print("Start inserting into database...")
threads = [estab.get_thread(my_queue) for _ in range(NUMBER_OF_THREADS)]
estab.create_table()
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()

print(f"\n\nExecution of data insertion {round(time.time()-start,2)}s")



