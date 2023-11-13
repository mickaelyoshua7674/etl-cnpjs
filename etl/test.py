from multiprocessing import Pool, cpu_count
from threading import Thread
from time import sleep

class MyThread(Thread):
    """
    Class inheriting from threading.Thread class
    """
    def __init__(self, name:str) -> None:
        Thread.__init__(self)
        self.name = name
        
    def run(self) -> None:
        print(f"Hi from {self.name}")

def run_threads():
    threads = [MyThread(f"{i}") for i in range(4)]

    for thread in threads: # Start all threads
        thread.start()
    for thread in threads: # wait all to finish
        thread.join()


def main(num:str):
    print(f"Process num {num}")
    run_threads()
    sleep(2)

if __name__ == "__main__":
    with Pool() as pool:
        pool.map(main, [f"{i}" for i in range(16)])