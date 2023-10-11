from threading import Thread
from queue import Empty

class MyThread(Thread):
    def __init__(self, engine, my_queue, insert_script:str) -> None:
        Thread.__init__(self)
        self.my_queue = my_queue
        self.insert_script = insert_script
        self.engine = engine

    def run(self) -> None:
        with self.engine.connect() as conn:
            while True:
                try:
                    row = self.my_queue.get(block=False)
                except Empty:
                    return
                else:
                    conn.execute(self.insert_script, row)
                    conn.commit()
