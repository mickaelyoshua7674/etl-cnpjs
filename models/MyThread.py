from threading import Thread
from queue import Queue, Empty

class MyThread(Thread):
    def __init__(self, table_obj, my_queue:Queue) -> None:
        Thread.__init__(self)
        self.my_queue = my_queue
        self.table_obj = table_obj
        self.insert_script = table_obj.get_insert_script()

    def run(self) -> None:
        with self.table_obj.engine.connect() as conn:
            while True:
                try:
                    row = self.my_queue.get(block=False)
                except Empty:
                    print("Queue is empty.\n Finished.")
                    return
                else:
                    conn.execute(self.insert_script, row)
                    conn.commit()
                    break
