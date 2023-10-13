from threading import Thread
from queue import Empty

class MyThread(Thread):
    """
    Class inheriting from threading.Thread class
    """
    def __init__(self, engine, my_queue, insert_script:str) -> None:
        """
        Define the shared queue, the insert script and the engine to connect and insert into DataBase
        """
        Thread.__init__(self)
        self.my_queue = my_queue # shared memory between threads
        self.insert_script = insert_script # should already passed through the 'text()' function from sqlalchemy
        self.engine = engine

    def run(self) -> None:
        """
        When 'Thread.start' is called, also call the 'run()' method.
        Here we are overwriting the method to create a connection to the DataBase and insert values
        """
        with self.engine.connect() as conn:
            while True:
                try:
                    # when '.get()' is called retuns a value and delete that value from the queue
                    row = self.my_queue.get(block=False) # 'block=False' allows to check the Empty exception
                except Empty: # if there is not values left inside the queue
                    return # end the thread excecution
                else:
                    conn.execute(self.insert_script, row)
                    conn.commit()
