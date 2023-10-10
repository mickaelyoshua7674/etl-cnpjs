from threading import Thread
import pandas

class MyThread(Thread):
    def __init__(self, table_obj, df:pandas.DataFrame) -> None:
        Thread.__init__(self)
        self.df = df
        self.table_obj = table_obj
        self.insert_script = table_obj.get_insert_script()

    def run(self) -> None:
        with self.table_obj.engine.connect() as conn:
            for row in self.df.itertuples(index=False):
                conn.execute(self.insert_script, self.table_obj.row_to_dict(row))
                conn.commit()
                break