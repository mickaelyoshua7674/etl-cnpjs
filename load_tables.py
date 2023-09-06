from helper_load import *
import pandas as pd
import json

TRANSFORMED_PATH = "transformedfiles"

with open("all_dtypes.json", "r") as f:
    all_dtypes = json.load(f)

with open("file_table.json", "r") as f:
    tables = [i for _, i in json.load(f).items()]

with open("./secrets.txt", "r") as f:
    driver, username, password, host, port, database = f.read().split(",")
engine = get_engine_database(driver, username, password, host, port, database)

remaining_files = get_files_list(TRANSFORMED_PATH)

for f in tables:
    f_csv = f+".csv"
    if f_csv in remaining_files:
        file = os.path.join(TRANSFORMED_PATH, f_csv)
        df = pd.read_csv(file, dtype=all_dtypes[f], chunksize=100_000)
        insert_into_table(engine, df, f)
        os.remove(file)
os.rmdir(TRANSFORMED_PATH)