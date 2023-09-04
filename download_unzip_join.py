from helper_functions import *

# download_and_unzip()

MERGED_DIR = "mergedfiles"
if not os.path.exists(MERGED_DIR):
    os.mkdir(MERGED_DIR)

with open("table_file.json", "r") as f:
    table_file = json.load(f)
with open("all_dtypes.json", "r") as f:
    dtypes = json.load(f)

for tf in table_file:
    number_files = len([f for f in os.listdir(RAWFILES_DIR) if f.startswith(tf[1])])
    if number_files > 1:
        header = True
        for i in range(number_files):
            file = tf[1]+str(i)+".csv"
            columns = [k for k in dtypes[file].keys()]
            clean_concat_data(RAWFILES_DIR, file, columns, MERGED_DIR, tf[0], header)
            print()
    else:
        file = tf[1]+".csv"
        columns = [k for k in dtypes[file].keys()]
        header = True
        clean_concat_data(RAWFILES_DIR, file, columns, MERGED_DIR, tf[0], header)
        print()
os.rmdir(RAWFILES_DIR)