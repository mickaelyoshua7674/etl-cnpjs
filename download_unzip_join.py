from helper_functions import *

# download_and_unzip()

MERGED_DIR = "mergedfiles"
if not os.path.exists(MERGED_DIR):
    os.mkdir(MERGED_DIR)

with open("file_table.json", "r") as f:
    file_table = json.load(f)
with open("all_dtypes.json", "r") as f:
    dtypes = json.load(f)

remaining_files = get_remaining_files(RAWFILES_DIR)

for file in remaining_files:
    table_name = file_table[get_core_file_name(file)]
    columns = [k for k in dtypes[table_name].keys()]

    header = True
    clean_concat_data(RAWFILES_DIR, file, columns, MERGED_DIR, table_name, header)
    print()

os.rmdir(RAWFILES_DIR)