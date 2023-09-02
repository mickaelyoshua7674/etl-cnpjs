from typing import List, Iterable
import os, traceback
import pandas as pd

def clean_concat_data(files_dir: str, file_name: str, dtypes: dict, save_files_dir: str, save_file_name: str, header: bool, i=0):
    lines = get_lines_iterator(files_dir, file_name)
    for j, line in enumerate(lines):
        try:
            concat_data(header, line, dtypes, save_files_dir, save_file_name)
            header = False
        except:
            print(f"\n\n{i}\n{j+1}\n{line}\n\n")
            traceback.print_exc()
            exit()

def get_lines_iterator(files_dir: str, file_name: str) -> Iterable:
    with open(os.path.join(files_dir, file_name), "r", encoding="latin-1") as f:
        return f.readlines()
    
def concat_data(header: bool, line: str, dtypes: dict, save_dir: str, file: str) -> None:
    pd.DataFrame(data=[clean_line(line)], columns=[k for k in dtypes[file].keys()])\
    .to_csv(os.path.join(save_dir, file+".csv"), header=header, index=False, mode="a")

def clean_line(line: str) -> List[str]:
    return [l.replace('"', "")for l in line.replace("\n", "").split('";"')]