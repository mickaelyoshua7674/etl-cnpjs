from typing import List, Iterable
from tqdm import tqdm
import pandas as pd
import pickle as pk
import os

LAST_LINE_PATH = "last_line.pkl"
REMAINING_FILE_PATH = "last_file.pkl"

def clean_concat_data(files_dir: str, file_name: str, dtypes: dict, save_files_dir: str, save_file_name: str, header: bool):
    last_line = get_last_line()
    if last_line > 0:
        header = False
    remaining_files = get_remaining_files(files_dir)

    if file_name == remaining_files[0]:
        lines = get_lines_iterator(files_dir, file_name)
        for i, line in enumerate(tqdm(lines, desc=file_name, unit=" lines")):
            if i > last_line:
                concat_data(header, line, dtypes, save_files_dir, save_file_name)
                header = False
                save_last_line(i)
        remaining_files.remove(file_name)
        save_remaining_files(remaining_files)
        os.remove(LAST_LINE_PATH)

def get_lines_iterator(files_dir: str, file_name: str) -> Iterable:
    with open(os.path.join(files_dir, file_name), "r", encoding="latin-1") as f:
        return f.readlines()
    
def concat_data(header: bool, line: str, dtypes: dict, save_dir: str, file: str) -> None:
    pd.DataFrame(data=[clean_line(line)], columns=[k for k in dtypes[file].keys()])\
    .to_csv(os.path.join(save_dir, file+".csv"), header=header, index=False, mode="a")

def clean_line(line: str) -> List[str]:
    return [l.replace('"', "")for l in line.replace("\n", "").split('";"')]

def save_last_line(i: int) -> None:
    with open(LAST_LINE_PATH, "wb") as f:
        pk.dump(i, f)

def get_last_line() -> int:
    if os.path.exists(LAST_LINE_PATH):
        with open(LAST_LINE_PATH, "rb") as f:
            return pk.load(f)
    return -1

def save_remaining_files(remaining_files: List[str]) -> None:
    with open(REMAINING_FILE_PATH, "wb") as f:
        pk.dump(remaining_files, f)

def get_remaining_files(files_dir: str) -> List[str]:
    if os.path.exists(REMAINING_FILE_PATH):
        with open(REMAINING_FILE_PATH, "rb") as f:
            return pk.load(f)
    return sorted(os.listdir(files_dir))