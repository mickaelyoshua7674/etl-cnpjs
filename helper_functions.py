from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from typing import List, Iterable
from zipfile import ZipFile
from tqdm import tqdm
import pandas as pd
import pickle as pk
import os, json
import wget

CHROMEDRIVER_PATH = "/usr/lib/chromium-browser/chromedriver"
URL = "https://dadosabertos.rfb.gov.br/CNPJ/"

RAWFILES_DIR = "rawfiles"
ZIPFILES_DIR = "zipfiles"

LAST_LINE_PATH = "last_line.pkl"
REMAINING_FILE_PATH = "last_file.pkl"
ERROR_LINE_FILE = "error_line_file.json"

def get_zipfiles_names() -> List[str]:
    op = webdriver.ChromeOptions()
    op.add_argument("log-level=3") # https://stackoverflow.com/questions/46744968/how-to-suppress-console-error-warning-info-messages-when-executing-selenium-pyth
    op.add_argument("headless") # don't open a Chrome window
    sc = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=sc, options=op)

    driver.get(URL)
    tr_elements = [e for e in driver.find_elements(By.TAG_NAME, "tr")]
    zipfiles_list = [e.text.split(" ")[0] for e in tr_elements[2:] if ".zip" in e.text]
    driver.quit()
    return zipfiles_list

def download_and_unzip():
    if not os.path.exists(RAWFILES_DIR):
        os.mkdir(RAWFILES_DIR)
    if not os.path.exists(ZIPFILES_DIR):
        os.mkdir(ZIPFILES_DIR)

    for zf in get_zipfiles_names():
        full_zip_path = os.path.join(ZIPFILES_DIR, zf)

        print(f"Downloading {zf}...")
        wget.download(URL + zf, ZIPFILES_DIR)
        print("\n\n")

        print(f"Extracting {zf}...")
        with ZipFile(full_zip_path, "r") as z:
            for i in z.infolist():
                i.filename = zf.split(".")[0] + ".csv"
                z.extract(i, RAWFILES_DIR)
        print(f"{zf} Extracted.\n\n")
        os.remove(full_zip_path)
    os.rmdir(ZIPFILES_DIR)


def clean_concat_data(files_dir: str, file_name: str, columns: List[str], save_files_dir: str, save_file_name: str, header: bool):
    if not os.path.exists(ERROR_LINE_FILE):
        with open(ERROR_LINE_FILE, "w") as f:
            json.dump([], f)

    last_line = get_last_line()
    if last_line > 0:
        header = False
    remaining_files = get_remaining_files(files_dir)

    if file_name == remaining_files[0]:
        lines = get_lines_iterator(files_dir, file_name)
        for i, line in enumerate(tqdm(lines, desc=file_name, unit=" lines")):
            if i > last_line:
                try:
                    concat_data(header, line, columns, save_files_dir, save_file_name)
                except:
                    save_error(i, file_name)
                header = False
                save_last_line(i)
        remaining_files.remove(file_name)
        save_remaining_files(remaining_files)
        os.remove(os.path.join(files_dir, file_name))
        os.remove(LAST_LINE_PATH)

def get_lines_iterator(files_dir: str, file_name: str) -> Iterable:
    with open(os.path.join(files_dir, file_name), "r", encoding="latin-1") as f:
        return f.readlines()
    
def concat_data(header: bool, line: str, columns: List[str], save_dir: str, file: str) -> None:
    pd.DataFrame(data=[clean_line(line)], columns=columns)\
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

def save_error(i: int, file: str) -> None:
    with open(ERROR_LINE_FILE, "r") as f:
        errors = json.load(f)
    errors.append((i,file))
    with open(ERROR_LINE_FILE, "w") as f:
        json.dump(errors, f)