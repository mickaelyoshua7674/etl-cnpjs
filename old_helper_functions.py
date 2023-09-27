from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from zipfile import ZipFile
from typing import Iterable
from tqdm import tqdm
import pandas as pd
import pickle as pk
import os, json, wget, re

RAWFILES_DIR = "rawfiles"
ZIPFILES_DIR = "zipfiles"

LAST_LINE_PATH = "last_line.pkl"
REMAINING_FILE_PATH = "last_file.pkl"
ERROR_LINE_FILE = "error_line_file.json"

def get_zipfiles_names() -> list[str]:
    op = webdriver.ChromeOptions()
    op.add_argument("log-level=3") # https://stackoverflow.com/questions/46744968/how-to-suppress-console-error-warning-info-messages-when-executing-selenium-pyth
    op.add_argument("headless") # don't open a Chrome window
    sc = Service("/usr/lib/chromium-browser/chromedriver")
    driver = webdriver.Chrome(service=sc, options=op)

    driver.get("https://dadosabertos.rfb.gov.br/CNPJ/")
    tr_elements = [e for e in driver.find_elements(By.TAG_NAME, "tr")]
    zipfiles_list = [e.text.split(" ")[0] for e in tr_elements[2:] if ".zip" in e.text]
    driver.quit()
    return zipfiles_list

def download_and_unzip() -> None:
    if not os.path.exists(RAWFILES_DIR):
        os.mkdir(RAWFILES_DIR)
    if not os.path.exists(ZIPFILES_DIR):
        os.mkdir(ZIPFILES_DIR)

    for zf in get_zipfiles_names():
        full_zip_path = os.path.join(ZIPFILES_DIR, zf)

        print(f"Downloading {zf}...")
        wget.download("https://dadosabertos.rfb.gov.br/CNPJ/" + zf, ZIPFILES_DIR)
        print("\n\n")

        print(f"Extracting {zf}...")
        with ZipFile(full_zip_path, "r") as z:
            for i in z.infolist():
                i.filename = zf.split(".")[0] + ".csv"
                z.extract(i, RAWFILES_DIR)
        print(f"{zf} Extracted.\n\n")
        os.remove(full_zip_path)
    os.rmdir(ZIPFILES_DIR)


def clean_concat_data(files_dir: str, file_name: str, columns: list[str], save_files_dir: str, save_file_name: str, header: bool):
    if not os.path.exists(ERROR_LINE_FILE):
        with open(ERROR_LINE_FILE, "w") as f:
            json.dump([], f)

    last_line = get_last_line()
    if last_line > 0:
        header = False

    save_file_path = os.path.join(save_files_dir, save_file_name+".csv")

    lines = get_lines_iterator(files_dir, file_name)
    for i, line in enumerate(tqdm(lines, desc=file_name, unit=" lines")):
        if i > last_line:
            try:
                concat_data(header, line, columns, save_file_path)
            except:
                save_error(i, file_name)
            header = False
            save_last_line(i)
    os.remove(os.path.join(files_dir, file_name))
    os.remove(LAST_LINE_PATH)

def get_lines_iterator(files_dir: str, file_name: str) -> Iterable:
    with open(os.path.join(files_dir, file_name), "r", encoding="latin-1") as f:
        return f.readlines()
    
def concat_data(header: bool, line: str, columns: list[str], save_file_path: str) -> None:
    pd.DataFrame(data=[clean_line(line)], columns=columns)\
    .to_csv(save_file_path, header=header, index=False, mode="a")

def clean_line(line: str) -> list[str]:
    return [l.replace('"', "")for l in line.replace("\n", "").split('";"')]

def save_last_line(i: int) -> None:
    with open(LAST_LINE_PATH, "wb") as f:
        pk.dump(i, f)

def get_last_line() -> int:
    if os.path.exists(LAST_LINE_PATH):
        with open(LAST_LINE_PATH, "rb") as f:
            return pk.load(f)
    return -1

def save_error(i: int, file: str) -> None:
    with open(ERROR_LINE_FILE, "r") as f:
        errors = json.load(f)
    errors.append((i,file))
    with open(ERROR_LINE_FILE, "w") as f:
        json.dump(errors, f)

def get_core_file_name(file: str) -> str:
    return re.sub("[0-9]", "", file.split(".")[0])

def clean_chars_fn(values: list[str]) -> list[str]:
    values.apply(lambda x: re.sub(r"['%\(\):]", "", x))