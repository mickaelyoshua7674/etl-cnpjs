import asyncio, os, wget
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from typing import Coroutine
from zipfile import ZipFile

def get_zipfiles_names(url: str) -> list[str]:
    """
    Go to the given 'url' then search for TAG_NAME 'tr' and return all .zip files founded.
    """
    op = webdriver.ChromeOptions()
    op.add_argument("log-level=3") # https://stackoverflow.com/questions/46744968/how-to-suppress-console-error-warning-info-messages-when-executing-selenium-pyth
    op.add_argument("headless") # don't open a Chrome window
    sc = Service("/usr/lib/chromium-browser/chromedriver")
    driver = webdriver.Chrome(service=sc, options=op)

    print(f"Going to {url}...")
    driver.get(url)
    print("Getting list of zipfiles...")
    tr_elements = [e for e in driver.find_elements(By.TAG_NAME, "tr")]
    zipfiles_list = [e.text.split(" ")[0] for e in tr_elements if ".zip" in e.text]
    print("List collected.\n")
    driver.quit()
    return zipfiles_list

URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
EXTRACT_DIR = "Extract"
ZIPFILES_DIR = os.path.join(EXTRACT_DIR, "zipfiles")
UNZIPED_DIR = os.path.join(EXTRACT_DIR, "unzipedfiles")

print("Getting list of downloaded zipfile...")
if not os.path.exists(ZIPFILES_DIR):
    os.mkdir(ZIPFILES_DIR)

downloaded_files = os.listdir(ZIPFILES_DIR)
print("List collected.\n")

async def download_file(url: str, file_name: str, zipfiles_dir: str):
    print(f"Downloading {file_name}...")
    wget.download(url + file_name, zipfiles_dir)
    print("\n\n")

async def unzip_file(file_name, zipfiles_dir, unziped_files_dir):
    full_zip_path = os.path.join(zipfiles_dir, file_name)
    print(f"Extracting {file_name}...")
    with ZipFile(full_zip_path, "r") as z:
        for i in z.infolist():
            i.filename = file_name.split(".")[0] + ".csv"
            z.extract(i, unziped_files_dir)
    print(f"{file_name} Extracted.\n\n")
    os.remove(full_zip_path)



for zf in (f for f in get_zipfiles_names(URL) if f not in downloaded_files):
    download_file(URL, zf, ZIPFILES_DIR)













# os.rmdir(ZIPFILES_DIR)