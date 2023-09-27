from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from zipfile import ZipFile
import os, wget

def _get_zipfiles_names(url: str) -> list[str]:
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

def download_zipflies(url: str, zipfiles_dir: str) -> None:
    """
    Create folder for zipfiles if don't exists.
    Get list of downloaded zipfiles to download only those left.
    Download zipfiles into folder created.
    """
    print("Getting list of downloaded zipfile...")
    if not os.path.exists(zipfiles_dir):
        os.mkdir(zipfiles_dir)

    downloaded_files = os.listdir(zipfiles_dir)
    print("List collected.\n")
    for zf in (f for f in _get_zipfiles_names(url) if f not in downloaded_files):
        print(f"Downloading {zf}...")
        wget.download(url + zf, zipfiles_dir)
        print("\n\n")

def unzip_files(zipfiles_dir: str, unziped_files_fir: str) -> None:
    """
    Create folder for unziped files if don't exists.
    Unzip files into folder created and delete the zipfile extracted.
    """
    if not os.path.exists(unziped_files_fir):
        os.mkdir(unziped_files_fir)

    for zf in os.listdir(zipfiles_dir):
        full_zip_path = os.path.join(zipfiles_dir, zf)
        print(f"Extracting {zf}...")
        with ZipFile(full_zip_path, "r") as z:
            for i in z.infolist():
                i.filename = zf.split(".")[0] + ".csv"
                z.extract(i, unziped_files_fir)
        print(f"{zf} Extracted.\n\n")
        os.remove(full_zip_path)