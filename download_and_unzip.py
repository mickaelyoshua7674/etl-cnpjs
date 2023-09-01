from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from zipfile import ZipFile
import wget, os, shutil

CHROMEDRIVER_PATH = "/usr/lib/chromium-browser/chromedriver"
URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
ZIPFILES_DIR = "zipfiles"
if not os.path.exists(ZIPFILES_DIR):
    os.mkdir(ZIPFILES_DIR)

RAWFILES_DIR = "rawfiles"
if not os.path.exists(RAWFILES_DIR):
    os.mkdir(RAWFILES_DIR)

op = webdriver.ChromeOptions()
op.add_argument("log-level=3") # https://stackoverflow.com/questions/46744968/how-to-suppress-console-error-warning-info-messages-when-executing-selenium-pyth
op.add_argument("headless") # don't open a Chrome window
sc = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=sc, options=op)

driver.get(URL)
tr_elements = [e for e in driver.find_elements(By.TAG_NAME, "tr")]
zipfiles_list =  [e.text.split(" ")[0] for e in tr_elements[2:] if ".zip" in e.text]
driver.quit()

for zf in zipfiles_list:
    full_zip_path = os.path.join(ZIPFILES_DIR, zf)

    print(f"Downloading {zf}...")
    wget.download(URL + zf, ZIPFILES_DIR)
    print("\n\n")

    print(f"Extracting {zf}...")
    with ZipFile(full_zip_path, "r") as z:
        for i in z.infolist():
            i.filename = zf.split(".")[0] + ".csv"
            z.extract(i, RAWFILES_DIR)
shutil.rmtree(ZIPFILES_DIR)