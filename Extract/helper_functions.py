from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from zipfile import ZipFile
import os, wget

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