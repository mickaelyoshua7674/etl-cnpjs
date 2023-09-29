from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from aiohttp import ClientSession
from typing import Coroutine, Generator
from io import BytesIO
from zipfile import ZipFile
import asyncio, os

URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
EXTRACT_DIR = "Extract"
UNZIPED_DIR = os.path.join(EXTRACT_DIR, "unzipedfiles")
if not os.path.exists(UNZIPED_DIR):
    os.mkdir(UNZIPED_DIR)

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

async def download_file(url:str):
    """
    Coroutine to download a single zip file from given url
    """
    file_name = url.split("/")[-1]
    async with ClientSession() as session: # start a session
        async with session.get(url) as response: # make get requests to url
            s = response.status
            if s == 200: # check if was successful
                with ZipFile(BytesIO(await response.read())) as zf:
                    for i in zf.infolist():
                        i.filename = file_name.split(".")[0] + ".csv" # rename file to extract
                        zf.extract(i, UNZIPED_DIR)
                print(f"{file_name} extracted.")
            else:
                print(f"Status code to file {file_name}: {s}")

async def download_all_files(download_file:Coroutine, urls:Generator):
    tasks = [download_file(url) for url in urls] # create tasks to all url downloads
    await asyncio.gather(*tasks) # schedule them

urls = (URL+zf for zf in get_zipfiles_names(URL))

print("Downloading all files simultaneously...")
asyncio.run(download_all_files(download_file, urls))
print("All finished.")