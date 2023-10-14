from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from aiohttp import ClientSession
from zipfile import ZipFile
from io import BytesIO
from time import time
import asyncio, os

URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
FILES_DIR = "Files"
if not os.path.exists(FILES_DIR):
    os.mkdir(FILES_DIR)

def get_zipfiles_names(url: str) -> list[str]:
    """
    Go to the given 'url' then search for TAG_NAME 'tr' and return all .zip files founded.
    """
    op = webdriver.ChromeOptions()
    op.add_argument("log-level=3") # https://stackoverflow.com/questions/46744968/how-to-suppress-console-error-warning-info-messages-when-executing-selenium-pyth
    op.add_argument("headless") # don't open a Chrome window
    sc = Service("chromedriver.exe")
    driver = webdriver.Chrome(service=sc, options=op)

    print(f"Going to {url}...")
    driver.get(url)
    print("Getting list of zipfiles...")
    tr_elements = [e for e in driver.find_elements(By.TAG_NAME, "tr")]
    zipfiles_list = [e.text.split(" ")[0] for e in tr_elements if ".zip" in e.text]
    print("List collected.\n")
    driver.quit()
    return zipfiles_list

def unzip(b:bytearray, file_name:str) -> None:
    """
    Save csv file from zipfile's bytes an rename with given file_name
    """
    with ZipFile(BytesIO(b)) as zf:
        for i in zf.infolist():
            i.filename = file_name.split(".")[0]+".csv" # rename file to extract
            zf.extract(i, FILES_DIR)

async def download_file(session:ClientSession, url:str) -> None:
    """
    Coroutine to download a single zip file from given url
    """
    async with session.get(url, timeout=None) as response: # make get requests to url / deactivate timeout
        file_name = url.split("/")[-1]
        full_content = bytearray()
        async for data, _ in response.content.iter_chunks():
            full_content += data
        unzip(full_content, file_name)
        print(f"{file_name} extracted.")

urls = (URL+zf for zf in get_zipfiles_names(URL))
async def download_all_files() -> None:
    async with ClientSession() as session: # start a session
        tasks = [asyncio.create_task(download_file(session, url)) for url in urls] # create tasks to all url downloads
        await asyncio.gather(*tasks) # schedule them

print("Downloading all files simultaneously...")
start = time()
asyncio.run(download_all_files())
end = time()
print(f"All finished.\nExecution time: {round(end-start,2)}s / {round((end-start)/60,2)}min / {round(((end-start)/60)/60,2)}hr")
# Execution time -> 30min