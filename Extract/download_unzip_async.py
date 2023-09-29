from helper_functions import *
from aiohttp import ClientSession
from typing import Coroutine
from io import BytesIO
from zipfile import ZipFile
import asyncio

URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
EXTRACT_DIR = "Extract"
UNZIPED_DIR = os.path.join(EXTRACT_DIR, "unzipedfiles")

if not os.path.exists(UNZIPED_DIR):
    os.mkdir(UNZIPED_DIR)

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

async def download_all_files(download_file:Coroutine, urls:list[str]):
    tasks = [download_file(url) for url in urls] # create tasks to all url downloads
    await asyncio.gather(*tasks) # schedule them

urls = [URL+zf for zf in get_zipfiles_names(URL)]

print("Downloading all files simultaneously...")
asyncio.run(download_all_files(download_file, urls))
print("All finished.")