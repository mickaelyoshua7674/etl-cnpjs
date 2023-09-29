from helper_functions import *
from aiohttp import ClientSession
from typing import Coroutine
import asyncio

URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
EXTRACT_DIR = "Extract"
ZIPFILES_DIR = os.path.join(EXTRACT_DIR, "zipfiles")

print("Getting list of downloaded zipfile...")
if not os.path.exists(ZIPFILES_DIR):
    os.mkdir(ZIPFILES_DIR)

async def download_file(url:str):
    """
    Coroutine to download a single zip file from given url
    """
    full_file_path = os.path.join(ZIPFILES_DIR,url.split("/")[-1])
    async with ClientSession() as session: # start a session
        async with session.get(url) as response: # make get requests to url
            s = response.status
            if s == 200: # check if was successful
                with open(full_file_path, "wb") as f: # write binary content into zip file
                    f.write(await response.read())
            else:
                print(f"Status code to file {full_file_path}: {s}")

async def download_all_files(download_file:Coroutine, urls:list[str]):
    tasks = [download_file(url) for url in urls]
    await asyncio.gather(*tasks)

urls = [URL+zf for zf in get_zipfiles_names(URL)]

print("Downloading all files simultaneously...")
asyncio.run(download_all_files(download_file, urls))



























# UNZIPED_DIR = os.path.join(EXTRACT_DIR, "unzipedfiles")

# from zipfile import ZipFile
# async def unzip_file(file_name, zipfiles_dir, unziped_files_dir):
#     full_zip_path = os.path.join(zipfiles_dir, file_name)
#     print(f"Extracting {file_name}...")
#     with ZipFile(full_zip_path, "r") as z:
#         for i in z.infolist():
#             i.filename = file_name.split(".")[0] + ".csv"
#             z.extract(i, unziped_files_dir)
#     print(f"{file_name} Extracted.\n\n")
#     os.remove(full_zip_path)






# os.rmdir(ZIPFILES_DIR)