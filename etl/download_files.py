from aiohttp import ClientSession
from __init__ import ZIPFILES
from time import time
import aiofiles
import asyncio
import os

URL = os.environ["URL"]
FILES_DIR = "Files"
if not os.path.exists(FILES_DIR):
    os.mkdir(FILES_DIR)

async def download_file(session:ClientSession, url:str) -> None:
    """
    Coroutine to download a single zip file from given url
    """
    async with session.get(url, timeout=None) as response: # make get requests to url / deactivate timeout
        file_name = url.split("/")[-1]
        print(f"Downloading file {file_name}...")
        async with aiofiles.open(os.path.join(FILES_DIR,file_name), mode="ab") as f:
            async for data, _ in response.content.iter_chunks():
                await f.write(data)
    print(f"{file_name} downloaded.\n")

urls = (URL+zf for zf in ZIPFILES)
async def download_all_files() -> None:
    async with ClientSession() as session: # start a session
        tasks = [asyncio.create_task(download_file(session, url)) for url in urls] # create tasks to all url downloads
        await asyncio.gather(*tasks) # schedule them

print("Downloading all files simultaneously...\n")
start = time()
asyncio.run(download_all_files())
exec_time_s = start-time()
exec_time_min = exec_time_s/60
exec_time_hr = exec_time_min/60
print(f"All finished.\nExecution time: {exec_time_s}s / {exec_time_min}min / {exec_time_hr}hr")