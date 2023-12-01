from aiohttp import ClientSession
from __init__ import LARGE_ZIPFILES, SMALL_ZIPFILES
import aiofiles, asyncio, os
from time import time

async def download_file(session:ClientSession, url:str) -> None:
    """
    Coroutine to download a single zip file from given url
    """
    async with session.get(url, timeout=None) as response: # make get requests to url / deactivate timeout
        file_name = url.split("/")[-1]
        print(f"Downloading file {file_name}...")
        async with aiofiles.open(os.path.join(FILES_FOLDER,file_name), mode="ab") as f:
            async for data, _ in response.content.iter_chunks():
                await f.write(data)
    print(f"{file_name} downloaded.\n")

async def download_all_files() -> None:
    async with ClientSession() as session: # start a session
        tasks = [asyncio.create_task(download_file(session, url)) for url in urls] # create tasks to all url downloads
        await asyncio.gather(*tasks) # schedule them

if __name__ == "__main__":
    URL = os.environ["URL"]
    FILES_FOLDER = os.environ["FILES_FOLDER"]
    if not os.path.exists(FILES_FOLDER):
        os.mkdir(FILES_FOLDER)
    urls = (URL+zf for zf in filter(lambda x: x not in os.listdir(FILES_FOLDER),LARGE_ZIPFILES+SMALL_ZIPFILES))

    print("Downloading all files simultaneously...\n")
    start = time()
    asyncio.run(download_all_files())
    exec_time_s = time()-start
    exec_time_min = exec_time_s/60.
    exec_time_hr = exec_time_min/60.
    print(f"All finished.\nExecution time: {round(exec_time_s,2)}s / {round(exec_time_min,2)}min / {round(exec_time_hr,2)}hr")
    # giving this error -> aiohttp.client_exceptions.ClientPayloadError: Response payload is not completed