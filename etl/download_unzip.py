from aiohttp import ClientSession
from zipfile import ZipFile
from io import BytesIO
from time import time
import asyncio, os

URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
ZIPFILES = ("Cnaes.zip", "Empresas0.zip", "Empresas1.zip", "Empresas2.zip", "Empresas3.zip",
            "Empresas4.zip", "Empresas5.zip", "Empresas6.zip", "Empresas7.zip", "Empresas8.zip",
            "Empresas9.zip", "Estabelecimentos0.zip", "Estabelecimentos1.zip", "Estabelecimentos2.zip",
            "Estabelecimentos3.zip", "Estabelecimentos4.zip", "Estabelecimentos5.zip",
            "Estabelecimentos6.zip", "Estabelecimentos7.zip", "Estabelecimentos8.zip",
            "Estabelecimentos9.zip", "Motivos.zip", "Municipios.zip", "Naturezas.zip",
            "Paises.zip", "Qualificacoes.zip", "Simples.zip", "Socios0.zip", "Socios1.zip",
            "Socios2.zip", "Socios3.zip", "Socios4.zip", "Socios5.zip", "Socios6.zip",
            "Socios7.zip", "Socios8.zip", "Socios9.zip")
URLS = (URL+zf for zf in ZIPFILES)
FILES_DIR = "Files"
if not os.path.exists(FILES_DIR):
    os.mkdir(FILES_DIR)

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
        async for data, _ in response.content.iter_chunks(): # iterate on content
            full_content += data
        unzip(full_content, file_name)
        print(f"{file_name} extracted.")

async def download_all_files() -> None:
    async with ClientSession() as session: # start a session
        tasks = [asyncio.create_task(download_file(session, url)) for url in URLS] # create tasks to all url downloads
        await asyncio.gather(*tasks) # schedule them

print("Downloading all files simultaneously...")
start = time()
asyncio.run(download_all_files())
end = time()
print(f"All finished.\nExecution time: {round(end-start,2)}s / {round((end-start)/60,2)}min / {round(((end-start)/60)/60,2)}hr")
# Execution time -> 30min