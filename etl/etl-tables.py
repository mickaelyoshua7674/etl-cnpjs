from sqlalchemy.types import INTEGER, VARCHAR
from models.BaseModel import BaseModel
from sqlalchemy import text
import pandas as pd

URL = "https://dadosabertos.rfb.gov.br/CNPJ/"

def create_insert_files(url:str, pk:str, conn) -> None:
    """
    Create table and insert data of small files.
    All tables have the following pattern:
                                        * table name -> id_<pk>
                                        * first column -> <pk> (Primary Key)
                                        * second column -> descricao
    """
    print(f"Creating and inserting on table id_{pk}...")
    df = pd.read_csv(url, header=None, encoding="latin-1", sep=";", dtype={0:int,1:str})
    df.columns = (pk, "descricao")
    len_descricao = int(df["descricao"].str.len().max()*1.2) # add a 20% margin
    df.to_sql(name=f"id_{pk}", con=conn, if_exists="replace", index=False, dtype={pk:INTEGER(), "descricao":VARCHAR(len_descricao)})
    conn.execute(text(f"ALTER TABLE id_{pk} ADD PRIMARY KEY ({pk});"))

def creat_insert_aditional_tables(pk:str, data:tuple[tuple], conn) -> None:
    """
    Create table and insert data of tables that are not among the downloaded files.
    All tables have the following pattern:
                                        * table name -> id_<pk>
                                        * first column -> <pk> (Primary Key)
                                        * second column -> descricao
    """
    print(f"Creating and inserting on table id_{pk}...")
    df = pd.DataFrame(columns=(pk,"descricao"), data=data)
    len_descricao = int(df["descricao"].str.len().max()*1.2) # add a 20% margin
    df.to_sql(name=f"id_{pk}", con=conn, if_exists="replace", index=False, dtype={pk:INTEGER(), "descricao":VARCHAR(len_descricao)})
    conn.execute(text(f"ALTER TABLE id_{pk} ADD PRIMARY KEY ({pk});"))

engine = BaseModel().engine
with engine.connect() as conn:
    create_insert_files(url=URL+"Cnaes.zip", pk="cnae", conn=conn) # null -> 8888888
    create_insert_files(url=URL+"Motivos.zip", pk="motivo_situacao_cadastral", conn=conn) # null -> 0
    create_insert_files(url=URL+"Municipios.zip", pk="municipio", conn=conn) # null -> inserted next (9999)
    conn.execute(text("INSERT INTO id_municipio VALUES (9999,'NÃO INFORMADO')")) # null -> 9999
    create_insert_files(url=URL+"Naturezas.zip", pk="natureza_juridica", conn=conn) # null -> 0
    create_insert_files(url=URL+"Paises.zip", pk="pais", conn=conn) # null -> 999
    create_insert_files(url=URL+"Qualificacoes.zip", pk="qualificacao", conn=conn) # null -> 0

    creat_insert_aditional_tables(pk="porte_empresa", data=((0, 'NAO INFORMADO'),
                                                            (1, 'MICRO EMPRESA'),
                                                            (3, 'EMPRESA DE PEQUENO PORTE'),
                                                            (5, 'DEMAIS')), conn=conn)
    creat_insert_aditional_tables(pk="identificador", data=((0, 'VAZIO'),
                                                            (1, 'MATRIZ'),
                                                            (2, 'FILIAL')), conn=conn)
    creat_insert_aditional_tables(pk="situacao_cadastral", data=((1, 'NULA'),
                                                                 (2, 'ATIVA'),
                                                                 (3, 'SUSPENSA'),
                                                                 (4, 'INAPTA'),
                                                                 (8, 'BAIXADA')), conn=conn)
    creat_insert_aditional_tables(pk="opcao_simples", data=((1, 'SIM'),
                                                            (0, 'NAO'),
                                                            (2, 'OUTROS')), conn=conn)
    creat_insert_aditional_tables(pk="opcao_mei", data=((1, 'SIM'),
                                                        (0, 'NAO'),
                                                        (2, 'OUTROS')), conn=conn)
    creat_insert_aditional_tables(pk="identificador_socio", data=((0, 'NENHUM'),
                                                                  (1, 'PESSOA JURIDICA'),
                                                                  (2, 'PESSOA FISICA'),
                                                                  (3, 'ESTRANGEIRO')), conn=conn)
    conn.commit()


#########################################################################################################################


from models.Estabelecimento import Estabelecimento
from multiprocessing import Pool, cpu_count
from models.Simples import Simples
from models.Empresa import Empresa
from models.Socio import Socio
from queue import Queue
from os import environ
from time import time

ZIPFILES = ("Empresas0.zip", "Empresas1.zip", "Empresas2.zip", "Empresas3.zip", "Empresas4.zip",
            "Empresas5.zip", "Empresas6.zip", "Empresas7.zip", "Empresas8.zip", "Empresas9.zip",
            "Estabelecimentos0.zip", "Estabelecimentos1.zip", "Estabelecimentos2.zip", "Estabelecimentos3.zip", "Estabelecimentos4.zip",
            "Estabelecimentos5.zip", "Estabelecimentos6.zip", "Estabelecimentos7.zip", "Estabelecimentos8.zip", "Estabelecimentos9.zip",
            "Simples.zip",
            "Socios0.zip", "Socios1.zip", "Socios2.zip", "Socios3.zip", "Socios4.zip",
            "Socios5.zip", "Socios6.zip", "Socios7.zip", "Socios8.zip", "Socios9.zip")
URLS = tuple((URL+zf for zf in ZIPFILES))
THREADS_NUMBER = int(environ["THREADS_NUMBER"]) # Warning -> the number of threads will be multiplied by the number of processes
PROCESSES_NUMBER = cpu_count() # using all cores availables (mine is 8)
CHUNKSIZE = int(environ["CHUNKSIZE"]) # Warning -> the size of chunk will be for each process

# Create objects of all table classes and create the DataBase table
estab = Estabelecimento()
estab.create_table()

socio = Socio()
socio.create_table()

empresa = Empresa()
empresa.create_table()

simples = Simples()
simples.create_table()

def process_and_insert(url:str) -> None:
    """
    Function to map all files in multiprocessing.Pool making the transformation and insertion of data into the DataBase.
    """
    filename = url.split("/")[-1]
    chunk_count = 0
    print(f"Processing {filename}...")
    # Switch to correct table
    obj = None
    if "Estabelecimento" in filename:
        obj = estab
    elif "Socio" in filename:
        obj = socio
    elif "Empresa" in filename:
        obj = empresa
    elif "Simples" in filename:
        obj = simples

    my_queue = Queue() # one queue to each process so the threads inside those processes can safely share data
    df = obj.get_reader_file(url, CHUNKSIZE)
    for chunk in df:
        obj.process_chunk(chunk, my_queue)
        # the threads will process one chunck at a time
        threads = [obj.get_thread(my_queue) for _ in range(THREADS_NUMBER)] # create objects of MyThread
        for thread in threads: # Start all threads
            thread.start()
        for thread in threads: # wait all to finish
            thread.join()
        chunk_count += 1
        print(f"\nChunk number {chunk_count} from {filename}\n")

if __name__ == "__main__":
    with Pool(processes=PROCESSES_NUMBER) as pool:
        start_all = time()
        pool.map(process_and_insert, URLS)
        exec_time_hr = ((time()-start_all)/60)/60
        print(f"\n\n############ Total time of execution {round(exec_time_hr,2)}hr ############\n\n")