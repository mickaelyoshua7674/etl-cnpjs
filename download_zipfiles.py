import wget
from os.path import exists
from os import mkdir

ZIPFILES_DIR = "zipfiles/"
URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
URL_T = "https://dadosabertos.rfb.gov.br/CNPJ/Empresas1.zip"

if not exists(ZIPFILES_DIR):
    mkdir(ZIPFILES_DIR)

wget.download(URL_T, ZIPFILES_DIR)