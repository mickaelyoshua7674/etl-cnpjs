from subprocess import call
from sys import executable
# install all requirements
call([executable,"-m","pip","install","-r","requirements.txt"])

from dotenv import load_dotenv
load_dotenv() # creatre the environment variable in .env file