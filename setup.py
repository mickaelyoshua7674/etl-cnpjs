from dotenv import load_dotenv
load_dotenv()

from subprocess import call
from sys import executable
call([executable,"-m","pip","install","-r","requirements.txt"])