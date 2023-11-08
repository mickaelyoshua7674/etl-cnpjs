from subprocess import call
from sys import executable
# install all requirements
call([executable,"-m","pip","install","-r","requirements.txt"])