# alpine is a light version of Linux
FROM python:3.12-slim
LABEL maintainer="mickaelyoshua7674"

# copy files to image
COPY ./requirements.txt /tmp/requirements.txt

# the output of python will be printed directly on console
ENV PYTHONUNBUFFERED=1

    # update Debian
RUN apt-get update && apt-get upgrade && \
    # in order to psycopg2 connect to postgres the dependencie 'libpq-dev' must be installed
    apt-get install libpq-dev -y && \
    # install gcc
    apt-get install gcc -y && \
    # remove apt cache
    rm -rf /var/lib/apt/lists/* && \
    # upgrade pip
    python -m pip install --upgrade pip && \
    # install requirements
    python -m pip install -r /tmp/requirements.txt && \
    # remove temporary folder
    rm -rf /tmp 