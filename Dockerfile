# alpine is a light version of Linux
FROM python:3.12-alpine3.18
LABEL maintainer="mickaelyoshua7674"

# the output of python will be printed directly on console
ENV PYTHONUNBUFERRED 1

# copy files to image
COPY ./requirements.txt /tmp/requirements.txt
COPY ./etl /etl

# change working directory
WORKDIR /etl

    # best practice not use the root user
RUN adduser \
        # no password necessary
        --disabled-password \
        # user name
        etl-user

# Adding folder of created user where pip will install packages to PATH
ENV PATH="/home/etl-user/.local/bin:$PATH"

    # upgrade pip
RUN python -m pip install --upgrade pip && \
    # install requirements
    python -m pip install -r /tmp/requirements.txt && \
    # remove temporary folder
    rm -rf /tmp

# change to created user
USER etl-user