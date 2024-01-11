# Documentation
## Description
This project is an automatic DataBase builder. Create a PostgreSQL DataBase and load data into it using data from Brazil's Receita Federal. The data used to ETL (Extract, Transform and Load) are the information about Brazilian companies around where the Governament let available.

## Tools
* Docker;
* Python 3.12;
* PostgreSQL.

## Structure Overview
Here we have two Docker containers, one is the PostgreSQL instance and the other is the Python container thais is going to create the DataBase and load data into it. They're manage on a [docker-compose file](docker-compose.yml).

The Python image for ETL is created from the [Dockerfile](Dockerfile) where is cofigured for the PostgreSQL connection and the Python enviroment.

## Algorithm Overview
The main Python scripts are the [download files](etl/download_files.py) using Asynchronous I/O, the [ETL small tables](etl/etl_small_tables.py) using Synchronous programming and the [ETL big tables](etl/etl_big_tables.py) using Processes and Threads.

The OOP (Object Oriented Programming) is the part where each big table have its own class inheriting from the same [Base Model](etl/models/BaseModel.py).

The Tests are used to test the DataBase connection, creation and ETL, also to test the [Base Model](etl/models/BaseModel.py).

## How to use
Install Docker on your machine them clone this repository into a folder. Open a terminal window, go to this repository cloned folder and execute the command:

    docker compose up

Then, on another terminal, execute the command:

    docker container ls

Using the first 3 characters from the ID of Python's container execute the command:

    docker exec -it <id> bash

After opening the container's terminal, execute the python scripts:

    python download_files.py && python etl_small_tables.py && python etl_big_tables.py

It'll take a lot of time to build the entire DataBase because is data from millions of companies in Brazil.

After the total creation of the DataBase, you can do a `docker compose down`. To open only the PostgreSQL instance where the data is storaged do a `docker compose up db` and to close do again a `docker compose down`.