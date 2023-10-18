# Documentação
## Descrição
Este projeto tem como objetivo criar um Banco de Dados com todas as informações disponíveis de CNPJs disponibilzados pela Receita Federal em https://dadosabertos.rfb.gov.br/CNPJ/ e serviu também para meus estudos de Concorrência e Paralelismo (programação Assíncrona).

Foram utilizadas três formas diferentes de programação Assíncrona: I/O, Processos e Threads.

## Ferramentas
* Sistema operacional: Windows 11;
* Liguagens de programação: Python-3.11.5 e SQL;
* Módulos de Python: [requeriments.txt](requirements.txt);
* Banco de Dados: PostgreSQL;
* Aplicação para acesso ao Banco de Dados: pgAdmin 4.

## Visão geral do algoritmo do projeto
### Download dos arquivos
Download é realizado utilizando lógica Asynchronous I/O através das bibliotecas [aiohttp](https://docs.aiohttp.org/en/stable/) e [asyncio](https://docs.python.org/3/library/asyncio.html). Através de Web Scraping (usando [selenium](https://selenium-python.readthedocs.io/)) são coletados os nomes dos arquivos no site, em seguida, de forma paralela, é iniciado o download dos Bytes iterando chunks do conteúdo do download (Stream), em seguida os Bytes são descompactados e é salvo o arquivo `.csv` resultante.

O script [download_unzip.py](download_unzip.py) é o responsável por fazer o download de todos os arquivos `.csv` necessários.

### Transformação e Carregamento dos arquivos
Para a leitura e transformação dos arquivos foi utlizado o [pandas](https://pandas.pydata.org/docs/) e para conexão com o Banco de Dados e inserção dos dados foi utilizado o [SQLAlchemy](https://docs.sqlalchemy.org/en/20/).

#### Arquivos menores
Os arquivos menores são as tabelas das chaves estrangeiras (Foreign Keys) dos arquivos maiores, por serem pequenas quantidades de dados e tabelas de rápida criação e inserção, foi totalmente feito de forma síncrona levando poucos segundos para ser executado.

O script [transform_and_load_small_tables.py](transform_and_load_small_tables.py) cria as tabelas e faz a povoação delas (inserção dos dados).

#### Arquivos maiores
As tabelas maiores são transformadas e inseridas através da Stream dos arquivos com a seguinte estrutura em cada processo criado:

![schema process](unit_of_process.png)

Cada processo irá ler em Stream seu arquivo individual, cada chunk de arquivo lido será processado com o Pandas e inserido numa Queue que é compartilhada entre todas as Threads naquele processo. Cada Thread irá estabelecer uma conexão com o banco de dados, e iterar na Queue inserindo os dados no Banco de Dados.

O processo de transformar e inserir dados se repete para cada chunk de dados.

Os processos criados possuem essa estrutura na imagem acima, sendo cada um lendo de um arquivo diferente, ou seja, não há compartilhamento de dados entre processos, mas há compartilhamento entre Threads através da Queue em cada processo.

O script [transform_and_load_big_tables.py](transform_and_load_big_tables.py) é responsável pela criação e povoação das tabelas maiores.

## Configuração do Ambiente
### Banco de Dados
Criar o Banco de Dados manualmente e localmente utilizando o pgAdmin 4 com as configurações padrão.

### Python
Instalar versão 3.11 ou mais recente de Python, em seguida criar um ambiente virtual executando o comando `python -m venv <nome da pasta>` no terminal (estando dentro da pasta do seu projeto) e ativar o abiente virtual com o comando `<nome da pasta>\Scripts\activate`.

Criar um arquivo `.env` para definir as variáveis de ambiente de acordo com o exemplo [.env_example](.env_example).

    Atenção ⚠: Nas variáveis de ambiente THREADS_NUMBER e CHUNKSIZE a quantidade informada será para cada processo definido, e.g.: com THREADS_NUMBER="8" você terá 8 Threads rodando em cada processo, com CHUNKSIZE="100000" serão chunks de dados de tamanho 100000 sendo processados e inseridos no Bando de Dados para cada processo.

Finalmente executar o script [setup.py](setup.py) que irá instalar todas as dependências (módulos) de Python e definiar as variáveis de ambiente infromadas no arquivo `.env`.

## Ordem de execução dos scripts
1. [setup.py](setup.py) -> Configuração do ambiente (Síncrono);
2. [download_unzip.py](download_unzip.py) -> Download dos arquivos (Assíncrono);
3. [transform_and_load_small_tables.py](transform_and_load_small_tables.py) -> Criação e povoação de tabelas menores (Síncrono);
4. [transform_and_load_big_tables.py](transform_and_load_big_tables.py) -> Criação e povoação de tabelas maiores (Assíncrono);

## Possíveis melhorias