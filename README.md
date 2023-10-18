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
Download é realizado utilizando lógica Asynchronous I/O através das bibliotecas [aiohttp](https://docs.aiohttp.org/en/stable/) e [asyncio](https://docs.python.org/3/library/asyncio.html). Através de Web Scraping (utilizando [selenium](https://selenium-python.readthedocs.io/)) são coletados os nomes dos arquivos no site, em seguida, de forma paralela, é iniciado o download dos Bytes iterando chunks de Bytes (Stream), em seguida os Bytes são descompactados e é salvo o arquivo `.csv` resultante.

### Transformação e Carregamento dos arquivos


## Configuração do Ambiente
### Banco de Dados
Criar o Banco de Dados manualmente e localmente utilizando o pgAdmin 4 com as configurações padrão.

### Python
Instalar versão 3.11 ou mais recente de Python, em seguida criar um ambiente virtual executando o comando `python -m venv <nome da pasta>` no terminal (estando dentro da pasta do seu projeto) e ativar o abiente virtual com o comando `<nome da pasta>\Scripts\activate`.

Criar um arquivo `.env` para definir as variáveis de ambiente de acordo com o exemplo [.env_example](.env_example).

    Atenção ⚠: Nas variáveis de ambiente THREADS_NUMBER e CHUNKSIZE a quantidade informada será para cada processo definido, e.g.: com THREADS_NUMBER="8" você terá 8 Threads rodando em cada processo, com CHUNKSIZE="100000" serão chunks de dados de tamanho 100000 sendo processados e inseridos no Bando de Dados para cada processo.

Finalmente executar o script [setup.py](setup.py) que irá instalar todas as dependências (módulos) de Python e definiar as variáveis de ambiente infromadas no arquivo `.env`.

## Possíveis melhorias