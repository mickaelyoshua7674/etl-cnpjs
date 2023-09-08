import pandas as pd
import warnings # disable warnings of pandas
warnings.filterwarnings("ignore")

paises = pd.read_csv("mergedfiles/paises.csv", usecols=["cod_pais"])["cod_pais"].values

df = pd.read_csv("transformedfiles/estabelecimentos.csv", chunksize=100_000)

unique_values = set()

for chunk in df:
    for v in chunk["cod_pais"].unique():
        if v not in paises:
            unique_values.add(v)

print(unique_values)