# import pandas as pd
# import warnings # disable warnings
# warnings.filterwarnings("ignore")

# df = pd.read_csv("transformedfiles/empresas.csv", usecols=["cnpj_basico", "razao_social"], chunksize=100_000)

# unique_values = set()

# for chunk in df:
#     for v in chunk.loc[chunk["cnpj_basico"] == "47638738"]["razao_social"].values:
#         print(v)
#         unique_values.add(v)

# print(unique_values)
import pickle as pk
with open("last_inserted_index.pkl", "rb") as f:
    i = pk.load(f)
with open("last_inserted_index.pkl", "wb") as f:
    pk.dump(i+1, f)