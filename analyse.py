# import pandas as pd
# import warnings # disable warnings
# warnings.filterwarnings("ignore")

# df = pd.read_csv("transformedfiles/estabelecimentos.csv", nrows=100_000)

# for t in df.itertuples(index=False):
#     print(t)

import pickle as pk

with open("last_inserted_index.pkl", "rb") as f:
    print(pk.load(f))