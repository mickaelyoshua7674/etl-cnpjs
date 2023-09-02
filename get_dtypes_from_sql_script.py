from collections import defaultdict
import json

with open("create_tables.sql", "r") as f:
    create_tables = f.read().split("----------------------------------------------------------------------------------------------------")[1:-1]
create_tables = [ct.replace("    ", "").replace(",", "").replace(";\n", "") for ct in create_tables]
table_names = [ct.split("CREATE TABLE public.")[1].split(" (")[0] for ct in create_tables]
columns = [ct.split("CREATE TABLE public.")[1].split(" (")[1].splitlines()[1:-1] for ct in create_tables]
columns_type = [[c.split(" ")[:2] for c in table] for table in columns]

ct = columns_type.copy()
for i in range(len(ct)):
    dicts = {}
    for j in range(len(ct[i])):
        if ct[i][j][-1].startswith("CHAR") or ct[i][j][-1] == "DATE":
            ct[i][j][-1] = "str"
            dicts.update({ct[i][j][0]: ct[i][j][-1]})
        elif ct[i][j][-1] == "INTEGER":
            ct[i][j][-1] = "int"
            dicts.update({ct[i][j][0]: ct[i][j][-1]})
        elif ct[i][j][-1] == "NUMERIC":
            ct[i][j][-1] = "float"
            dicts.update({ct[i][j][0]: ct[i][j][-1]})
    ct[i] = dicts

table_cols_types = defaultdict(str)
for t, c in zip(table_names, ct):
    table_cols_types[t] = c

with open("all_dtypes.json", "w") as f:
    json.dump(dict(table_cols_types), f)