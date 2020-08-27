import os
import pandas as pd

tables = []

for i in range(1, 100):
    filename = f"result-{i}.csv"
    if os.path.exists(filename):
        tables.append(pd.read_csv(filename), header=None)
    else:
        break

df_avg = pd.concat(tables).groupby(by=[0, 1, 2]).std()
df_std = pd.concat(tables).groupby(by=[0, 1, 2]).std()
df_final = pd.concat([df_avg, df_std], axis=1)
df_final.to_csv("all_results.csv", header=False)
