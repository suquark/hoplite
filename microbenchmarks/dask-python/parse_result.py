import os
import pandas as pd

tables = []

for i in range(1, 100):
    filename = f"result-{i}.csv"
    if os.path.exists(filename):
        tables.append(pd.read_csv(filename, header=None))
    else:
        break

df_avg = pd.concat(tables).groupby(by=[0, 1, 2]).mean()
df_std = pd.concat(tables).groupby(by=[0, 1, 2]).std()
df_cnt = pd.concat(tables).groupby(by=[0, 1, 2]).count()
df_final = pd.concat([df_avg, df_std, df_cnt], axis=1)
df_final.reset_index(inplace=True)
columns = ['Benchmark Name', '#Nodes', 'Object Size (in bytes)',
           'Average Time (s)', 'Std Time (s)', 'Repeated Times']
df_final.to_csv("dask_results.csv", header=columns, index=False)
