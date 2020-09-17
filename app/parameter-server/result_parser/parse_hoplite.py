import os
import sys
import numpy as np

filename = sys.argv[1]
n_nodes = int(sys.argv[3])

all_step_time = []

for i in range(n_nodes):
    step_time_rank = []
    with open(filename, 'r') as f:
        for line in f.readlines():
            if "{i} in actor time" in line:
                step_time_rank.append(float(line.split(f"{i} in actor time")[1]))
    all_step_time.append(step_time_rank)

all_step_time = np.array(all_step_time)
all_step_time = all_step_time[:, 5:]
all_step_time = np.amax(all_step_time, axis=0)

print(np.mean(all_step_time), np.std(all_step_time))
