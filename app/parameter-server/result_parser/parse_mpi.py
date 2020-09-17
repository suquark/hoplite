import os
import sys
import numpy as np

filename = sys.argv[1]
n_nodes = int(sys.argv[2])

all_step_time = []
min_len = 1e10

for i in range(n_nodes):
    step_time_rank = []
    with open(filename, 'r') as f:
        for line in f.readlines():
            if f" {i} step time:" in line:
                step_time_rank.append(float(line.split(f" {i} step time:")[1]))
    print(len(step_time_rank))
    min_len = min(min_len, len(step_time_rank))
    all_step_time.append(np.array(step_time_rank))

all_step_time = np.array([a[:min_len] for a in all_step_time])
all_step_time = all_step_time[:, 5:]
all_step_time = np.amax(all_step_time, axis=0)

print(np.mean(all_step_time), np.std(all_step_time))
