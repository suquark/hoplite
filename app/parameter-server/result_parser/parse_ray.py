import os
import sys
import numpy as np

filename = sys.argv[1]

all_step_time = []
min_len = 1e10

with open(filename, 'r') as f:
    for line in f.readlines():
        if f"step time:" in line:
            all_step_time.append(float(line.split(f"step time:")[1]))

all_step_time = all_step_time[3:]

print(np.mean(all_step_time), np.std(all_step_time))
