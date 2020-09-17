import os
import sys
import numpy as np

path = sys.argv[1]
prefix = sys.argv[2]

all_step_time = []

for filename in os.listdir(path):
    if filename.startswith(prefix):
        step_time_rank = []
        with open(os.path.join(path, filename), 'r') as f:
            for line in f.readlines():
                if "step time:" in line:
                    step_time_rank.append(float(line.split("step time:")[1]))
        all_step_time.append(np.array(step_time_rank))

all_step_time = np.array(all_step_time)
all_step_time = all_step_time[:, 5:]
all_step_time = np.amax(all_step_time, axis=0)

all_step_throughput = 1.0 / all_step_time

print(np.mean(all_step_throughput), np.std(all_step_throughput))
