import numpy as np
import os
log_dir = 'ps-log/'
for log_file in os.listdir(log_dir):
    with open(os.path.join(log_dir, log_file), "r") as f:
        all_time = []
        for line in f:
            if "step time:" in line:
                all_time.append(float(line.split()[-1]))
        print(log_file, all_times)
