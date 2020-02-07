import numpy as np
import os

for log_file in os.listdir('ps-log/'):
    with open(log_file, "r") as f:
        all_time = []
        for line in f:
            if "step time:" in line:
                all_time.append(float(line.split()[-1]))
        print(log_file, all_times)
