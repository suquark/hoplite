import numpy as np
import os
log_dir = 'ps-log/'
for log_file in sorted(os.listdir(log_dir)):
    with open(os.path.join(log_dir, log_file), "r") as f:
        all_time = []
        for line in f:
            if "step time:" in line:
                all_time.append(float(line.split()[-1]))
        all_time = all_time[1:]
        all_time = all_time[1:-10]
        if log_file.split('-')[0] == 'async':
            all_time = 8 * ((int(log_file.split('-')[1]) - 1) // 2) / all_time
        else:
            all_time = 8 * int(log_file.split('-')[1] / all_time
        print(log_file.ljust(20), np.mean(all_time), np.std(all_time), sep='\t')
