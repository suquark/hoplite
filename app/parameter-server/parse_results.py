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
        all_time = np.array(all_time[3:-3])
        if log_file.split('-')[0] == 'async':
            all_time = 8 * ((int(log_file.split('-')[1]) - 1) // 2) / all_time
        else:
            all_time = 8 * int(log_file.split('-')[1]) / all_time
        new_all_time = []
        for i in range(0, len(all_time), 4):
            new_all_time.append(np.mean(all_time[i:i + 4]))
        print(log_file.ljust(20), np.mean(new_all_time), np.std(new_all_time), len(new_all_time), sep='\t')
