import numpy as np

def parse(filename):
    all_step_time = []
    with open(filename, 'r') as f:
        for line in f.readlines():
            if f"step time:" in line:
                all_step_time.append(float(line.split(f"step time:")[1]))
    all_step_time = np.array(all_step_time[6:])
    all_step_throughput = 1.0 / all_step_time
    all_step_throughput = (all_step_throughput[0::2] + all_step_throughput[1::2]) / 2
    return np.mean(all_step_throughput), np.std(all_step_throughput)
