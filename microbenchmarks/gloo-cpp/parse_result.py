import os
import sys
import numpy as np

# Example output

"""
Device:      tcp, pci=0000:00:05.0, iface=ens5, speed=-1, addr=[172.31.49.113]
Algorithm:   allreduce_ring_chunked
Options:     processes=4, inputs=1, threads=1

   elements   min (us)   p50 (us)   p99 (us)   max (us)   avg (GB/s)    samples
  268435456    1443672    1443672    1443672    1443672        0.693          1
"""

def parse_file(task_name, log_dir, foldername):
    file_name = os.path.join(log_dir, foldername, "rank_0.log")
    with open(file_name) as f:
        try:
            # The unit of the original result is microsecond. We turn it into seconds.
            return float(f.readlines()[5].split()[2]) / 1000 / 1000
        except Exception:
            return float("nan")


def main(log_dir):
    files = os.listdir(log_dir)

    tasks = {}

    for filename in files:
        if filename == "latest":
            continue
        splited = filename.split('-')
        if len(splited) != 5:
            exit(-1)
        task_name = splited[2]
        number_of_nodes = splited[3]
        object_size = splited[4]

        task = task_name + '-' + number_of_nodes + '-' + object_size

        if task not in tasks:
            tasks[task] = []

        tasks[task].append(filename)

    results = {}

    for task in tasks:
        task_results = []
        for foldername in tasks[task]:
            result = parse_file(task.split('-')[0], log_dir, foldername)
            if not np.isnan(result):
                task_results.append(result)
        task_results = np.array(task_results)

        results[task] = task_results

    task_list = []
    for task in results:
        task_list.append(task)

    task_list = sorted(task_list, reverse=True)

    for task in task_list:
        print(", ".join(task.split("-") + [str(np.mean(results[task])), str(np.std(results[task])), str(len(results[task]))]))


if __name__ == "__main__":
    assert len(sys.argv) == 2, "Usage: python parse_result.py LOG_DIR"
    log_dir = sys.argv[1]
    main(log_dir)
