import argparse
import os
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
    tasks = {}

    for filename in os.listdir(log_dir):
        if filename == "latest":
            continue
        # log name format: $date-$time-$test_name-$world_size-$object_size
        splited = filename.split('-')
        if len(splited) != 5:
            raise Exception(f"Unexpected log name {filename}.")
        task_name, number_of_nodes, object_size = splited[2:5]
        task = (task_name, number_of_nodes, object_size)
        if task not in tasks:
            tasks[task] = []
        tasks[task].append(filename)

    results = {}

    for task, folders in tasks.items():
        task_results = []
        for foldername in folders:
            result = parse_file(task[0], log_dir, foldername)
            if not np.isnan(result):
                task_results.append(result)
        results[task] = np.array(task_results)

    task_list = sorted(list(results.keys()), reverse=True)

    for task in task_list:
        task_name, number_of_nodes, object_size = task
        print(f"{task_name}, {number_of_nodes}, {object_size}, {np.mean(results[task])}, {np.std(results[task])}, {len(results[task])}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Gloo benchmark results parser.')
    parser.add_argument('log_dir', metavar='PATH', nargs='?', type=str, default='log',
                        help='The logging directory of Gloo benchmarks')
    args = parser.parse_args()
    main(args.log_dir)
