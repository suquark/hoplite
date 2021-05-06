
import os
import numpy as np
import pandas as pd


def collect_log_folders(log_dir):
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

    return tasks


def parse(log_dir, parse_file):
    tasks = collect_log_folders(log_dir)

    results = {}

    for task, folders in tasks.items():
        task_results = []
        for foldername in folders:
            result = parse_file(task[0], log_dir, foldername)
            if result is None or np.isnan(result):
                print(f"Error parsing {foldername}: cannot read out value.")
            else:
                task_results.append(result)
        results[task] = np.array(task_results)

    task_list = sorted(list(results.keys()), reverse=True)

    df = pd.DataFrame(columns = ['Benchmark Name', '#Nodes', 'Object Size (in bytes)',
                                'Average Time (s)', 'Std Time (s)', 'Repeated Times'])

    for i, task in enumerate(task_list):
        task_name, number_of_nodes, object_size = task
        df.loc[i] = [task_name, number_of_nodes, object_size, np.mean(results[task]), np.std(results[task]),
                     len(results[task])]
    return df
