import argparse
import os
import numpy as np

def parse_multicast(folder_path):
    try:
        f = open(folder_path)
        for line in f.readlines():
            if 'Avg MPI_Bcast time =' in line:
                tmp = line.split('MPI_Bcast time = ')[1]
                retrieval_time = float(tmp)
        f.close()
    except:
        print (folder_path)
    try:
        a = retrieval_time
    except:
        print (folder_path)
    return retrieval_time

def parse_reduce(folder_path):
    try:
        f = open(folder_path)
        for line in f.readlines():
            if 'MPI_Reduce time =' in line:
                tmp = line.split('time = ')[1]
                retrieval_time = float(tmp)
        f.close()
    except:
        print (folder_path)
    try:
        a = retrieval_time
    except:
        print (folder_path)
    return retrieval_time

def parse_gather(folder_path):
    try:
        f = open(folder_path)
        for line in f.readlines():
            if 'MPI_Gather time =' in line:
                tmp = line.split('time = ')[1]
                retrieval_time = float(tmp)
        f.close()
    except:
        print (folder_path)
    try:
        a = retrieval_time
    except:
        print (folder_path)
    return retrieval_time

def parse_allreduce(folder_path):
    try:
        f = open(folder_path)
        for line in f.readlines():
            if 'MPI_Allreduce time =' in line:
                tmp = line.split('time = ')[1]
                retrieval_time = float(tmp)
        f.close()
    except:
        print (folder_path)
    try:
        a = retrieval_time
    except:
        print (folder_path)
    return retrieval_time

def parse_file(task_name, log_dir, foldername):
    path = os.path.join(log_dir, foldername)

    if task_name == 'multicast':
        return parse_multicast(path)

    if task_name == 'reduce':
        return parse_reduce(path)

    if task_name == 'allreduce':
        return parse_allreduce(path)

    if task_name == 'gather':
        return parse_gather(path)

    assert (False)

def main(log_dir, verbose):
    files = os.listdir(log_dir)

    tasks = {}

    for filename in files:
        splited = filename.split('-')
        if len(splited) != 4:
            exit(-1)
        task_name = splited[0]
        number_of_nodes = splited[1]
        object_size = splited[2]

        task = task_name + '-' + number_of_nodes + '-' + object_size

        if task not in tasks:
            tasks[task] = []

        tasks[task].append(filename)

    results = {}

    for task in tasks:
        task_results = []
        for foldername in tasks[task]:
            task_results.append(parse_file(task.split('-')[0], log_dir, foldername))
        task_results = np.array(task_results)

        results[task] = task_results

    task_list = []
    for task in results:
        task_list.append(task)

    task_list = sorted(task_list, reverse=True)

    for task in task_list:
        print(", ".join(task.split("-") + [str(np.mean(results[task])), str(np.std(results[task])), str(len(results[task]))]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MPI (C++) benchmark results parser.')
    parser.add_argument('log_dir', metavar='PATH', nargs='?', type=str, default='log',
                        help='The logging directory of Gloo benchmarks')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    main(args.log_dir, args.verbose)
