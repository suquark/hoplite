import os
import sys

def parse_multicast(folder_path):
    last_retrieval_time = 0
    try:
        f = open(folder_path, 'r')
        for line in f.readlines():
            if 'duration =' in line:
                tmp = line.split('duration =')[1]
                retrieval_time = float(tmp)
                if retrieval_time > last_retrieval_time:
                    last_retrieval_time = retrieval_time

        f.close()
    except:
        print (folder_path)
    try:
        a = retrieval_time
    except:
        print (folder_path)
    return last_retrieval_time

def parse_reduce(folder_path):
    try:
        f = open(folder_path, 'r')
        for line in f.readlines():
            if 'duration =' in line:
                tmp = line.split('duration =')[1]
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
    last_retrieval_time = 0
    try:
        f = open(folder_path, 'r')
        for line in f.readlines():
            if 'duration =' in line:
                tmp = line.split('duration =')[1]
                retrieval_time = float(tmp)
                if retrieval_time > last_retrieval_time:
                    last_retrieval_time = retrieval_time

        f.close()
    except:
        print (folder_path)
    try:
        a = retrieval_time
    except:
        print (folder_path)
    return last_retrieval_time


def parse_file(task_name, log_dir, foldername):
    path = os.path.join(log_dir, foldername)

    if task_name == 'multicast':
        return parse_multicast(path)

    if task_name == 'reduce':
        return parse_reduce(path)

    if task_name == 'allreduce':
        return parse_allreduce(path)

    assert (False)

def main(log_dir):
    files = os.listdir(log_dir)

    tasks = {}

    for filename in files:
        splited = filename.split('-');
        if len(splited) != 3:
            exit(-1)
        task_name = splited[0]
        number_of_nodes = splited[1]
        object_size = splited[2].split('.')[0]
        
        task = task_name + '-' + number_of_nodes + '-' + object_size

        if task not in tasks:
            tasks[task] = []

        tasks[task].append(filename)

    results = {}
    
    for task in tasks:
        result = 0
        for foldername in tasks[task]:
            result += parse_file(task.split('-')[0], log_dir, foldername);
        result /= len(tasks[task])

        results[task] = result

    task_list = []
    for task in results:
        task_list.append(task)

    task_list = sorted(task_list, reverse=True)

    for task in task_list:
        print (task.replace('-',','), ',', results[task])


if __name__ == "__main__":
    assert len(sys.argv) == 2, "Usage: python parse_result.py LOG_DIR"
    log_dir = sys.argv[1]
    main(log_dir)
