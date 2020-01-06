import os
import sys

def parse_multicast(folder_path):
    last_retrieval_time = 0
    files = os.listdir(folder_path)
    for filename in files:
        try:
            f = open(os.path.join(folder_path, filename))
            for line in f.readlines():
                if 'client' in filename and 'Object is retrieved' in line:
                    retrival_time = int(line.split()[0])
                    if retrival_time > last_retrieval_time:
                        last_retrieval_time = retrival_time

                if 'server' in filename and 'object sender is ready' in line:
                    start_time = int(line.split()[0])
            f.close()
        except:
            print (filename)
    return last_retrieval_time - start_time

def parse_reduce(folder_path):
    return 0

def parse_file(task_name, log_dir, foldername):
    path = os.path.join(log_dir, foldername)

    if task_name == 'multicast':
        return parse_multicast(path)

    if task_name == 'reduce':
        return parse_reduce(path)

    assert (False)

def main(log_dir):
    files = os.listdir(log_dir)

    tasks = {}

    for filename in files:
        splited = filename.split('-');
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
        print (task, results[task])


if __name__ == "__main__":
    assert len(sys.argv) == 2, "Usage: python parse_result.py LOG_DIR"
    log_dir = sys.argv[1]
    main(log_dir)
