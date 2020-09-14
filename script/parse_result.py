import os
import sys
import numpy as np

WARMUP_ROUNDS = 2

def parse_multicast(folder_path):
    files = os.listdir(folder_path)
    all_trial_times = []
    for filename in files:
        if 'rank' in filename and 'rank_0' not in filename:
            try:
                f = open(os.path.join(folder_path, filename))
                trial_times = []
                for line in f.readlines():
                    if 'is retrieved using' in line:
                        tmp = line.split('is retrieved using')[1]
                        tmp = tmp.split('seconds')[0]
                        retrieval_time = float(tmp)
                        trial_times.append(retrieval_time)
                f.close()
                all_trial_times.append(np.array(trial_times))
            except:
                print("Bad file", folder_path, filename)
                return None
    n_trials = len(all_trial_times[0])
    if not all([len(x) == n_trials for x in all_trial_times]):
        print("Bad folder", folder_path, filename)
        return None
    all_trial_times = np.array(all_trial_times)
    all_trial_times = np.max(all_trial_times, axis=0)
    return all_trial_times

def parse_reduce(folder_path):
    files = os.listdir(folder_path)
    all_trial_times = []
    for filename in files:
        if 'rank_0' in filename:
            try:
                f = open(os.path.join(folder_path, filename))
                for line in f.readlines():
                    if 'is reduced using' in line:
                        tmp = line.split('is reduced using')[1]
                        tmp = tmp.split()[0]
                        reduce_time = float(tmp)
                        all_trial_times.append(reduce_time)
                f.close()
            except:
                print("Bad file", folder_path, filename)
    all_trial_times = np.array(all_trial_times)
    return all_trial_times

def parse_allreduce(folder_path):
    files = os.listdir(folder_path)
    all_trial_times = []
    for filename in files:
        if 'rank' in filename:
            try:
                f = open(os.path.join(folder_path, filename))
                trial_times = []
                for line in f.readlines():
                    if 'is reduced using' in line:
                        tmp = line.split('is reduced using')[1]
                        tmp = tmp.split()[0]
                        reduce_time = float(tmp)
                        trial_times.append(reduce_time)
                f.close()
                all_trial_times.append(np.array(trial_times))
            except:
                print("Bad file", folder_path, filename)

    n_trials = len(all_trial_times[0])
    if not all([len(x) == n_trials for x in all_trial_times]):
        print("Bad folder", folder_path, filename)
        return None
    all_trial_times = np.array(all_trial_times)
    all_trial_times = np.max(all_trial_times, axis=0)
    return all_trial_times

def parse_gather(folder_path):
    files = os.listdir(folder_path)
    all_trial_times = []
    for filename in files:
        if 'rank_0' in filename:
            try:
                f = open(os.path.join(folder_path, filename))
                for line in f.readlines():
                    if 'gathered using' in line:
                        tmp = line.split('gathered using')[1]
                        tmp = tmp.split()[0]
                        gather_time = float(tmp)
                        all_trial_times.append(gather_time)
                f.close()
            except:
                print("Bad file", folder_path, filename)
    all_trial_times = np.array(all_trial_times)
    return all_trial_times


def parse_file(task_name, log_dir, foldername):
    path = os.path.join(log_dir, foldername)

    if task_name == 'multicast_test':
        return parse_multicast(path)
    elif task_name == 'reduce_test':
        return parse_reduce(path)
    elif task_name == 'allreduce_test':
        return parse_allreduce(path)
    elif task_name == 'gather_test':
        return parse_gather(path)
    else:
        raise ValueError('Unknown task', task_name)

def main(log_dir):
    files = os.listdir(log_dir)

    tasks = {}

    for filename in files:
        if filename != "latest":
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
        assert len(tasks[task]) == 1
        for foldername in tasks[task]:
            results[task] = parse_file(task.split('-')[0], log_dir, foldername)[WARMUP_ROUNDS:]

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
