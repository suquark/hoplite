import argparse
import os
import numpy as np

import sys

sys.path.insert(0, "../../test_utils")
import result_parser_utils


WARMUP_ROUNDS = 2


def get_durations(lines):
    durations = []
    for line in lines:
        if 'duration = ' in line:
            tmp = line.split('duration = ')[1]
            durations.append(float(tmp))
    return durations


def parse_all_ranks(folder_path, with_rank0=True):
    files = os.listdir(folder_path)
    all_rank_durations = []
    for filename in files:
        if 'rank' in filename and (with_rank0 or 'rank_0' not in filename):
            try:
                with open(os.path.join(folder_path, filename)) as f:
                    durations = get_durations(f.readlines())
                all_rank_durations.append(durations)
            except Exception:
                print("Bad file", folder_path, filename)
                return None
    return np.max(all_rank_durations, axis=0)


def parse_file(task_name, log_dir, foldername):
    path = os.path.join(log_dir, foldername)

    if task_name in ('allreduce', 'allgather'):
        return parse_all_ranks(path)
    elif task_name == 'multicast_test':
        return parse_all_ranks(path, with_rank0=False)
    elif task_name in ('reduce', 'gather', 'subset_reduce'):
        return result_parser_utils.default_parse_file(task_name, log_dir, foldername)
    else:
        raise ValueError('Unknown task', task_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Hoplite (C++) benchmark results parser.')
    parser.add_argument('log_dir', metavar='PATH', nargs='?', type=str, default='log',
                        help='The logging directory of Gloo benchmarks')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    df = result_parser_utils.parse(args.log_dir, parse_file)
    if args.verbose:
        print(df)
    df.to_csv('hoplite_results.csv', index=False)
