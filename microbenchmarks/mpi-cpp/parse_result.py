import argparse
import sys
import os

sys.path.insert(0, "../../test_utils")
import result_parser_utils


def parse_file(task_name, log_dir, foldername):
    folder_path = os.path.join(log_dir, foldername)
    retrieval_time = None
    try:
        with open(folder_path) as f:
            for line in f.readlines():
                if 'duration = ' in line:
                    tmp = line.split('duration = ')[1]
                    retrieval_time = float(tmp)
                    break
    except Exception:
        pass
    return retrieval_time


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MPI (C++) benchmark results parser.')
    parser.add_argument('log_dir', metavar='PATH', nargs='?', type=str, default='log',
                        help='The logging directory of Gloo benchmarks')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    df = result_parser_utils.parse(args.log_dir, parse_file)
    if args.verbose:
        print(df)
    df.to_csv('mpi_results.csv', index=False)
