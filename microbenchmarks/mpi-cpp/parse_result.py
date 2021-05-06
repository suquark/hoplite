import argparse
import sys

sys.path.insert(0, "../../test_utils")
import result_parser_utils


def parse_file(task_name, log_dir, foldername):
    try:
        lines = result_parser_utils.read_rank0_lines(log_dir, foldername)
        for line in lines:
            if 'duration = ' in line:
                tmp = line.split('duration = ')[1]
                return float(tmp)
    except Exception:
        return None


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
