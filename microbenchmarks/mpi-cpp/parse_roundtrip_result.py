import argparse
import sys

sys.path.insert(0, "../../test_utils")
import result_parser_utils


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MPI roundtrip benchmark results parser.')
    parser.add_argument('log_dir', metavar='PATH', nargs='?', type=str, default='log',
                        help='The logging directory of Gloo benchmarks')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    df = result_parser_utils.parse(args.log_dir, result_parser_utils.default_parse_file)
    if args.verbose:
        print(df)
    df.to_csv('mpi-roundtrip.csv', index=False)
