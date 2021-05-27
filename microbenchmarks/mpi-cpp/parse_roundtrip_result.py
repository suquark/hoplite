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

    df = df[df['Benchmark Name'].str.contains('roundtrip')]
    sz = df['Object Size (in bytes)'].astype('int64')
    df = df[(sz == 2**10) | (sz == 2**20) | (sz == 2**30)]

    if args.verbose:
        print(df)
    
    rs = df[['Object Size (in bytes)', 'Average Time (s)', 'Std Time (s)', 'Repeated Times']].values
    with open('mpi-roundtrip.csv', "w") as f:
        for r in rs:
            f.write(f"mpi,{r[0]},{r[1]},{r[2]}\n")
