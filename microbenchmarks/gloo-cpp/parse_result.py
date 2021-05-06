import argparse
import sys

sys.path.insert(0, "../../test_utils")
import result_parser_utils

# Example output

"""
Device:      tcp, pci=0000:00:05.0, iface=ens5, speed=-1, addr=[172.31.49.113]
Algorithm:   allreduce_ring_chunked
Options:     processes=4, inputs=1, threads=1

   elements   min (us)   p50 (us)   p99 (us)   max (us)   avg (GB/s)    samples
  268435456    1443672    1443672    1443672    1443672        0.693          1
"""

def parse_file(task_name, log_dir, foldername):
    try:
        lines = result_parser_utils.read_rank0_lines(log_dir, foldername)
        # The unit of the original result is microsecond. We turn it into seconds.
        return float(lines[5].split()[2]) / 1000 / 1000
    except Exception:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Gloo (C++) benchmark results parser.')
    parser.add_argument('log_dir', metavar='PATH', nargs='?', type=str, default='log',
                        help='The logging directory of Gloo benchmarks')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    df = result_parser_utils.parse(args.log_dir, parse_file)
    if args.verbose:
        print(df)
    df.to_csv('gloo_results.csv', index=False)
