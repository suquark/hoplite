import argparse
from subprocess import Popen, PIPE

parser = argparse.ArgumentParser(description='Automatic Dask collective communication benchmark')
parser.add_argument('rounds', type=int, help="How many rounds we would to run the benchmark.")

args = parser.parse_args()

for i in range(args.rounds):
    with open(f"result-{i+1}.csv", "w") as f:
        for algorithm in ('multicast', 'gather', 'reduce', 'allreduce'):
            for world_size in (4, 8, 12, 16):
                for object_size in (2 ** 10, 2 ** 15, 2 ** 20, 2 ** 25, 2 ** 30):
                    process = Popen(["python", "dask_benchmark.py",
                            algorithm, "-n", str(world_size), "-s", str(object_size)], stdout=PIPE)
                    (output, err) = process.communicate()
                    exit_code = process.wait()
                    print(algorithm, world_size, object_size, float(output))
                    f.write(f"{algorithm},{world_size},{object_size},{float(output)}\n")
