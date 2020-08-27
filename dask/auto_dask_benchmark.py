from subprocess import Popen, PIPE

with open("result.txt", "wb") as f:
    for algorithm in ('multicast', 'gather', 'reduce', 'allreduce'):
        for world_size in (4, 8, 12, 16):
            for object_size in (2 ** 10, 2 ** 15, 2 ** 20, 2 ** 25, 2 ** 30):
                process = Popen(["python", "dask_benchmark.py",
                        algorithm, "-n", world_size, "-s"], stdout=PIPE)
                (output, err) = process.communicate()
                exit_code = process.wait()
                print(algorithm, world_size, object_size, float(output))
                f.write(f"{algorithm},{world_size},{object_size},{float(output)}")
