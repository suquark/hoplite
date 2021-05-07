import argparse
import numpy as np
import ray

import hoplite
import ray_microbenchmarks

NUM_NODES = (4, 8, 12, 16)
OBJECT_SIZES = (2 ** 10, 2 ** 15, 2 ** 20, 2 ** 25, 2 ** 30)
REPEAT_TIMES = 5

microbenchmark_names = ['roundtrip', 'multicast', 'reduce', 'allreduce', 'gather', 'allgather', 'auto']
parser = argparse.ArgumentParser(description='Ray microbenchmarks')
parser.add_argument('test_name', type=str, choices=microbenchmark_names, help='Microbenchmark name')
parser.add_argument('-n', '--world-size', type=int, required=False,
                    help='Size of the collective processing group')
parser.add_argument('-s', '--object-size', type=int, required=False,
                    help='The size of the object')
args = parser.parse_args()


def test_with_mean_std(test_name, notification_address, world_size, object_size,
                       repeat_times=REPEAT_TIMES):
    results = []
    for _ in range(repeat_times):
        test_case = ray_microbenchmarks.__dict__[test_name]
        duration = test_case(notification_address, world_size, object_size)
        results.append(duration)
    return np.mean(results), np.std(results)


if __name__ == "__main__":
    hoplite.start_location_server()
    notification_address = hoplite.get_my_address()

    ray.init(address='auto')
    test_name = 'ray_' + args.test_name
    assert test_name in ray_microbenchmarks.__dict__ or args.test_name == 'auto'
    if args.test_name != 'auto':
        assert args.world_size is not None and args.object_size is not None
        mean, std = test_with_mean_std(test_name, notification_address, args.world_size, args.object_size, 5)
        print(f"{args.test_name},{args.world_size},{args.object_size},{mean},{std}")
    else:
        assert args.world_size is None and args.object_size is None
        with open("ray-microbenchmark.csv", "w") as f:
            algorithm = 'ray_roundtrip'
            world_size = 2
            for object_size in OBJECT_SIZES:
                mean, std = test_with_mean_std(algorithm, notification_address, world_size, object_size)
                print(f"{algorithm}, {world_size}, {object_size}, {mean}, {std}")
                f.write(f"{algorithm},{world_size},{object_size},{mean},{std}\n")
            for algorithm in ('ray_multicast', 'ray_gather', 'ray_reduce', 'ray_allreduce'):
                for world_size in NUM_NODES:
                    for object_size in OBJECT_SIZES:
                        mean, std = test_with_mean_std(algorithm, notification_address, world_size, object_size)
                        print(f"{algorithm}, {world_size}, {object_size}, {mean}, {std}")
                        f.write(f"{algorithm},{world_size},{object_size},{mean},{std}\n")
