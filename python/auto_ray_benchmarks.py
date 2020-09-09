import argparse
import numpy as np
import ray

import utils
import ray_benchmarks

parser = argparse.ArgumentParser(description='Ray microbenchmarks')
utils.add_arguments(parser)
parser.add_argument('test_name', type=str, help='Name of the test (multicast, reduce, allreduce, gather, allgather)')
parser.add_argument('-n', '--world-size', type=int, required=False,
                    help='Size of the collective processing group')
parser.add_argument('-s', '--object-size', type=int, required=False,
                    help='The size of the object')
args = parser.parse_args()


def test_with_mean_std(repeat_times, test_name, notification_address, world_size, object_size):
    results = []
    for _ in range(repeat_times):
        test_case = ray_benchmarks.__dict__[test_name]
        duration = test_case(notification_address, world_size, object_size)
        results.append(duration)
    return np.mean(results), np.std(results)


if __name__ == "__main__":
    utils.start_location_server()
    notification_address = utils.get_my_address()
    ray.init(address='auto')
    test_name = 'ray_' + args.test_name
    assert test_name in ray_benchmarks.__dict__ or args.test_name == 'auto'
    if args.test_name != 'auto':
        assert args.world_size is not None and args.object_size is not None
        mean, std = test_with_mean_std(5, test_name, notification_address, args.world_size, args.object_size)
        print(f"{args.test_name},{args.world_size},{args.object_size},{mean},{std}")
    else:
        assert args.world_size is None and args.object_size is None
        with open("ray-microbenchmark.csv", "w") as f:
            for algorithm in ('ray_multicast', 'ray_gather', 'ray_reduce', 'ray_allreduce'):
                for world_size in (4, 8, 12, 16):
                    for object_size in (2 ** 10, 2 ** 15, 2 ** 20, 2 ** 25, 2 ** 30):
                        mean, std = test_with_mean_std(5, algorithm, notification_address, world_size, object_size)
                        print(f"{algorithm}, {world_size}, {object_size}, {mean}, {std}")
                        f.write(f"{algorithm},{world_size},{object_size},{mean},{std}\n")
