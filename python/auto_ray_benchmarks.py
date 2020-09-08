import argparse
import ray

import utils
import ray_benchmarks

parser = argparse.ArgumentParser(description='Ray microbenchmarks')
utils.add_arguments(parser)
parser.add_argument('test_name', type=str, help='Name of the test (multicast, reduce, allreduce, gather, allgather)')
parser.add_argument('-n', '--world-size', type=int, required=True,
                    help='Size of the collective processing group')
parser.add_argument('-s', '--object-size', type=int, required=True,
                    help='The size of the object')
args = parser.parse_args()

if __name__ == "__main__":
    utils.start_location_server()
    notification_address = utils.get_my_address()
    ray.init(address='auto')
    test_name = 'ray_' + args.test_name
    assert test_name in ray_benchmarks.__dict__
    test_case = ray_benchmarks.__dict__[test_name]
    during = test_case(notification_address, args.world_size, args.object_size)
    print(f"{args.test_name},{args.world_size},{args.object_size},{during}")
