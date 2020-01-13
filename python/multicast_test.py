import argparse
import subprocess
import numpy as np
import os
import ray

import py_distributed_object_store as store_lib

import utils

parser = argparse.ArgumentParser(description='broadcast test')
utils.add_arguments(parser)
parser.add_argument('--world-size', type=int, required=True,
                    help='Size of the collective processing group')
parser.add_argument('--object-size', type=int, required=True,
                    help='The size of the object')

args = parser.parse_args()
args_dict = utils.extract_dict_from_args(args)


@ray.remote(resources={'node': 1}, max_calls=1)
def multicast(world_rank, object_size):
    store = utils.create_store_using_dict(args_dict)
    object_id = store_lib.ObjectID(b'\0' * 20)
    if world_rank == 0:
        array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        buffer = store_lib.Buffer.from_numpy(array)
        store.put(buffer, object_id)
    else:
        buffer = store.get(buffer, object_id)


notification_p = subprocess.Popen(['../notification', utils.get_my_address()])
utils.register_cleanup([notification_p])

ray.init(address='auto')

tasks = []

for rank in range(args.world_size):
    task_id = multicast.remote(rank, args.object_size)
    tasks.append(task_id)

ray.get(tasks)
