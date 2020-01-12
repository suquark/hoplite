import argparse
import subprocess
import numpy as np
import ray

import py_distributed_object_store as store_lib

from utils import add_arguments, get_my_address, create_store_using_args

parser = argparse.ArgumentParser(description='The basic test example')
add_arguments(parser)
parser.add_argument('--world-size', type=int, required=True,
                    help='Size of the collective processing group')
parser.add_argument('--object-size', type=int, required=True,
                    help='The size of the object')

args = parser.parse_args()

store = create_store_using_args(args)
object_id = store_lib.ObjectID(b'\0' * 20)


@ray.remote(resources={'node': 1}, max_calls=1)
def multicast(world_rank, object_size):
    if world_rank == 0:
        array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        buffer = store_lib.Buffer.from_numpy(array)
        store.put(buffer, object_id)
    else:
        buffer = store.get(buffer, object_id)


notification_p = subprocess.Popen(['../notification', get_my_address()])

ray.init(address='auto')

tasks = []

for rank in range(args.world_size):
    task_id = multicast.remote(rank, args.object_size)
    tasks.append(task_id)

ray.get(tasks)

notification_p.terminate()
