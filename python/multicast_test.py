import argparse
import subprocess
import numpy as np
import py_distributed_object_store as store_lib

from utils import add_arguments, get_my_address, create_store_using_args

parser = argparse.ArgumentParser(description='The basic test example')
add_arguments(parser)
parser.add_argument('--world-size', type=int,
                    help='Size of the collective processing group')
parser.add_argument('--world-rank', type=int,
                    help='The index of the process in the group')
parser.add_argument('--object-size', type=int, help='The size of the object')

args = parser.parse_args()

store = create_store_using_args(args)
object_id = store_lib.ObjectID(b'\0' * 20)

if args.world_rank == 0:
    redis_p = subprocess.call(['redis-server', 'redis.conf'])
    notification_p = subprocess.call(['notification', get_my_address()])
    array = np.random.randint(2**30, size=args.object_size//4, dtype=np.int32)
    buffer = store_lib.Buffer.from_numpy(array)
    store.put(buffer, object_id)
else:
    buffer = store.get(buffer, object_id)
