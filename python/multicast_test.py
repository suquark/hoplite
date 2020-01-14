import argparse
import subprocess
import numpy as np
import os
import time
import ray

import py_distributed_object_store as store_lib

import utils

@ray.remote(resources={'node': 1}, max_calls=1)
def multicast(args_dict, world_rank, object_size):
    store = utils.create_store_using_dict(args_dict)
    object_id = store_lib.ObjectID(b'\0' * 20)
    if world_rank == 0:
        array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        buffer = store_lib.Buffer.from_numpy(array)
        print("Buffer created, crc32 =", buffer.crc32())
        store.put(buffer, object_id)
    else:
        time.sleep(5)
        start = time.time()
        buffer = store.get(object_id)
        during = time.time() - start
        print("Buffer received, crc32 =", buffer.crc32(), "during =", during)
    time.sleep(20)

