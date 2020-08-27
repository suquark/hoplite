import argparse
import time
import sys
import os

import numpy as np
from dask.distributed import Client

parser = argparse.ArgumentParser(description='Dask collective communication benchmark')
parser.add_argument('algorithm', type=str,
                    help="The algorithm to be tested (multicast, gather, reduce, allreduce)")
parser.add_argument('-n', '--world_size', type=int, required=True,
                    help="Size of the collective processing group")
parser.add_argument('-s', '--object-size', type=int, required=True,
                    help='The size of the object')

args = parser.parse_args()


def create_object(index, object_size):
    return np.empty(object_size//4, dtype=np.float32)


def get_object(index, o):
    return True


def gather_object(o):
    return True


def reduce_object(obj_list, object_size):
    a = np.zeros(object_size//4, dtype=np.float32)
    for obj in obj_list:
        a += obj
    return True


def reduce_object_return(obj_list, object_size):
    a = np.zeros(object_size//4, dtype=np.float32)
    for obj in obj_list:
        a += obj
    return a


def main(algorithm, world_size, object_size):
    client = Client("127.0.0.1:8786")
    if algorithm == 'multicast':
        sender = client.submit(create_object, object_size, workers=['Dask-0'])
        receivers = []
        for i in range(1, world_size):
            receivers.append(client.submit(get_object, i, sender, workers=[f'Dask-{i}']))
        before = time.time()
        for receiver in receivers:
            receiver.result()
        after = time.time()
    elif algorithm == 'gather':
        senders = []
        for i in range(0, world_size):   
            senders.append(client.submit(create_object, i, object_size, workers=[f'Dask-{i}']))
        receiver = client.submit(gather_object, senders, workers=['Dask-0'])
        before = time.time()
        receiver.result()
        after = time.time()
    elif algorithm == 'reduce':
        senders = []
        for i in range(0, world_size):   
            senders.append(client.submit(create_object, i, object_size, workers=[f'Dask-{i}']))
        receiver = client.submit(reduce_object, senders, object_size, workers=['Dask-0'])
        before = time.time()
        receiver.result()
        after = time.time()
    elif algorithm == 'allreduce':
        senders = []
        for i in range(0, world_size):   
            senders.append(client.submit(create_object, i, object_size, workers=[f'Dask-{i}']))
        receiver = client.submit(reduce_object_return, senders, object_size, workers=['Dask-0'])

        other_receivers = []
        for i in range(1, world_size):   
            other_receivers.append(client.submit(get_object, i, receiver, workers=[f'Dask-{i}']))
    
        before = time.time()
        for r in other_receivers:
            r.result()
        after = time.time()
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")
    print(after - before)


if __name__ == "__main__":
    pass