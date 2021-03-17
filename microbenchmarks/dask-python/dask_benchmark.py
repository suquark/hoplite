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


def multicast_small(client, world_size, object_size, epoch):
    # Here we try to schedule more tasks and make then queuing in the worker.
    # This would greatly reduce the task scheduling overhead.
    if object_size > 1024 * 1024:
        factor = 10
    else:
        factor = 100
    prefix = world_size * epoch
    senders = []
    for i in range(factor):
        senders.append(client.submit(create_object, prefix + 10000 * i, object_size, workers=['Dask-0']))
    time.sleep(0.1)
    receivers = []
    for j in range(factor):
        for i in range(1, world_size):
            receivers.append(client.submit(get_object, prefix + i + 10000 * j, senders[j], workers=[f'Dask-{i}']))
    before = time.time()
    results = client.gather(receivers)
    after = time.time()
    return (after - before) / factor


def multicast(client, world_size, object_size, epoch):
    prefix = world_size * epoch
    sender = client.submit(create_object, prefix, object_size, workers=['Dask-0'])
    time.sleep(0.1)
    receivers = []
    for i in range(1, world_size):
        receivers.append(client.submit(get_object, prefix + i, sender, workers=[f'Dask-{i}']))
    before = time.time()
    client.gather(receivers)
    after = time.time()
    return after - before


def gather(client, world_size, object_size, epoch):
    prefix = world_size * epoch
    senders = []
    for i in range(0, world_size):   
        senders.append(client.submit(create_object, prefix + i, object_size, workers=[f'Dask-{i}']))
    time.sleep(0.1)
    receiver = client.submit(gather_object, senders, workers=['Dask-0'])
    before = time.time()
    receiver.result()
    after = time.time()
    return after - before 


def reduce(client, world_size, object_size, epoch):
    prefix = world_size * epoch
    senders = []
    for i in range(0, world_size):   
        senders.append(client.submit(create_object, prefix + i, object_size, workers=[f'Dask-{i}']))
    time.sleep(0.1)
    receiver = client.submit(reduce_object, senders, object_size, workers=['Dask-0'])
    before = time.time()
    receiver.result()
    after = time.time()
    return after - before  


def reduce_object_return_indexed(_, obj_list, object_size):
    a = np.zeros(object_size//4, dtype=np.float32)
    for obj in obj_list:
        a += obj
    return a


def allreduce_small(client, world_size, object_size, epoch):
    factor = 4
    prefix = world_size * epoch
    senders = []
    for i in range(0, world_size):
        senders.append(client.submit(create_object, prefix + i, object_size, workers=[f'Dask-{i}']))
    time.sleep(0.1)
    receivers = []
    for i in range(factor):
        receivers.append(client.submit(reduce_object_return_indexed, prefix + i, senders, object_size, workers=['Dask-0']))

    other_receivers = []
    for j in range(factor):
        for i in range(1, world_size):
            other_receivers.append(client.submit(get_object,  10000 * j + prefix + i, receivers[j], workers=[f'Dask-{i}']))

    before = time.time()
    client.gather(other_receivers)
    after = time.time()
    return (after - before) / factor


def allreduce(client, world_size, object_size, epoch):
    prefix = world_size * epoch
    senders = []
    for i in range(0, world_size):   
        senders.append(client.submit(create_object, prefix + i, object_size, workers=[f'Dask-{i}']))
    time.sleep(0.1)
    receiver = client.submit(reduce_object_return, senders, object_size, workers=['Dask-0'])

    other_receivers = []
    for i in range(1, world_size):   
        other_receivers.append(client.submit(get_object,  prefix + i, receiver, workers=[f'Dask-{i}']))

    before = time.time()
    client.gather(other_receivers)
    after = time.time()
    return after - before


def main(algorithm, world_size, object_size):
    client = Client("127.0.0.1:8786")
    if algorithm == 'multicast':
         if object_size < 64 * 1024 * 1024:
             func = multicast_small
         else:
             func = multicast
    elif algorithm == 'gather':
        func = gather
    elif algorithm == 'reduce':
        func = reduce
    elif algorithm == 'allreduce':
        if object_size < 1024 * 1024:
            func = allreduce_small
        else:
            func = allreduce
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")
    for i in range(3 if object_size <= 16 * 2 ** 20 else 1):
        func(client, world_size, object_size, i)
    if object_size > 16 * 2 ** 20:
        duration = func(client, world_size, object_size, i + 1)
    else:
        # Accumulate time for more precision.
        duration = 0.0
        for j in range(i + 1, i + 1 + 10):
            duration += func(client, world_size, object_size, j)
        duration /= 10
    print(duration)


if __name__ == "__main__":
    main(args.algorithm, args.world_size, args.object_size)
