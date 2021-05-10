#!/usr/bin/env python3
import argparse
import time

from mpi4py import MPI
import numpy as np

import hoplite

notification_port = 7777
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
world_size = comm.Get_size()


def roundtrip(store, object_size):
    object_id = hoplite.ObjectID(b'\0' * 20)
    object_id2 = hoplite.ObjectID(b'\1' * 20)
    if rank == 0:
        array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        buffer = hoplite.Buffer.from_buffer(array)
        hash_s = hash(buffer)
        print("Buffer created, hash =", hash_s)

        comm.Barrier()
        start = time.time()
        store.put(buffer, object_id)
        buffer = store.get(object_id2)
        duration = time.time() - start
        hash_r =  hash(buffer)
        print(f"Buffer received, hash = {hash_r}, duration = {duration}")
        assert hash_s == hash_r, "Hash mismatch!"
    else:
        comm.Barrier()
        buffer = store.get(object_id)
        store.put(buffer, object_id2)


def multicast(store, object_size):
    object_id = hoplite.ObjectID(b'\0' * 20)
    if rank == 0:
        array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        buffer = hoplite.Buffer.from_buffer(array)
        print("Buffer created, hash =", hash(buffer))
        store.put(buffer, object_id)
        comm.Barrier()
    else:
        comm.Barrier()
        start = time.time()
        buffer = store.get(object_id)
        duration = time.time() - start
        print(f"Buffer received, hash = {hash(buffer)}, duration = {duration}")
        array = np.frombuffer(buffer, dtype=np.int32)
        print(array)


def reduce(store, object_size):
    object_id = hoplite.object_id_from_int(rank)
    array = np.random.rand(object_size//4).astype(np.float32)
    buffer = hoplite.Buffer.from_buffer(array)
    store.put(buffer, object_id)
    print("Buffer created, hash =", hash(buffer))
    object_ids = []
    for i in range(0, world_size):
        object_ids.append(hoplite.object_id_from_int(i))
    comm.Barrier()

    if rank == 0:
        start = time.time()
        reduction_id = store.reduce_async(object_ids, hoplite.ReduceOp.SUM)
        reduced_buffer = store.get(reduction_id)
        duration = time.time() - start

        reduce_result = np.frombuffer(reduced_buffer)
        print(f"Reduce completed, hash = {hash(reduced_buffer)}, duration = {duration}")
        print(reduce_result)


def allreduce(store, object_size):
    object_id = hoplite.object_id_from_int(rank)
    array = np.random.rand(object_size//4).astype(np.float32)
    buffer = hoplite.Buffer.from_buffer(array)
    store.put(buffer, object_id)
    print("Buffer created, hash =", hash(buffer))
    object_ids = []
    for i in range(0, world_size):
        object_ids.append(hoplite.object_id_from_int(i))
    reduction_id = hoplite.object_id_from_int(99999999999)
    comm.Barrier()

    start = time.time()
    if rank == 0:
        store.reduce_async(object_ids, hoplite.ReduceOp.SUM, reduction_id=reduction_id)
    reduced_buffer = store.get(reduction_id)
    duration = time.time() - start

    reduce_result = np.frombuffer(reduced_buffer)
    print(f"AllReduce completed, hash = {hash(reduced_buffer)}, duration = {duration}")
    print(reduce_result)


def gather(store, object_size):
    object_id = hoplite.object_id_from_int(rank)
    array = np.random.rand(object_size//4).astype(np.float32)
    buffer = hoplite.Buffer.from_buffer(array)
    store.put(buffer, object_id)

    print("Buffer created, hash =", hash(buffer))
    object_ids = []
    for i in range(0, world_size):
        object_ids.append(hoplite.object_id_from_int(i))
    comm.Barrier()

    if rank == 0:
        buffers = []
        start = time.time()
        for object_id in object_ids:
            buffers.append(store.get(object_id))
        duration = time.time() - start
        print(f"Gather completed, hash = {list(map(hash, buffers))}, duration = {duration}")


def allgather(store, object_size):
    object_id = hoplite.object_id_from_int(rank)
    array = np.random.rand(object_size//4).astype(np.float32)
    buffer = hoplite.Buffer.from_buffer(array)
    store.put(buffer, object_id)

    print("Buffer created, hash =", hash(buffer))
    object_ids = []
    for i in range(0, world_size):
        object_ids.append(hoplite.object_id_from_int(i))
    comm.Barrier()

    buffers = []
    start = time.time()
    for object_id in object_ids:
        buffers.append(store.get(object_id))
    duration = time.time() - start
    print(f"AllGather completed, hash = {list(map(hash, buffers))}, duration = {duration}")


microbenchmark_names = ['roundtrip', 'multicast', 'reduce', 'allreduce', 'gather', 'allgather']
parser = argparse.ArgumentParser(description='Hoplite microbenchmark with the Python library.')
parser.add_argument('microbenchmark_name', type=str, choices=microbenchmark_names,
                    help='Name of the microbenchmark.')
parser.add_argument('-s', '--object-size', type=int, required=True, help='The size of the object')
args = parser.parse_args()
if args.microbenchmark_name not in microbenchmark_names:
    raise ValueError(f"Microbenchmark '{args.microbenchmark_name}' does not exist.")
elif args.microbenchmark_name == 'roundtrip':
    if world_size != 2:
        raise ValueError("For the roundtrip microbenchmark, the world_size must be 2.")

if rank == 0:
    hoplite.start_location_server()
    time.sleep(1)
    object_directory_address = hoplite.get_my_address()
else:
    object_directory_address = None

# broadcast object directory address
object_directory_address = comm.bcast(object_directory_address, root=0)
store = hoplite.HopliteClient(object_directory_address)

globals()[args.microbenchmark_name](store, args.object_size)

# avoid disconnecting from hoplite store before another one finishes
comm.Barrier()

# exit clients before the server to suppress connection error messages
del store
comm.Barrier()
