#!/usr/bin/env python3
import argparse
import grpc
import numpy as np
import time
import ray

import object_store_pb2
import object_store_pb2_grpc

import hoplite

notification_port = 7777

def barrier(notification_address, notification_port, world_size):
    channel = grpc.insecure_channel(notification_address + ':' + str(notification_port))
    stub = object_store_pb2_grpc.NotificationServerStub(channel)
    request = object_store_pb2.BarrierRequest(num_of_nodes=world_size)
    reply = stub.Barrier(request)


@ray.remote(resources={'machine': 1})
def sendrecv(object_directory_address, world_size, world_rank, object_size):
    store = hoplite.HopliteClient(object_directory_address)
    object_id = hoplite.ObjectID(b'\0' * 20)
    object_id2 = hoplite.ObjectID(b'\1' * 20)
    if world_rank == 0:
        array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        buffer = hoplite.Buffer.from_buffer(array)
        hash_s = hash(buffer)
        print("Buffer created, hash =", hash_s)
        barrier(object_directory_address, notification_port, world_size)
        start = time.time()
        store.put(buffer, object_id)
        buffer = store.get(object_id2)
        duration = time.time() - start
        hash_r =  hash(buffer)
        print("Buffer received, hash =", hash_r)
        print("duration = ", duration)
        assert hash_s == hash_r, "Hash mismatch!"
    else:
        barrier(object_directory_address, notification_port, world_size)
        buffer = store.get(object_id)
        store.put(buffer, object_id2)


@ray.remote(resources={'machine': 1})
def multicast(object_directory_address, world_size, world_rank, object_size):
    store = hoplite.HopliteClient(object_directory_address)
    object_id = hoplite.ObjectID(b'\0' * 20)
    if world_rank == 0:
        array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        buffer = hoplite.Buffer.from_buffer(array)
        print("Buffer created, hash =", hash(buffer))
        store.put(buffer, object_id)
        barrier(object_directory_address, notification_port, world_size)
    else:
        barrier(object_directory_address, notification_port, world_size)
        start = time.time()
        buffer = store.get(object_id)
        duration = time.time() - start
        print("Buffer received, hash =", hash(buffer), "duration =", duration)
        array = np.frombuffer(buffer, dtype=np.int32)
        print(array)


@ray.remote(resources={'machine': 1})
def reduce(object_directory_address, world_size, world_rank, object_size):
    store = hoplite.HopliteClient(object_directory_address)
    object_id = hoplite.object_id_from_int(world_rank)
    array = np.random.rand(object_size//4).astype(np.float32)
    buffer = hoplite.Buffer.from_buffer(array)
    store.put(buffer, object_id)

    print("Buffer created, hash =", hash(buffer))
    if world_rank == 0:
        object_ids = []
        for i in range(0, world_size):
            object_ids.append(hoplite.object_id_from_int(i))
        barrier(object_directory_address, notification_port, world_size)
        start = time.time()
        reduction_id = store.reduce_async(object_ids, hoplite.ReduceOp.SUM)
        reduced_buffer = store.get(reduction_id)
        duration = time.time() - start
        reduce_result = np.frombuffer(reduced_buffer)
        print("Reduce completed, hash =", hash(reduced_buffer), "duration =", duration)
        print(reduce_result)
    else:
        barrier(object_directory_address, notification_port, world_size)

@ray.remote(resources={'machine': 1})
def allreduce(object_directory_address, world_size, world_rank, object_size):
    store = hoplite.HopliteClient(object_directory_address)
    object_id = hoplite.object_id_from_int(world_rank)
    array = np.random.rand(object_size//4).astype(np.float32)
    buffer = hoplite.Buffer.from_buffer(array)
    store.put(buffer, object_id)
    print("Buffer created, hash =", hash(buffer))
    object_ids = []
    for i in range(0, world_size):
        object_ids.append(hoplite.object_id_from_int(i))
    reduction_id = hoplite.object_id_from_int(99999999999)
    barrier(object_directory_address, notification_port, world_size)

    start = time.time()
    if world_rank == 0:
        store.reduce_async(object_ids, hoplite.ReduceOp.SUM, reduction_id=reduction_id)
    reduced_buffer = store.get(reduction_id)
    duration = time.time() - start

    reduce_result = np.frombuffer(reduced_buffer)
    print("AllReduce completed, hash =", hash(reduced_buffer), "duration =", duration)
    print(reduce_result)


@ray.remote(resources={'machine': 1})
def gather(object_directory_address, world_size, world_rank, object_size):
    store = hoplite.HopliteClient(object_directory_address)
    object_id = hoplite.object_id_from_int(world_rank)
    array = np.random.rand(object_size//4).astype(np.float32)
    buffer = hoplite.Buffer.from_buffer(array)
    store.put(buffer, object_id)

    print("Buffer created, hash =", hash(buffer))
    object_ids = []
    for i in range(0, world_size):
        object_ids.append(hoplite.object_id_from_int(i))
    barrier(object_directory_address, notification_port, world_size)

    if world_rank == 0:
        buffers = []
        start = time.time()
        for object_id in object_ids:
            buffers.append(store.get(object_id))
        duration = time.time() - start

        print("Gather completed, hash =", [hash(b) for b in buffers], "duration =", duration)


@ray.remote(resources={'machine': 1})
def allgather(object_directory_address, world_size, world_rank, object_size):
    store = hoplite.HopliteClient(object_directory_address)
    object_id = hoplite.object_id_from_int(world_rank)
    array = np.random.rand(object_size//4).astype(np.float32)
    buffer = hoplite.Buffer.from_buffer(array)
    store.put(buffer, object_id)

    print("Buffer created, hash =", hash(buffer))
    object_ids = []
    for i in range(0, world_size):
        object_ids.append(hoplite.object_id_from_int(i))
    barrier(object_directory_address, notification_port, world_size)

    buffers = []
    start = time.time()
    for object_id in object_ids:
        buffers.append(store.get(object_id))
    duration = time.time() - start

    print("AllGather completed, hash =", [hash(b) for b in buffers], "duration =", duration)


parser = argparse.ArgumentParser(description='broadcast test')
parser.add_argument('-t', '--type-of-test', type=str, required=True,
                    help='Type of the test')
parser.add_argument('-n', '--world-size', type=int, required=True,
                    help='Size of the collective processing group')
parser.add_argument('-s', '--object-size', type=int, required=True,
                    help='The size of the object')

args = parser.parse_args()
hoplite.start_location_server()
object_directory_address = hoplite.get_my_address()

ray.init(address='auto')

tasks = []

time.sleep(1)

for rank in range(args.world_size):
    if args.type_of_test == 'sendrecv':
        assert (args.world_size == 2)
        task_id = sendrecv.remote(object_directory_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'multicast':
        task_id = multicast.remote(object_directory_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'reduce':
        task_id = reduce.remote(object_directory_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'allreduce':
        task_id = allreduce.remote(object_directory_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'gather':
        task_id = gather.remote(object_directory_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'allgather':
        task_id = allgather.remote(object_directory_address, args.world_size, rank, args.object_size)
    else:
        raise ValueError(f"Test '{args.type_of_test}' not exists.")
    tasks.append(task_id)

ray.get(tasks)
