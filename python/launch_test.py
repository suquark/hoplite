#!/usr/bin/env python3
import argparse
import grpc
import subprocess
import numpy as np
import os
import socket
import sys
import time
import ray

import utils

import object_store_pb2
import object_store_pb2_grpc
import py_distributed_object_store as store_lib

notification_port = 7777

def register_group(notification_address, notification_port, world_size):
    channel = grpc.insecure_channel(notification_address + ':' + str(notification_port))
    stub = object_store_pb2_grpc.NotificationServerStub(channel)
    request = object_store_pb2.RegisterRequest(num_of_nodes=world_size)
    reply = stub.Register(request)
    return reply.ok

def is_ready(notification_address, notification_port, my_address):
    channel = grpc.insecure_channel(notification_address + ':' + str(notification_port))
    stub = object_store_pb2_grpc.NotificationServerStub(channel)
    request = object_store_pb2.IsReadyRequest(ip=str.encode(my_address))
    reply = stub.IsReady(request)
    return reply.ok

def barrier(world_rank, notification_address, notification_port, world_size):
    my_address = utils.get_my_address()
    is_ready(notification_address, notification_port, my_address)

def barrier_exit(world_rank, notification_address, notification_port):
    my_address = utils.get_my_address()
    channel = grpc.insecure_channel(notification_address + ':' + str(notification_port))
    stub = object_store_pb2_grpc.NotificationServerStub(channel)
    request = object_store_pb2.ExitRequest(ip=str.encode(my_address))
    reply = stub.Exit(request)
    return reply.ok

@ray.remote(resources={'node': 1})
def ray_sendrecv(args_dictt, notification_address, world_size, world_rank, object_size):
    object_id = ray.ObjectID(str(args_dict['seed']).encode().rjust(20, b'\0'))
    object_id2 = ray.ObjectID(str(args_dict['seed'] + 1).encode().rjust(20, b'\0'))
    if world_rank == 0:
        array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        barrier(world_rank, notification_address, notification_port, world_size)
        start = time.time()
        ray.worker.global_worker.put_object(array, object_id=object_id)
        ready_set, unready_set = ray.wait([object_id2], timeout=100)
        assert ready_set
        array = ray.get(object_id2)
        duration = time.time() - start
        buffer = store_lib.Buffer.from_buffer(array)
        print("Buffer created, hash =", hash(buffer))
        print("duration = ", duration)
    else:
        return_array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        barrier(world_rank, notification_address, notification_port, world_size)
        ready_set, unready_set = ray.wait([object_id], timeout=100)
        assert ready_set
        array = ray.get(object_id)
        ray.worker.global_worker.put_object(return_array, object_id=object_id2)
    barrier_exit(world_rank, notification_address, notification_port)
    ray.internal.free([object_id, object_id2])


@ray.remote(resources={'node': 1})
def ray_multicast(args_dict, notification_address, world_size, world_rank, object_size):
    object_id = ray.ObjectID(str(args_dict['seed']).encode().rjust(20, b'\0'))
    if world_rank == 0:
        array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        buffer = store_lib.Buffer.from_buffer(array)
        print("Buffer created, hash =", hash(buffer))
        ray.worker.global_worker.put_object(array, object_id=object_id)
        barrier(world_rank, notification_address, notification_port, world_size)
    else:
        barrier(world_rank, notification_address, notification_port, world_size)
        start = time.time()
        ready_set, unready_set = ray.wait([object_id], timeout=5)
        assert ready_set
        array = ray.get(object_id)
        duration = time.time() - start
        buffer = store_lib.Buffer.from_buffer(array)
        print("Buffer received, hash =", hash(buffer), "duration =", duration)
        print(array)
    barrier_exit(world_rank, notification_address, notification_port)
    ray.internal.free([object_id])


@ray.remote(resources={'node': 1})
def ray_reduce(args_dict, notification_address, world_size, world_rank, object_size):
    object_id = ray.ObjectID(str(args_dict['seed'] + world_rank).encode().rjust(20, b'\0'))
    array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
    ray.worker.global_worker.put_object(array, object_id=object_id)
    buffer = store_lib.Buffer.from_buffer(array)
    print("Buffer created, hash =", hash(buffer))
    if world_rank == 0:
        object_ids = []
        for i in range(0, world_size):
            object_ids.append(ray.ObjectID(str(args_dict['seed'] + i).encode().rjust(20, b'\0')))
        reduce_result = np.zeros(object_size//4, dtype=np.int32)
        barrier(world_rank, notification_address, notification_port, world_size)
        start = time.time()
        ready_set, unready_set = ray.wait(object_ids, num_returns=1, timeout=600)
        while True:
            assert ready_set
            array = ray.get(ready_set[0])
            reduce_result += array
            if not unready_set:
                break
            ready_set, unready_set = ray.wait(unready_set, num_returns=1, timeout=600)
        duration = time.time() - start
        buffer = store_lib.Buffer.from_buffer(reduce_result)
        print("Reduce completed, hash =", hash(buffer), "duration =", duration)
        print(reduce_result)
        ray.internal.free(object_ids)
    else:
        barrier(world_rank, notification_address, notification_port, world_size)

    barrier_exit(world_rank, notification_address, notification_port)
    if world_rank != 0:
        ray.internal.free([object_id])


@ray.remote(resources={'node': 1})
def ray_allreduce(args_dict, notification_address, world_size, world_rank, object_size):
    object_id = ray.ObjectID(str(args_dict['seed'] + world_rank).encode().rjust(20, b'\0'))
    reduce_id = ray.ObjectID(str(args_dict['seed'] + world_size).encode().rjust(20, b'\0'))
    array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
    ray.worker.global_worker.put_object(array, object_id=object_id)
    buffer = store_lib.Buffer.from_buffer(array)
    print("Buffer created, hash =", hash(buffer))
    if world_rank == 0:
        object_ids = []
        for i in range(0, world_size):
            object_ids.append(ray.ObjectID(str(args_dict['seed'] + i).encode().rjust(20, b'\0')))
        allreduce_result = np.zeros(object_size//4, dtype=np.int32)
        barrier(world_rank, notification_address, notification_port, world_size)
        start = time.time()
        ready_set, unready_set = ray.wait(object_ids, num_returns=1, timeout=100)
        while True:
            assert ready_set
            array = ray.get(ready_set[0])
            allreduce_result += array
            if not unready_set:
                break
            ready_set, unready_set = ray.wait(unready_set, num_returns=1, timeout=100)
        ray.worker.global_worker.put_object(allreduce_result, object_id=reduce_id)
        print("allreduce result is generated, size = ", allreduce_result.shape)
    else:
        barrier(world_rank, notification_address, notification_port, world_size)
        start = time.time()
        # this hack is to prevent RAY from failling when waiting for object
        # without this hack, ray will think the object will never be created
        if object_size >= 2**30:
            time.sleep(30)
        if object_size >= 2**29:
            time.sleep(8)
        allreduce_result = ray.get(reduce_id, timeout=100)
    duration = time.time() - start
    buffer = store_lib.Buffer.from_buffer(allreduce_result)
    print("Allreduce completed, hash =", hash(buffer), "duration =", duration)
    print(allreduce_result)
    barrier_exit(world_rank, notification_address, notification_port)
    ray.internal.free([reduce_id])
    if world_rank == 0:
        ray.internal.free(object_ids)
    else:
        ray.internal.free([object_id])
 

@ray.remote(resources={'node': 1})
def ray_gather(args_dict, notification_address, world_size, world_rank, object_size):
    object_id = ray.ObjectID(str(args_dict['seed'] + world_rank).encode().rjust(20, b'\0'))
    array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
    ray.worker.global_worker.put_object(array, object_id=object_id)
    buffer = store_lib.Buffer.from_buffer(array)
    print("Buffer created, hash =", hash(buffer))
    if world_rank == 0:
        object_ids = []
        for i in range(0, world_size):
            object_ids.append(ray.ObjectID(str(args_dict['seed'] + i).encode().rjust(20, b'\0')))
        barrier(world_rank, notification_address, notification_port, world_size)
        start = time.time()
        ready_set, unready_set = ray.wait(object_ids, num_returns=1, timeout=5)
        gather_result = []
        while True:
            assert ready_set
            array = ray.get(ready_set[0])
            gather_result.append(array)
            if not unready_set:
                break
            ready_set, unready_set = ray.wait(unready_set, num_returns=1, timeout=5)
        duration = time.time() - start
        hash_sum = 0
        for array in gather_result:
            buffer = store_lib.Buffer.from_buffer(array)
            hash_sum += hash(buffer)
        print("Gather completed, hash =", hash_sum, "duration =", duration)
    else:
        barrier(world_rank, notification_address, notification_port, world_size)
    barrier_exit(world_rank, notification_address, notification_port)

@ray.remote(resources={'node': 1})
def ray_allgather(args_dict, notification_address, world_size, world_rank, object_size):
    object_id = ray.ObjectID(str(args_dict['seed'] + world_rank).encode().rjust(20, b'\0'))
    array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
    ray.worker.global_worker.put_object(array, object_id=object_id)
    buffer = store_lib.Buffer.from_buffer(array)
    print("Buffer created, hash =", hash(buffer))
    barrier(world_rank, notification_address, notification_port, world_size)
    object_ids = []
    for i in range(0, world_size):
        object_ids.append(ray.ObjectID(str(args_dict['seed'] + i).encode().rjust(20, b'\0')))
    start = time.time()
    ready_set, unready_set = ray.wait(object_ids, num_returns=1, timeout=5)
    gather_result = []
    while True:
        assert ready_set
        array = ray.get(ready_set[0])
        gather_result.append(array)
        if not unready_set:
            break
        ready_set, unready_set = ray.wait(unready_set, num_returns=1, timeout=5)
    duration = time.time() - start
    hash_sum = 0
    for array in gather_result:
        buffer = store_lib.Buffer.from_buffer(array)
        hash_sum += hash(buffer)
    print("Allgather completed, hash =", hash_sum, "duration =", duration)
    barrier_exit(world_rank, notification_address, notification_port)


@ray.remote(resources={'node': 1})
def sendrecv(args_dict, notification_address, world_size, world_rank, object_size):
    store = utils.create_store_using_dict(args_dict)
    object_id = store_lib.ObjectID(b'\0' * 20)
    object_id2 = store_lib.ObjectID(b'\1' * 20)
    if world_rank == 0:
        array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        buffer = store_lib.Buffer.from_buffer(array)
        barrier(world_rank, notification_address, notification_port, world_size)
        start = time.time()
        store.put(buffer, object_id)
        buffer = store.get(object_id2)
        duration = time.time() - start
        print("Buffer created, hash =", hash(buffer))
        print("duration = ", duration)
    else:
        return_array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        return_buffer = store_lib.Buffer.from_buffer(return_array)
        barrier(world_rank, notification_address, notification_port, world_size)
        buffer = store.get(object_id)
        store.put(return_buffer, object_id2)
    barrier_exit(world_rank, notification_address, notification_port)

@ray.remote(resources={'node': 1})
def multicast(args_dict, notification_address, world_size, world_rank, object_size):
    store = utils.create_store_using_dict(args_dict)
    object_id = store_lib.ObjectID(b'\0' * 20)
    if world_rank == 0:
        array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        buffer = store_lib.Buffer.from_buffer(array)
        print("Buffer created, hash =", hash(buffer))
        store.put(buffer, object_id)
        barrier(world_rank, notification_address, notification_port, world_size)
    else:
        barrier(world_rank, notification_address, notification_port, world_size)
        start = time.time()
        buffer = store.get(object_id)
        duration = time.time() - start
        print("Buffer received, hash =", hash(buffer), "duration =", duration)
        array = np.frombuffer(buffer, dtype=np.int32)
        print(array)
    barrier_exit(world_rank, notification_address, notification_port)

@ray.remote(resources={'node': 1})
def reduce(args_dict, notification_address, world_size, world_rank, object_size):
    store = utils.create_store_using_dict(args_dict)
    object_id = utils.object_id_from_int(world_rank)
    array = np.random.rand(object_size//4).astype(np.float32)
    buffer = store_lib.Buffer.from_buffer(array)
    store.put(buffer, object_id)

    print("Buffer created, hash =", hash(buffer))
    if world_rank == 0:
        object_ids = []
        for i in range(0, world_size):
            object_ids.append(utils.object_id_from_int(i))
        barrier(world_rank, notification_address, notification_port, world_size)
        start = time.time()
        reduction_id = store.reduce_async(object_ids, store_lib.ReduceOp.SUM)
        reduced_buffer = store.get(reduction_id)
        duration = time.time() - start
        reduce_result = np.frombuffer(reduced_buffer)
        print("Reduce completed, hash =", hash(reduced_buffer), "duration =", duration)
        print(reduce_result)
    else:
        barrier(world_rank, notification_address, notification_port, world_size)
    barrier_exit(world_rank, notification_address, notification_port)

@ray.remote(resources={'node': 1})
def allreduce(args_dict, notification_address, world_size, world_rank, object_size):
    store = utils.create_store_using_dict(args_dict)
    object_id = utils.object_id_from_int(world_rank)
    array = np.random.rand(object_size//4).astype(np.float32)
    buffer = store_lib.Buffer.from_buffer(array)
    store.put(buffer, object_id)
    print("Buffer created, hash =", hash(buffer))
    object_ids = []
    for i in range(0, world_size):
        object_ids.append(utils.object_id_from_int(i))
    reduction_id = utils.object_id_from_int(99999999999)
    barrier(world_rank, notification_address, notification_port, world_size)

    start = time.time()
    if world_rank == 0:
        store.reduce_async(object_ids, store_lib.ReduceOp.SUM, reduction_id=reduction_id)
    reduced_buffer = store.get(reduction_id)
    duration = time.time() - start

    reduce_result = np.frombuffer(reduced_buffer)
    print("AllReduce completed, hash =", hash(reduced_buffer), "duration =", duration)
    print(reduce_result)
    barrier_exit(world_rank, notification_address, notification_port)


@ray.remote(resources={'node': 1})
def gather(args_dict, notification_address, world_size, world_rank, object_size):
    store = utils.create_store_using_dict(args_dict)
    object_id = utils.object_id_from_int(world_rank)
    array = np.random.rand(object_size//4).astype(np.float32)
    buffer = store_lib.Buffer.from_buffer(array)
    store.put(buffer, object_id)

    print("Buffer created, hash =", hash(buffer))
    object_ids = []
    for i in range(0, world_size):
        object_ids.append(utils.object_id_from_int(i))
    barrier(world_rank, notification_address, notification_port, world_size)

    buffers = []
    start = time.time()
    if world_rank == 0:
        for object_id in object_ids:
            buffers.append(store.get(object_id))
    duration = time.time() - start

    print("Gather completed, hash =", [hash(b) for b in buffers], "duration =", duration)
    barrier_exit(world_rank, notification_address, notification_port)


@ray.remote(resources={'node': 1})
def allgather(args_dict, notification_address, world_size, world_rank, object_size):
    store = utils.create_store_using_dict(args_dict)
    object_id = utils.object_id_from_int(world_rank)
    array = np.random.rand(object_size//4).astype(np.float32)
    buffer = store_lib.Buffer.from_buffer(array)
    store.put(buffer, object_id)

    print("Buffer created, hash =", hash(buffer))
    object_ids = []
    for i in range(0, world_size):
        object_ids.append(utils.object_id_from_int(i))
    barrier(world_rank, notification_address, notification_port, world_size)

    buffers = []
    start = time.time()
    for object_id in object_ids:
        buffers.append(store.get(object_id))
    duration = time.time() - start

    print("AllGather completed, hash =", [hash(b) for b in buffers], "duration =", duration)
    barrier_exit(world_rank, notification_address, notification_port)


parser = argparse.ArgumentParser(description='broadcast test')
utils.add_arguments(parser)
parser.add_argument('-t', '--type-of-test', type=str, required=True,
                    help='Type of the test')
parser.add_argument('-n', '--world-size', type=int, required=True,
                    help='Size of the collective processing group')
parser.add_argument('-s', '--object-size', type=int, required=True,
                    help='The size of the object')

args = parser.parse_args()
args_dict = utils.extract_dict_from_args(args)

utils.start_location_server()
notification_address = utils.get_my_address()

ray.init(address='auto')

tasks = []

time.sleep(1)
register_group(notification_address, notification_port, args.world_size)

args_dict['seed'] = np.random.randint(0, 2**30)

for rank in range(args.world_size):
    if args.type_of_test == 'ray-sendrecv':
        assert (args.world_size == 2)
        task_id = ray_sendrecv.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'ray-multicast':
        task_id = ray_multicast.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'ray-reduce':
        task_id = ray_reduce.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'ray-allreduce':
        task_id = ray_allreduce.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'ray-gather':
        task_id = ray_gather.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'ray-allgather':
        task_id = ray_allgather.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'sendrecv':
        assert (args.world_size == 2)
        task_id = sendrecv.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'multicast':
        task_id = multicast.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'reduce':
        task_id = reduce.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'allreduce':
        task_id = allreduce.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'gather':
        task_id = gather.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'allgather':
        task_id = allgather.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    else:
        raise ValueError(f"Test '{args.type_of_test}' not exists.")
    tasks.append(task_id)

ray.get(tasks)
