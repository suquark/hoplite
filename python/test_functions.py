import argparse
import grpc
import subprocess
import numpy as np
import os
import time
import ray

import object_store_pb2
import object_store_pb2_grpc
import py_distributed_object_store as store_lib

import utils

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
    if world_rank == 0:
        register_group(notification_address, notification_port, world_size)
    else:
        # we must ensure that the master will register group first.
        time.sleep(30)
    is_ready(notification_address, notification_port, my_address)

@ray.remote(resources={'node': 1}, max_calls=1)
def ray_multicast(args_dict, notification_address, world_size, world_rank, object_size):
    store = utils.create_store_using_dict(args_dict)
    object_id = ray.ObjectID(str(args_dict['seed']).encode().rjust(20, b'\0'))
    if world_rank == 0:
        array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
        buffer = store_lib.Buffer.from_buffer(array)
        print("Buffer created, hash =", hash(buffer))
        ray.worker.global_worker.put_object(array, object_id=object_id)
        barrier(world_rank, notification_address, notification_port, world_size)
    else:
        time.sleep(20)
        barrier(world_rank, notification_address, notification_port, world_size)
        start = time.time()
        ready_set, unready_set = ray.wait([object_id], timeout=5)
        assert ready_set
        array = ray.get(object_id)
        during = time.time() - start
        buffer = store_lib.Buffer.from_buffer(array)
        print("Buffer received, hash =", hash(buffer), "during =", during)
        print(array)
    time.sleep(30)

@ray.remote(resources={'node': 1}, max_calls=1)
def ray_reduce(args_dict, notification_address, world_size, world_rank, object_size):
    store = utils.create_store_using_dict(args_dict)
    object_id = ray.ObjectID(str(args_dict['seed'] + world_rank).encode().rjust(20, b'\0'))
    time.sleep(5)
    array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
    ray.worker.global_worker.put_object(array, object_id=object_id)
    buffer = store_lib.Buffer.from_buffer(array)
    print("Buffer created, hash =", hash(buffer))
    time.sleep(5)
    if world_rank == 0:
        object_ids = []
        for i in range(0, world_size):
            object_ids.append(ray.ObjectID(str(args_dict['seed'] + i).encode().rjust(20, b'\0')))
        barrier(world_rank, notification_address, notification_port, world_size)
        start = time.time()
        ready_set, unready_set = ray.wait(object_ids, num_returns=1, timeout=600)
        first_object = True
        while True:
            assert ready_set
            array = ray.get(ready_set[0])
            if first_object:
                reduce_result = array
                first_object = False
            else:
                reduce_result = reduce_result + array
            if not unready_set:
                break
            ready_set, unready_set = ray.wait(unready_set, num_returns=1, timeout=600)
        duration = time.time() - start
        buffer = store_lib.Buffer.from_buffer(reduce_result)
        print("Reduce completed, hash =", hash(buffer), "duration =", duration)
        print(reduce_result)
    else:
        barrier(world_rank, notification_address, notification_port, world_size)

    time.sleep(30)

@ray.remote(resources={'node': 1}, max_calls=1)
def ray_allreduce(args_dict, notification_address, world_size, world_rank, object_size):
    store = utils.create_store_using_dict(args_dict)
    object_id = ray.ObjectID(str(args_dict['seed'] + world_rank).encode().rjust(20, b'\0'))
    reduce_id = ray.ObjectID(str(args_dict['seed'] + world_size).encode().rjust(20, b'\0'))
    time.sleep(5)
    array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
    ray.worker.global_worker.put_object(array, object_id=object_id)
    buffer = store_lib.Buffer.from_buffer(array)
    print("Buffer created, hash =", hash(buffer))
    time.sleep(5)
    barrier(world_rank, notification_address, notification_port, world_size)
    start = time.time()
    if world_rank == 0:
        object_ids = []
        for i in range(0, world_size):
            object_ids.append(ray.ObjectID(str(args_dict['seed'] + i).encode().rjust(20, b'\0')))
        ready_set, unready_set = ray.wait(object_ids, num_returns=1, timeout=5)
        first_object = True
        while True:
            assert ready_set
            array = ray.get(ready_set[0])
            if first_object:
                reduce_result = array
                first_object = False
            else:
                reduce_result = reduce_result + array
            if not unready_set:
                break
            ready_set, unready_set = ray.wait(unready_set, num_returns=1, timeout=5)
        ray.worker.global_worker.put_object(reduce_result, object_id=reduce_id)

    ready_set, unready_set = ray.wait([reduce_id], num_returns=1, timeout=20)
    allreduce_result = ray.get(ready_set[0])
    duration = time.time() - start
    buffer = store_lib.Buffer.from_buffer(allreduce_result)
    print("Allreduce completed, hash =", hash(buffer), "duration =", duration)
    print(allreduce_result)

    time.sleep(30)

@ray.remote(resources={'node': 1}, max_calls=1)
def ray_gather(args_dict, notification_address, world_size, world_rank, object_size):
    store = utils.create_store_using_dict(args_dict)
    object_id = ray.ObjectID(str(args_dict['seed'] + world_rank).encode().rjust(20, b'\0'))
    time.sleep(5)
    array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
    ray.worker.global_worker.put_object(array, object_id=object_id)
    buffer = store_lib.Buffer.from_buffer(array)
    print("Buffer created, hash =", hash(buffer))
    time.sleep(5)
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

    time.sleep(30)

@ray.remote(resources={'node': 1}, max_calls=1)
def ray_allgather(args_dict, notification_address, world_size, world_rank, object_size):
    store = utils.create_store_using_dict(args_dict)
    object_id = ray.ObjectID(str(args_dict['seed'] + world_rank).encode().rjust(20, b'\0'))
    time.sleep(5)
    array = np.random.randint(2**30, size=object_size//4, dtype=np.int32)
    ray.worker.global_worker.put_object(array, object_id=object_id)
    buffer = store_lib.Buffer.from_buffer(array)
    print("Buffer created, hash =", hash(buffer))
    time.sleep(5)
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

    time.sleep(30)

@ray.remote(resources={'node': 1}, max_calls=1)
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
        time.sleep(5)
        barrier(world_rank, notification_address, notification_port, world_size)
        start = time.time()
        buffer = store.get(object_id)
        during = time.time() - start
        print("Buffer received, hash =", hash(buffer), "during =", during)
        array = np.frombuffer(buffer, dtype=np.int32)
        print(array)
    time.sleep(20)

@ray.remote(resources={'node': 1}, max_calls=1)
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

    time.sleep(30)


@ray.remote(resources={'node': 1}, max_calls=1)
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
    time.sleep(30)


@ray.remote(resources={'node': 1}, max_calls=1)
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
    time.sleep(30)


@ray.remote(resources={'node': 1}, max_calls=1)
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
    time.sleep(30)
