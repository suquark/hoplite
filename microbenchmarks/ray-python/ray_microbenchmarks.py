import time
import numpy as np
import ray

import grpc
import object_store_pb2
import object_store_pb2_grpc


def barrier(notification_address, notification_port, world_size):
    channel = grpc.insecure_channel(notification_address + ':' + str(notification_port))
    stub = object_store_pb2_grpc.NotificationServerStub(channel)
    request = object_store_pb2.BarrierRequest(num_of_nodes=world_size)
    reply = stub.Barrier(request)


@ray.remote(resources={'machine': 1})
class RayBenchmarkWorker:
    def __init__(self, notification_address, world_size, world_rank, object_size):
        self.notification_address = notification_address
        self.notification_port = 7777
        self.world_size = world_size
        self.world_rank = world_rank
        self.object_size = object_size

    def barrier(self):
        # We sleep for a while to make sure all later
        # function calls are already queued in the execution queue.
        # This will make timing more precise.
        time.sleep(1)
        barrier(self.notification_address, self.notification_port, self.world_size)

    def put_object(self):
        return ray.put(np.ones(self.object_size//4, dtype=np.float32))

    def get_and_put_object(self, object_id):
        """This method is specifically for round trip"""
        return ray.put(ray.get(object_id))

    def get_objects(self, object_ids):
        object_ids = ray.get(object_ids)
        start = time.time()
        _ = ray.get(object_ids)
        duration = time.time() - start
        return duration

    def get_objects_with_creation_time(self, object_ids):
        start = time.time()
        object_ids = ray.get(object_ids)
        _ = ray.get(object_ids)
        duration = time.time() - start
        return duration

    @ray.method(num_returns=2)
    def reduce_objects(self, object_ids):
        object_ids = ray.get(object_ids)
        start = time.time()
        reduce_result = np.zeros(self.object_size//4, dtype=np.float32)
        for object_id in object_ids:
            array = ray.get(object_id)
            reduce_result += array
        duration = time.time() - start
        result_id = ray.put(reduce_result)
        return result_id, duration


class RayBenchmarkActorPool:
    def __init__(self, notification_address, world_size, object_size):
        self.actors = []
        for world_rank in range(world_size):
            self.actors.append(
                RayBenchmarkWorker.remote(notification_address, world_size, world_rank, object_size))

    def barrier(self):
        return [w.barrier.remote() for w in self.actors]

    def prepare_object(self, rank):
        object_id = self.actors[rank].put_object.remote()
        # wait until we have put that object
        ray.wait([object_id], num_returns=1, timeout=None)
        return object_id

    def prepare_objects(self):
        object_ids = [w.put_object.remote() for w in self.actors]
        # wait until we put all objects
        ray.wait(object_ids, num_returns=len(object_ids), timeout=None)
        return object_ids

    def __getitem__(self, k):
        return self.actors[k]

    def __del__(self):
        for w in self.actors:
            ray.kill(w)
    
    def __len__(self):
        return len(self.actors)


def ray_roundtrip(notification_address, world_size, object_size):
    actor_pool = RayBenchmarkActorPool(notification_address, world_size, object_size)
    object_id = actor_pool.prepare_object(rank=0)
    object_id = actor_pool[1].get_and_put_object.remote(object_id)
    return ray.get(actor_pool[0].get_objects.remote([object_id]))


def ray_multicast(notification_address, world_size, object_size):
    actor_pool = RayBenchmarkActorPool(notification_address, world_size, object_size)
    object_id = actor_pool.prepare_object(rank=0)
    durations = ray.get([w.get_objects.remote([object_id]) for w in actor_pool.actors])
    return max(durations)


def ray_reduce(notification_address, world_size, object_size):
    actor_pool = RayBenchmarkActorPool(notification_address, world_size, object_size)
    object_ids = actor_pool.prepare_objects()
    reduction_id, duration_id = actor_pool[0].reduce_objects.remote(object_ids)
    return ray.get(duration_id)


# TODO: the timing is not precise
def ray_allreduce(notification_address, world_size, object_size):
    actor_pool = RayBenchmarkActorPool(notification_address, world_size, object_size)
    object_ids = actor_pool.prepare_objects()
    actor_pool.barrier()
    reduction_id, duration_id = actor_pool[0].reduce_objects.remote(object_ids)
    results = [duration_id]
    for i in range(1, len(actor_pool)):
        results.append(actor_pool[i].get_objects_with_creation_time.remote([reduction_id]))
    durations = ray.get(results)
    return max(durations)


def ray_gather(notification_address, world_size, object_size):
    actor_pool = RayBenchmarkActorPool(notification_address, world_size, object_size)
    object_ids = actor_pool.prepare_objects()
    return ray.get(actor_pool[0].get_objects.remote(object_ids))


# TODO: the timing is not precise
def ray_allgather(notification_address, world_size, object_size):
    actor_pool = RayBenchmarkActorPool(notification_address, world_size, object_size)
    object_ids = actor_pool.prepare_objects()
    actor_pool.barrier()
    results = [w.get_objects.remote(object_ids) for w in actor_pool.actors]
    durations = ray.get(results)
    return max(durations)
