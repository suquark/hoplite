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

    def warmup(self):
        pass

    def barrier(self):
        barrier(self.notification_address, self.notification_port, self.world_size)

    def put_object(self):
        return np.random.rand(self.object_size//4).astype(np.float32)

    def get_objects(self, object_ids):
        self.barrier()
        start = time.time()
        ray.get(object_ids)
        during = time.time() - start
        return during

    @ray.method(num_return_vals=2)
    def reduce_objects(self, object_ids):
        self.barrier()
        start = time.time()
        reduce_result = np.zeros(self.object_size//4, dtype=np.float32)
        for object_id in object_ids:
            array = ray.get(object_id)
            reduce_result += array
        during = time.time() - start
        return reduce_result, during


class RayBenchmarkActorPool:
    def __init__(self, notification_address, world_size, object_size):
        self.actors = []
        for world_rank in range(world_size):
            self.actors.append(
                RayBenchmarkWorker.remote(notification_address, world_size, world_rank, object_size))
    
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


def ray_multicast(notification_address, world_size, object_size):
    actor_pool = RayBenchmarkActorPool(notification_address, world_size, object_size)
    object_id = actor_pool[0].put_object.remote()
    # wait until we have put that object
    ray.wait([object_id], num_returns=1, timeout=None)
    durings = ray.get([w.get_objects.remote([object_id]) for w in actor_pool.actors])
    return max(durings)


def ray_reduce(notification_address, world_size, object_size):
    actor_pool = RayBenchmarkActorPool(notification_address, world_size, object_size)
    object_ids = actor_pool.prepare_objects()
    reduction_id, during_id = actor_pool[0].reduce_objects.remote(object_ids)
    results = [reduction_id]
    for i in range(1, len(actor_pool)):
        results.append(actor_pool[i].barrier.remote())
    ray.wait(results, num_returns=len(results), timeout=None)
    return ray.get(during_id)


def ray_allreduce(notification_address, world_size, object_size):
    actor_pool = RayBenchmarkActorPool(notification_address, world_size, object_size)
    object_ids = actor_pool.prepare_objects()
    reduction_id, during_id = actor_pool[0].reduce_objects.remote(object_ids)
    results = [during_id]
    for i in range(1, len(actor_pool)):
        results.append(actor_pool[i].get_objects.remote([reduction_id]))
    durings = ray.get(results)
    return max(durings)


def ray_gather(notification_address, world_size, object_size):
    actor_pool = RayBenchmarkActorPool(notification_address, world_size, object_size)
    object_ids = actor_pool.prepare_objects()
    during_id = actor_pool[0].get_objects.remote(object_ids)
    results = [during_id]
    for i in range(1, len(actor_pool)):
        results.append(actor_pool[i].barrier.remote())
    ray.wait(results, num_returns=len(results), timeout=None)
    return ray.get(during_id)


def ray_allgather(notification_address, world_size, object_size):
    actor_pool = RayBenchmarkActorPool(notification_address, world_size, object_size)
    object_ids = actor_pool.prepare_objects()
    results = [w.get_objects.remote(object_ids) for w in actor_pool.actors]
    durings = ray.get(results, num_returns=len(results), timeout=None)
    return max(durings)
