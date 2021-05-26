import time
import numpy as np
import ray

import grpc
import object_store_pb2
import object_store_pb2_grpc

import hoplite


def barrier(notification_address, notification_port, world_size):
    channel = grpc.insecure_channel(notification_address + ':' + str(notification_port))
    stub = object_store_pb2_grpc.NotificationServerStub(channel)
    request = object_store_pb2.BarrierRequest(num_of_nodes=world_size)
    reply = stub.Barrier(request)


@ray.remote(resources={'machine': 1})
class RayBenchmarkWorker:
    def __init__(self, notification_address, object_size):
        self.notification_address = notification_address
        self.notification_port = 7777
        self.object_size = object_size
        self.payload = ray.put(np.ones(object_size//4, dtype=np.float32))

    def poll(self):
        pass

    def send(self):
        return ray.put(self.payload)

    def recv(self, x):
        return ray.get(x)


def ray_roundtrip(notification_address, object_size):
    sender = RayBenchmarkWorker.remote(notification_address, object_size)
    receiver = RayBenchmarkWorker.remote(notification_address, object_size)
    ray.get([sender.poll.remote(), receiver.poll.remote()])
    start = time.time()
    ray.get(sender.recv.remote(receiver.recv.remote(sender.send.remote())))
    return time.time() - start


REPEAT_TIMES = 5

def test_with_mean_std(notification_address, object_size, repeat_times=REPEAT_TIMES):
    results = []
    for _ in range(repeat_times):
        duration = ray_roundtrip(notification_address, object_size)
        results.append(duration)
    return np.mean(results), np.std(results)


if __name__ == "__main__":
    notification_address = hoplite.start_location_server()

    ray.init(address='auto')
    with open("ray-roundtrip.csv", "w") as f:
        for object_size in (2 ** 10, 2 ** 20, 2 ** 30):
            mean, std = test_with_mean_std(notification_address, object_size)
            print(f"roundtrip: {object_size} {mean:.6f} ± {std:.6f}s")
            f.write(f"{object_size},{mean},{std}\n")