import time
import numpy as np
import ray


@ray.remote(resources={'machine': 1})
class RayBenchmarkWorker:
    def __init__(self, object_size):
        self.object_size = object_size
        self.payload = np.ones(object_size//4, dtype=np.float32)

    def poll(self):
        pass

    def send(self):
        return ray.put(self.payload)

    def recv(self, x):
        return ray.get(x)

    def recv2(self, x):
        return None


def ray_roundtrip(object_size):
    sender = RayBenchmarkWorker.remote(object_size)
    receiver = RayBenchmarkWorker.remote(object_size)
    ray.get([sender.poll.remote(), receiver.poll.remote()])
    start = time.time()
    ray.get(sender.recv2.remote(receiver.recv.remote(sender.send.remote())))
    return time.time() - start


REPEAT_TIMES = 5

def test_with_mean_std(object_size, repeat_times=REPEAT_TIMES):
    results = []
    for _ in range(repeat_times):
        duration = ray_roundtrip(object_size)
        results.append(duration)
    return np.mean(results), np.std(results)


if __name__ == "__main__":
    ray.init(address='auto')
    with open("ray-roundtrip.csv", "w") as f:
        for object_size in (2 ** 10, 2 ** 20, 2 ** 30):
            mean, std = test_with_mean_std(object_size)
            print(f"roundtrip: {object_size} {mean:.6f} ± {std:.6f}s")
            f.write(f"ray,{object_size},{mean},{std}\n")