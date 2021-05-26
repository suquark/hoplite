import time

import numpy as np
from dask.distributed import Client


def round_trip(obj):
    return obj


def measure_round_trip(client, object_size):
    payload = np.empty(object_size, dtype=np.uint8)
    before = time.time()
    receiver = client.submit(round_trip, payload, workers=['Dask-1'])
    receiver.result()
    duration = time.time() - before
    return duration


def main():
    client = Client("127.0.0.1:8786")
    means = []
    stds = []
    for size in range(2**10, 2**20, 2**30):
        t = []
        for _ in range(5):
            duration = measure_round_trip(client, size)
            t.append(duration)
        means.append(np.mean(t))
        stds.append(np.std(t))

    # # Accumulate time for more precision.
    # duration = 0.0
    # for j in range(i + 1, i + 1 + 10):
    #     duration += func(client, world_size, object_size, j)
    # duration /= 10
    # print(duration)
    with open(f"dask-roundtrip.csv", "w") as f:
        f.write("1 KB, 1 MB, 1 GB, 1 KB (std), 1 MB (std), 1 GB (std)\n" +
                ",".join(map(str, means + stds)) + "\n")


if __name__ == "__main__":
    main()