import numpy as np
import ray

import hoplite
import ray_microbenchmarks

REPEAT_TIMES = 5

def test_with_mean_std(test_name, notification_address, world_size, object_size,
                       repeat_times=REPEAT_TIMES):
    results = []
    for _ in range(repeat_times):
        test_case = ray_microbenchmarks.__dict__[test_name]
        duration = test_case(notification_address, world_size, object_size)
        results.append(duration)
    return np.mean(results), np.std(results)


if __name__ == "__main__":
    notification_address = hoplite.start_location_server()

    ray.init(address='auto')
    with open("ray-roundtrip.csv", "w") as f:
        world_size = 2
        for object_size in (2 ** 10, 2 ** 20, 2 ** 30):
            mean, std = test_with_mean_std('ray_roundtrip', notification_address, world_size, object_size)
            print(f"roundtrip: {object_size} {mean:.6f} ± {std:.6f}s")
            f.write(f"{object_size},{mean},{std}\n")
