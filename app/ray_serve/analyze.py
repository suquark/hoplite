import json
import numpy as np

with open('ray_serve_log.json', 'w') as f:
    ray_log = json.load(f)
    durations = [l['duration'] for l in ray_log if l['event'] == 'fail']
    print(f"Ray Failure Latency: {np.mean(durations):.6f} ± {np.std(durations):.6f}s")

with open('hoplite_ray_serve_log.json', 'w') as f:
    hoplite_log = json.load(f)
    durations = [l['duration'] for l in hoplite_log if l['event'] == 'fail']
    print(f"Ray+Hoplite Failure Latency: {np.mean(durations):.6f} ± {np.std(durations):.6f}s")
