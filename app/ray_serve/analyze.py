import json
import numpy as np

with open('ray_serve_log.json', 'r') as f:
    ray_log = json.load(f)
    durations = [l['duration'] for l in ray_log if l['event'] == 'fail']
    print(f"Baseline latency caused by failure: {np.mean(durations):.6f} ± {np.std(durations):.6f}s")

with open('hoplite_ray_serve_log.json', 'r') as f:
    hoplite_log = json.load(f)
    durations = [l['duration'] for l in hoplite_log if l['event'] == 'fail']
    print(f"Hoplite latency caused by failure: {np.mean(durations):.6f} ± {np.std(durations):.6f}s")
