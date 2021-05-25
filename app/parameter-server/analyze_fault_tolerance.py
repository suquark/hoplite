import json
import numpy as np

with open('ray_asgd_fault_tolerance.json', 'r') as f:
    ray_log = json.load(f)
    durations = [l['duration'] for l in ray_log if l['event'] == 'fail']
    # we only fail once in the paper, so no calculating std here
    print(f"Baseline latency caused by failure: {np.mean(durations):.6f}s")

with open('hoplite_asgd_fault_tolerance.json', 'r') as f:
    hoplite_log = json.load(f)
    durations = [l['duration'] for l in hoplite_log if l['event'] == 'fail']
    # we only fail once in the paper, so no calculating std here
    print(f"Hoplite latency caused by failure: {np.mean(durations):.6f}s")
