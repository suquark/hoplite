#!/usr/bin/env python3

import os
import ray
import sys

root_directory = os.path.dirname(os.path.abspath(__file__))
print(root_directory)

dirs = [
    os.path.join(root_directory, 'python'),
    os.path.join(root_directory, 'app', 'parameter-server'),
]

ray.init(address='auto', load_code_from_local=True)

ray.worker.global_worker.run_function_on_all_workers(
    lambda worker_info____: [sys.path.insert(1, d) for d in dirs])
