#!/usr/bin/env python3

import os
import ray
import sys

current_directory = os.path.abspath(os.path.curdir)
print(current_directory)

dirs = [
    os.path.join(current_directory, 'python'),
    os.path.join(current_directory, 'app', 'parameter-server'),
]

ray.init(address='auto', load_code_from_local=True)

ray.worker.global_worker.run_function_on_all_workers(
    lambda worker_info: [sys.path.insert(1, d) for d in dirs])
