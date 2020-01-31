#!/usr/bin/env python3

import ray

ray.init(address='auto')
@ray.remote(resources={'node': 1})
def check_env():
    import socket
    import sys
    print(socket.gethostbyname(socket.gethostname()), sys.path)
tasks = []

for _ in ray.nodes():
    tasks.append(check_env.remote())

ray.get(tasks)
