#!/usr/bin/env python3

import ray
import check_env_remote

ray.init(address='auto')

tasks = []

for _ in ray.nodes():
    tasks.append(check_env_remote.check_env.remote())

ray.get(tasks)