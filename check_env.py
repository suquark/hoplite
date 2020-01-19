import ray
import check_env_remote

ray.init(address='auto')

tasks = []

for _ in range(5):
    tasks.append(check_env_remote.check_env.remote())

ray.get(tasks)
