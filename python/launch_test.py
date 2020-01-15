#!/usr/bin/env python3
import argparse
import subprocess
import os
import sys
import time

import ray

import utils

parser = argparse.ArgumentParser(description='broadcast test')
utils.add_arguments(parser)
parser.add_argument('-n', '--world-size', type=int, required=True,
                    help='Size of the collective processing group')
parser.add_argument('-s', '--object-size', type=int, required=True,
                    help='The size of the object')

args = parser.parse_args()
args_dict = utils.extract_dict_from_args(args)

import multicast_test

notification_p = subprocess.Popen(['../notification', utils.get_my_address()])
utils.register_cleanup([notification_p])

current_directory = os.path.abspath(os.path.curdir)
ray.worker.global_worker.run_function_on_all_workers(
    lambda worker_info: sys.path.insert(1, current_directory))

ray.init(address='auto', load_code_from_local=True)

# wait for the location server & the function run on all clients
time.sleep(5)

tasks = []

for rank in range(args.world_size):
    task_id = multicast_test.multicast.remote(args_dict, rank, args.object_size)
    tasks.append(task_id)

ray.get(tasks)
