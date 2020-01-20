#!/usr/bin/env python3
import argparse
import subprocess
import os
import socket
import sys
import time

import ray

import utils

parser = argparse.ArgumentParser(description='broadcast test')
utils.add_arguments(parser)
parser.add_argument('-t', '--type-of-test', type=str, required=True,
                    help='Type of the test')
parser.add_argument('-n', '--world-size', type=int, required=True,
                    help='Size of the collective processing group')
parser.add_argument('-s', '--object-size', type=int, required=True,
                    help='The size of the object')

args = parser.parse_args()
args_dict = utils.extract_dict_from_args(args)

import test_functions

notification_p = subprocess.Popen(['../notification', utils.get_my_address()])
utils.register_cleanup([notification_p])

notification_address = utils.get_my_address()

current_directory = os.path.abspath(os.path.curdir)
print(current_directory)
ray.worker.global_worker.run_function_on_all_workers(
    lambda worker_info: sys.path.insert(1, current_directory), print(sys.path))

ray.init(address='auto', load_code_from_local=True)

# wait for the location server & the function run on all clients
time.sleep(20)

tasks = []

for rank in range(args.world_size):
    if args.type_of_test == 'ray-multicast':
        task_id = test_functions.ray_multicast.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    if args.type_of_test == 'ray-reduce':
        task_id = test_functions.ray_reduce.remote(args_dict, args.world_size, rank, args.object_size) 
    if args.type_of_test == 'ray-allreduce':
        task_id = test_functions.ray_allreduce.remote(args_dict, args.world_size, rank, args.object_size)
    if args.type_of_test == 'ray-gather':
        task_id = test_functions.ray_gather.remote(args_dict, args.world_size, rank, args.object_size)
    if args.type_of_test == 'ray-allgather':
        task_id = test_functions.ray_allgather.remote(args_dict, args.world_size, rank, args.object_size)
    if args.type_of_test == 'multicast':
        task_id = test_functions.multicast.remote(args_dict, rank, args.object_size)
    tasks.append(task_id)

ray.get(tasks)
