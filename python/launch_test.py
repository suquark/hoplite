#!/usr/bin/env python3
import argparse
import subprocess
import numpy as np
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

os.system('../check_env.py')

import test_functions

utils.start_location_server()
notification_address = utils.get_my_address()

ray.init(address='auto', load_code_from_local=True)

tasks = []

args_dict['seed'] = np.random.randint(0, 2**30)

for rank in range(args.world_size):
    if args.type_of_test == 'ray-multicast':
        task_id = test_functions.ray_multicast.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'ray-reduce':
        task_id = test_functions.ray_reduce.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'ray-allreduce':
        task_id = test_functions.ray_allreduce.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'ray-gather':
        task_id = test_functions.ray_gather.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'ray-allgather':
        task_id = test_functions.ray_allgather.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'multicast':
        task_id = test_functions.multicast.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'reduce':
        task_id = test_functions.reduce.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'allreduce':
        task_id = test_functions.allreduce.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'gather':
        task_id = test_functions.gather.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    elif args.type_of_test == 'allgather':
        task_id = test_functions.allgather.remote(args_dict, notification_address, args.world_size, rank, args.object_size)
    else:
        raise ValueError(f"Test '{args.type_of_test}' not exists.")
    tasks.append(task_id)

ray.get(tasks)
