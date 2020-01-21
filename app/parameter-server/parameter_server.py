#!/usr/bin/env python3

import argparse
import os
import sys
from pathlib import Path
python_src = Path(__file__).resolve().parents[2] / 'python'

sys.path.append(str(python_src))

import torch
import torch.nn as nn
import torch.nn.functional as F

import numpy as np

import ray

import utils
import py_distributed_object_store as store_lib

from ps_helper import ConvNet, get_data_loader, evaluate
from parameter_server_remote import ParameterServer, DataWorker

parser = argparse.ArgumentParser(description='parameter server')
parser.add_argument('-n', '--num-workers', type=int, required=True,
                    help='number of parameter server workers')
utils.add_arguments(parser)

utils.start_location_server()
args = parser.parse_args()
args_dict = utils.extract_dict_from_args(args)

iterations = 200
num_workers = args.num_workers

ray.init(address='auto', ignore_reinit_error=True)
ps = ParameterServer.remote(args_dict, 1e-2)
workers = [DataWorker.remote() for i in range(args_dict, num_workers)]

model = ConvNet()
test_loader = get_data_loader()[1]

# get initial weights
current_weights = ps.get_weights.remote()

if not args.async:
    print("Running synchronous parameter server training.")
    for i in range(iterations):
        gradients = [
            worker.compute_gradients.remote(current_weights) for worker in workers
        ]
        # Calculate update after all gradients are available.
        current_weights = ps.apply_gradients.remote(*gradients)

        if i % 10 == 0:
            # Evaluate the current model.
            model.set_weights(ray.get(current_weights))
            accuracy = evaluate(model, test_loader)
            print("Iter {}: \taccuracy is {:.1f}".format(i, accuracy))
else:
    print("Running Asynchronous Parameter Server Training.")
    gradients = {}
    for worker in workers:
        gradients[worker.compute_gradients.remote(current_weights)] = worker

    for i in range(iterations * num_workers):
        ready_gradient_list, _ = ray.wait(list(gradients))
        ready_gradient_id = ready_gradient_list[0]
        worker = gradients.pop(ready_gradient_id)

        # Compute and apply gradients.
        current_weights = ps.apply_gradients.remote(*[ready_gradient_id])
        gradients[worker.compute_gradients.remote(current_weights)] = worker

        if i % 10 == 0:
            # Evaluate the current model after every 10 updates.
            model.set_weights(ray.get(current_weights))
            accuracy = evaluate(model, test_loader)
            print("Iter {}: \taccuracy is {:.1f}".format(i, accuracy))

print("Final accuracy is {:.1f}.".format(accuracy))
# Clean up Ray resources and processes before the next example.
ray.shutdown()
