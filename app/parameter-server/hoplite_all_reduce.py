#!/usr/bin/env python3

import argparse
import os
import sys
import time

import torch
import torch.nn as nn
import torch.nn.functional as F

import numpy as np

import ray

import ray.rllib.utils.hoplite as hoplite
store_lib = hoplite.store_lib

from ps_helper import ConvNet, get_data_loader, evaluate, criterion


@ray.remote(num_gpus=1, resources={'machine': 1})
class DataWorker(object):
    def __init__(self, args_dict, rank, model_type="custom", device="cpu"):
        self.store = hoplite.utils.create_store_using_dict(args_dict)
        self.device = device
        self.model = ConvNet(model_type).to(device)
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=0.02)
        self.rank = rank
        self.is_master = hoplite.utils.get_my_address().encode() == args_dict['redis_address']

    def compute_gradients(self, gradient_id, gradient_ids, reduction_id, batch_size=128):
        start_time = time.time()
        data = torch.randn(batch_size, 3, 224, 224, device=self.device)
        self.model.zero_grad()
        output = self.model(data)
        loss = torch.mean(output)
        loss.backward()
        gradients = self.model.get_gradients()
        cont_g = np.concatenate([g.ravel().view(np.uint8) for g in gradients])
        buffer = hoplite.store_lib.Buffer.from_buffer(cont_g)
        gradient_id = self.store.put(buffer, gradient_id)
        if self.is_master:
            # print("i'm master and i start reduce")
            reduced_gradient_id = self.store.reduce_async(gradient_ids, hoplite.store_lib.ReduceOp.SUM, reduction_id)
        grad_buffer = self.store.get(reduction_id)
        summed_gradients = self.model.buffer_to_tensors(grad_buffer)
        self.optimizer.zero_grad()
        self.model.set_gradients(summed_gradients)
        self.optimizer.step()
        print(self.rank, "in actor time", time.time() - start_time)
        return None


parser = argparse.ArgumentParser(description='parameter server')
parser.add_argument('-n', '--num-workers', type=int, required=True,
                    help='number of parameter server workers')
parser.add_argument('-m', '--model', type=str, default="custom",
                    help='neural network model type')
hoplite.utils.add_arguments(parser)

hoplite.utils.start_location_server()
args = parser.parse_args()
args_dict = hoplite.utils.extract_dict_from_args(args)

iterations = 50
num_workers = args.num_workers

ray.init(address='auto', ignore_reinit_error=True)
workers = [DataWorker.remote(args_dict, i, model_type=args.model, device='cuda') for i in range(num_workers)]

print("Running synchronous parameter server training.")
step_start = time.time()
for i in range(iterations):
    gradients = []
    rediction_id = hoplite.utils.random_object_id()
    all_grad_ids = [hoplite.utils.random_object_id() for worker in workers]
    all_updates = []
    for grad_id, worker in zip(all_grad_ids, workers):
        all_updates.append(worker.compute_gradients.remote(grad_id, all_grad_ids, rediction_id))
    ray.get(all_updates)
    now = time.time()
    print("step time:", now - step_start, flush=True)
    step_start = now

# Clean up Ray resources and processes before the next example.
ray.shutdown()
