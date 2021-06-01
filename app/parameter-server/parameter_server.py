#!/usr/bin/env python3

import argparse
import time

import numpy as np
import ray
import torch

import hoplite

from ps_helper import ConvNet


###########################################################################
# Defining the Parameter Server
# -----------------------------
#
# The parameter server will hold a copy of the model.
# During training, it will:
#
# 1. Receive gradients and apply them to its model.
#
# 2. Send the updated model back to the workers.
#
# The ``@ray.remote`` decorator defines a remote process. It wraps the
# ParameterServer class and allows users to instantiate it as a
# remote actor.


@ray.remote(num_gpus=1, resources={'machine': 1})
class ParameterServer(object):
    def __init__(self, object_directory_address, lr, model_type="custom"):
        self.store = hoplite.HopliteClient(object_directory_address)
        self.model = ConvNet(model_type)
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=lr)

    def apply_gradients(self, *gradients):
        reduced_gradient_id = self.store.reduce_async(gradients, hoplite.ReduceOp.SUM)
        grad_buffer = self.store.get(reduced_gradient_id)
        summed_gradients = self.model.buffer_to_tensors(grad_buffer)
        self.optimizer.zero_grad()
        self.model.set_gradients(summed_gradients)
        self.optimizer.step()
        return self.get_parameter_id()

    def get_parameter_id(self):
        new_parameters = [p.data.cpu().numpy() for p in self.model.parameters()]
        cont_p = np.concatenate([p.ravel().view(np.uint8) for p in new_parameters])
        buffer = hoplite.Buffer.from_buffer(cont_p)
        parameter_id = self.store.put(buffer)
        return parameter_id

    def get_weights(self):
        return self.model.get_weights()

    def set_parameters(self, parameter_id):
        parameter_buffer = self.store.get(parameter_id)
        parameters = self.model.buffer_to_tensors(parameter_buffer)
        self.model.set_parameters(parameters)


###########################################################################
# Defining the Worker
# -------------------
# The worker will also hold a copy of the model. During training. it will
# continuously evaluate data and send gradients
# to the parameter server. The worker will synchronize its model with the
# Parameter Server model weights.


@ray.remote(num_gpus=1, resources={'machine': 1})
class DataWorker(object):
    def __init__(self, object_directory_address, model_type="custom", device="cpu"):
        self.store = hoplite.HopliteClient(object_directory_address)
        self.device = device
        self.model = ConvNet(model_type).to(device)

    def compute_gradients(self, parameter_id, gradient_id=None, batch_size=128):
        parameter_buffer = self.store.get(parameter_id)
        parameters = self.model.buffer_to_tensors(parameter_buffer)
        self.model.set_parameters(parameters)

        data = torch.randn(batch_size, 3, 224, 224, device=self.device)
        self.model.zero_grad()
        output = self.model(data)
        loss = torch.mean(output)
        loss.backward()
        gradients = self.model.get_gradients()
        cont_g = np.concatenate([g.ravel().view(np.uint8) for g in gradients])
        buffer = hoplite.Buffer.from_buffer(cont_g)
        gradient_id = self.store.put(buffer, gradient_id)
        return gradient_id


parser = argparse.ArgumentParser(description='parameter server')
parser.add_argument('-a', '--num-async', type=int, default=None,
                    help='enable asynchronous training')
parser.add_argument('-n', '--num-workers', type=int, required=True,
                    help='number of parameter server workers')
parser.add_argument('-m', '--model', type=str, default="custom",
                    help='neural network model type')
args = parser.parse_args()

object_directory_address = hoplite.start_location_server()

iterations = 50
num_workers = args.num_workers

ray.init(address='auto', ignore_reinit_error=True)
ps = ParameterServer.remote(object_directory_address, 1e-2, model_type=args.model)
workers = [DataWorker.remote(
    object_directory_address, model_type=args.model, device='cuda') for _ in range(num_workers)]

# get initial weights
current_weights = ps.get_parameter_id.remote()

start = time.time()

if args.num_async is None:
    print("Running synchronous parameter server training.")
    step_start = time.time()
    for i in range(iterations):
        gradients = []
        for worker in workers:
            gradient_id = hoplite.random_object_id()
            gradients.append(gradient_id)
            worker.compute_gradients.remote(current_weights, gradient_id)
        # Calculate update after all gradients are available.
        current_weights = ps.apply_gradients.remote(*gradients)
        ray.wait([current_weights])
        now = time.time()
        print("step time:", now - step_start, flush=True)
        step_start = now

else:
    print("Running Asynchronous Parameter Server Training.")
    step_start = time.time()
    gradients = {}
    for worker in workers:
        gradients[worker.compute_gradients.remote(current_weights)] = worker

    for i in range(iterations):
        ready_gradient_list, _ = ray.wait(list(gradients), num_returns=min(args.num_async, len(gradients)))
        current_weights = ps.apply_gradients.remote(*ready_gradient_list)
        for ready_gradient_id in ready_gradient_list:
            worker = gradients.pop(ready_gradient_id)
            gradients[worker.compute_gradients.remote(current_weights)] = worker
        now = time.time()
        print("step time:", now - step_start, flush=True)
        step_start = now

# Clean up Ray resources and processes before the next example.
ray.shutdown()
