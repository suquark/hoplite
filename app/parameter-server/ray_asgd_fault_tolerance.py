#!/usr/bin/env python3

"""
Parameter Server
================

The parameter server is a framework for distributed machine learning training.

In the parameter server framework, a centralized server (or group of server
nodes) maintains global shared parameters of a machine-learning model
(e.g., a neural network) while the data and computation of calculating
updates (i.e., gradient descent updates) are distributed over worker nodes.

.. image:: ../images/param_actor.png
    :align: center

Parameter servers are a core part of many machine learning applications. This
document walks through how to implement simple synchronous and asynchronous
parameter servers using Ray actors.

To run the application, first install some dependencies.

.. code-block:: bash

  pip install torch torchvision filelock

Let's first define some helper functions and import some dependencies.

"""
import argparse
import time
import torch

import numpy as np

from ps_helper import ConvNet

import ray


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
    def __init__(self, lr, model_type="custom"):
        self.model = ConvNet(model_type)
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=lr)

    def apply_gradients(self, *gradients):
        summed_gradients = [
            np.stack(gradient_zip).sum(axis=0)
            for gradient_zip in zip(*gradients)
        ]
        self.optimizer.zero_grad()
        self.model.set_gradients(summed_gradients)
        self.optimizer.step()
        return self.model.get_weights()

    def get_weights(self):
        return self.model.get_weights()


###########################################################################
# Defining the Worker
# -------------------
# The worker will also hold a copy of the model. During training. it will
# continuously evaluate data and send gradients
# to the parameter server. The worker will synchronize its model with the
# Parameter Server model weights.


@ray.remote(num_gpus=1, resources={'machine': 1})
class DataWorker(object):
    def __init__(self, index, model_type="custom", device="cpu", enable_fail=True):
        self.device = device
        self.model = ConvNet(model_type).to(device)

        if index == 2 and enable_fail:
            import threading
            def kill():
                for i in reversed(range(20)):
                    print(f"failing in {i+1} second(s)...")
                    time.sleep(1)
                import os
                os._exit(1)
            self.t = threading.Thread(target=kill)
            self.t.start()

    def poll(self):
        pass

    def compute_gradients(self, weights, batch_size=128):
        self.model.set_weights(weights)

        data = torch.randn(batch_size, 3, 224, 224, device=self.device)
        self.model.zero_grad()
        output = self.model(data)
        loss = torch.mean(output)
        loss.backward()
        return self.model.get_gradients()


parser = argparse.ArgumentParser(description='parameter server')
parser.add_argument('-a', '--num-async', type=int, required=True,
                    help='enable asynchronous training')
parser.add_argument('-n', '--num-workers', type=int, required=True,
                    help='number of parameter server workers')
parser.add_argument('-m', '--model', type=str, default="custom",
                    help='neural network model type')
parser.add_argument('--iterations', type=int, default=20, help='number of iterations')

args = parser.parse_args()

ray.init(address='auto', ignore_reinit_error=True)
ps = ParameterServer.remote(1e-2, model_type=args.model)
workers = []
for i in range(args.num_workers):
    workers.append(DataWorker.remote(i, model_type=args.model, device='cuda'))

# get initial weights
current_weights = ps.get_weights.remote()

start = time.time()

###########################################################################
# Asynchronous Parameter Server Training
# --------------------------------------
# We'll now create a synchronous parameter server training scheme. We'll first
# instantiate a process for the parameter server, along with multiple
# workers.

###########################################################################
# Here, workers will asynchronously compute the gradients given its
# current weights and send these gradients to the parameter server as
# soon as they are ready. When the Parameter server finishes applying the
# new gradient, the server will send back a copy of the current weights to the
# worker. The worker will then update the weights and repeat.
print("Running Asynchronous Parameter Server Training.")
step_start = time.time()

aliveness_map = {}

gradients = {}
for worker in workers:
    grad_ref = worker.compute_gradients.remote(current_weights)
    aliveness_map[grad_ref] = worker.poll.remote()
    gradients[grad_ref] = worker

backup_workers = {}

record = []

for i in range(args.iterations):
    event = ""
    # recovery check
    rejoined = []
    for index, (w, p) in backup_workers.items():
        q, _ = ray.wait([p], num_returns=1, timeout=0)
        if q:
            event = "rejoin"
            print(f"worker {index} rejoined!")
            workers[index] = w
            grad_ref = w.compute_gradients.remote(current_weights)
            aliveness_map[grad_ref] = w.poll.remote()
            gradients[grad_ref] = w
            rejoined.append(index)
    for index in rejoined:
        del backup_workers[index]

    # failure check
    while True:
        ready_gradient_list, _ = ray.wait(list(gradients), num_returns=min(args.num_async, len(gradients)))
        no_except = True
        for grad_ref in ready_gradient_list:
            try:
                ray.get(aliveness_map[grad_ref])
            except ray.exceptions.RayActorError:
                event = "fail"
                no_except = False
                worker = gradients.pop(grad_ref)
                worker_index = workers.index(worker)
                del aliveness_map[grad_ref]
                print(f"worker {worker_index} failed. starting a new one...")
                # start a new one
                new_worker = DataWorker.remote(worker_index, model_type=args.model, device='cuda', enable_fail=False)
                backup_workers[worker_index] = (new_worker, new_worker.poll.remote())
        if no_except:
            break

    # actual iteration
    current_weights = ps.apply_gradients.remote(*ready_gradient_list)
    for ready_gradient_id in ready_gradient_list:
        worker = gradients.pop(ready_gradient_id)
        grad_ref = worker.compute_gradients.remote(current_weights)
        aliveness_map[grad_ref] = worker.poll.remote()
        gradients[grad_ref] = worker

    now = time.time()
    print("step time:", now - step_start, flush=True)
    record.append({'duration': now - step_start, 'event': event})
    step_start = now

import json
with open("ray_asgd_fault_tolerance.json", "w") as f:
    json.dump(record, f)

# Clean up Ray resources and processes before the next example.
ray.shutdown()
