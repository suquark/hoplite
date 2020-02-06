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
import os
import time
import torch
import torch.nn as nn
import torch.nn.functional as F

import numpy as np

from ps_helper import ConvNet, get_data_loader, evaluate, criterion

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


@ray.remote(resources={'node': 1})
class ParameterServer(object):
    def __init__(self, lr):
        self.model = ConvNet()
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


@ray.remote(resources={'node': 1})
class DataWorker(object):
    def __init__(self):
        self.model = ConvNet()
        self.data_iterator = iter(get_data_loader()[0])

    def compute_gradients(self, weights):
        self.model.set_weights(weights)
        try:
            data, target = next(self.data_iterator)
        except StopIteration:  # When the epoch ends, start a new epoch.
            self.data_iterator = iter(get_data_loader()[0])
            data, target = next(self.data_iterator)
        self.model.zero_grad()
        output = self.model(data)
        loss = criterion(output, target)
        loss.backward()
        return self.model.get_gradients()


parser = argparse.ArgumentParser(description='parameter server')
parser.add_argument('-a', '--num-async', type=int, default=None,
                    help='enable asynchronous training')
parser.add_argument('-n', '--num-workers', type=int, required=True,
                    help='number of parameter server workers')
parser.add_argument('--no-test', action='store_true',
                    help='skip all tests except the last one')

args = parser.parse_args()
iterations = 50
num_workers = args.num_workers

ray.init(address='auto', ignore_reinit_error=True)
ps = ParameterServer.remote(1e-2)
workers = [DataWorker.remote() for i in range(num_workers)]

model = ConvNet()
test_loader = get_data_loader()[1]

# get initial weights
current_weights = ps.get_weights.remote()

start = time.time()

if args.num_async is None:
    ###########################################################################
    # Synchronous Parameter Server Training
    # -------------------------------------
    # We'll now create a synchronous parameter server training scheme. We'll first
    # instantiate a process for the parameter server, along with multiple
    # workers.

    ###########################################################################
    # We'll also instantiate a model on the driver process to evaluate the test
    # accuracy during training.

    ###########################################################################
    # Training alternates between:
    #
    # 1. Computing the gradients given the current weights from the server
    # 2. Updating the parameter server's weights with the gradients.

    print("Running synchronous parameter server training.")
    step_start = time.time()
    for i in range(iterations):
        gradients = [
            worker.compute_gradients.remote(current_weights) for worker in workers
        ]
        # Calculate update after all gradients are available.
        current_weights = ps.apply_gradients.remote(*gradients)
        ray.wait([current_weights])
        now = time.time()
        print("step time:", now - step_start)
        step_start = now

        if i % 10 == 0 and not args.no_test:
            # Evaluate the current model.
            model.set_weights(ray.get(current_weights))
            accuracy = evaluate(model, test_loader)
            print("Iter {}: \taccuracy is {:.1f}".format(i, accuracy))
else:
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

    gradients = {}
    for worker in workers:
        gradients[worker.compute_gradients.remote(current_weights)] = worker

    for i in range(iterations * num_workers):
        ready_gradient_list, _ = ray.wait(list(gradients), num_returns=min(args.num_async, len(gradients)))
        current_weights = ps.apply_gradients.remote(*ready_gradient_list)
        for ready_gradient_id in ready_gradient_list:
            worker = gradients.pop(ready_gradient_id)
            gradients[worker.compute_gradients.remote(current_weights)] = worker
        now = time.time()
        print("step time:", now - step_start)
        step_start = now

        if i % 10 == 0 and not args.no_test:
            # Evaluate the current model after every 10 updates.
            model.set_weights(ray.get(current_weights))
            accuracy = evaluate(model, test_loader)
            print("Iter {}: \taccuracy is {:.1f}".format(i, accuracy))

model.set_weights(ray.get(current_weights))
during = time.time() - start
accuracy = evaluate(model, test_loader)
print("Final accuracy is {:.1f}.".format(accuracy), f"during = {during}s")
# Clean up Ray resources and processes before the next example.
ray.shutdown()

##############################################################################
# Final Thoughts
# --------------
#
# This approach is powerful because it enables you to implement a parameter
# server with a few lines of code as part of a Python application.
# As a result, this simplifies the deployment of applications that use
# parameter servers and to modify the behavior of the parameter server.
#
# For example, sharding the parameter server, changing the update rule,
# switch between asynchronous and synchronous updates, ignoring
# straggler workers, or any number of other customizations,
# will only require a few extra lines of code.
