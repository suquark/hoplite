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
import os
import torch
import torch.nn as nn
import torch.nn.functional as F

import numpy as np

from ray_parameter_server_remote import ParameterServer, DataWorker, ConvNet, get_data_loader

import ray


def evaluate(model, test_loader):
    """Evaluates the accuracy of the model on a validation dataset."""
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for batch_idx, (data, target) in enumerate(test_loader):
            # This is only set to finish evaluation faster.
            if batch_idx * len(data) > 1024:
                break
            outputs = model(data)
            _, predicted = torch.max(outputs.data, 1)
            total += target.size(0)
            correct += (predicted == target).sum().item()
    return 100. * correct / total


###########################################################################
# Synchronous Parameter Server Training
# -------------------------------------
# We'll now create a synchronous parameter server training scheme. We'll first
# instantiate a process for the parameter server, along with multiple
# workers.

iterations = 200
num_workers = 2

ray.init(address='auto', ignore_reinit_error=True)
ps = ParameterServer.remote(1e-2)
workers = [DataWorker.remote() for i in range(num_workers)]

###########################################################################
# We'll also instantiate a model on the driver process to evaluate the test
# accuracy during training.

model = ConvNet()
test_loader = get_data_loader()[1]

###########################################################################
# Training alternates between:
#
# 1. Computing the gradients given the current weights from the server
# 2. Updating the parameter server's weights with the gradients.

print("Running synchronous parameter server training.")
current_weights = ps.get_weights.remote()
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

print("Final accuracy is {:.1f}.".format(accuracy))
# Clean up Ray resources and processes before the next example.
ray.shutdown()

###########################################################################
# Asynchronous Parameter Server Training
# --------------------------------------
# We'll now create a synchronous parameter server training scheme. We'll first
# instantiate a process for the parameter server, along with multiple
# workers.

print("Running Asynchronous Parameter Server Training.")

ray.init(address='auto', ignore_reinit_error=True)
ps = ParameterServer.remote(1e-2)
workers = [DataWorker.remote() for i in range(num_workers)]

###########################################################################
# Here, workers will asynchronously compute the gradients given its
# current weights and send these gradients to the parameter server as
# soon as they are ready. When the Parameter server finishes applying the
# new gradient, the server will send back a copy of the current weights to the
# worker. The worker will then update the weights and repeat.

current_weights = ps.get_weights.remote()

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
