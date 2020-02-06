
import argparse
import os
import time
import torch
import torch.nn as nn
import torch.nn.functional as F

import numpy as np

from ps_helper import ConvNet, get_data_loader, evaluate, criterion

from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

class ParameterServer(object):
    def __init__(self, lr):
        self.model = ConvNet()
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=lr)

    def apply_gradients(self, grad_buffer):
        grad_buffer = grad_buffer.view(np.uint8)
        summed_gradients = self.model.buffer_to_tensors(grad_buffer)
        self.optimizer.zero_grad()
        self.model.set_gradients(summed_gradients)
        self.optimizer.step()
        return self.model.get_weights()

    def get_weights(self):
        parameters = self.model.buffer_to_tensors(parameter_buffer)
        self.model.set_parameters(parameters)
        new_parameters = [p.data.cpu().numpy() for p in self.model.parameters()]
        cont_p = np.concatenate([p.ravel() for p in new_parameters])
        return cont_p


class DataWorker(object):
    def __init__(self):
        self.model = ConvNet()
        self.data_iterator = iter(get_data_loader()[0])

    def compute_gradients(self, parameter_buffer):
        parameters = self.model.buffer_to_tensors(parameter_buffer)
        self.model.set_parameters(parameters)
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

iterations = 20

start = time.time()
if rank == 0:
    ps = ParameterServer(1e-2)
    for i in range(iterations):

else:
    worker = [DataWorker() for i in range(num_workers)]
    for i in range(iterations):

model = ConvNet()

# get initial weights
current_weights = ps.get_weights.remote()

step_start = time.time()
for i in range(iterations):
    gradients = [
        worker.compute_gradients.remote(current_weights) for worker in workers
    ]
    current_weights = ps.apply_gradients.remote(*gradients)
    ray.wait([current_weights])
    now = time.time()
    print("step time:", now - step_start)
    step_start = now

during = time.time() - start
print(f"during = {during}s")
