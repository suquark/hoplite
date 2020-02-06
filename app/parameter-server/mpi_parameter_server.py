
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

    def apply_gradients(self):
        new_parameters = [p.data.cpu().numpy() for p in self.model.parameters()]
        cont_p = np.concatenate([p.ravel() for p in new_parameters])
        comm.Bcast(cont_p, root=0)
        zero_grad = np.zeros(self.model.n_param, dtype=np.float32)
        grad_buffer = np.empty(self.model.n_param, dtype=np.float32)
        comm.Reduce(zero_grad, grad_buffer, op=MPI.SUM, root=0)
        grad_buffer = grad_buffer.view(np.uint8)
        summed_gradients = self.model.buffer_to_tensors(grad_buffer)
        self.optimizer.zero_grad()
        self.model.set_gradients(summed_gradients)
        self.optimizer.step()


class DataWorker(object):
    def __init__(self):
        self.model = ConvNet()
        self.data_iterator = iter(get_data_loader()[0])

    def compute_gradients(self):
        parameter_buffer = np.empty(self.model.n_param, dtype=np.float32)
        comm.Bcast(parameter_buffer, root=0)
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
        gradients = self.model.get_gradients()
        cont_grad = np.concatenate([p.ravel() for p in gradients])
        grad_buffer = np.empty(self.model.n_param, dtype=np.float32)
        comm.Reduce(cont_grad, grad_buffer, op=MPI.SUM, root=0)

iterations = 20


if rank == 0:
    ps = ParameterServer(1e-2)
    step_start = time.time()
    for i in range(iterations):
        ps.apply_gradients()
        now = time.time()
        print("step time:", now - step_start)
        step_start = now

else:
    worker = DataWorker()
    for i in range(iterations):
        worker.compute_gradients()