
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
    def __init__(self, lr, model_type="custom"):
        self.model = ConvNet(model_type)
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=lr)

    def apply_gradients(self):
        new_parameters = [p.data.cpu().numpy() for p in self.model.parameters()]
        cont_p = np.concatenate([p.ravel() for p in new_parameters])
        comm.Bcast(cont_p, root=0)
        zero_grad = np.zeros(self.model.n_param, dtype=np.float32)
        grad_buffer = np.empty(self.model.n_param, dtype=np.float32)
        comm.Reduce(zero_grad, grad_buffer, op=MPI.SUM, root=0)
        summed_gradients = self.model.buffer_to_tensors(grad_buffer.view(np.uint8))
        self.optimizer.zero_grad()
        self.model.set_gradients(summed_gradients)
        self.optimizer.step()


class DataWorker(object):
    def __init__(self, model_type="custom", device="cpu"):
        self.device = device
        self.model = ConvNet(model_type).to(device)

    def compute_gradients(self, batch_size=128):
        parameter_buffer = np.empty(self.model.n_param, dtype=np.float32)
        comm.Bcast(parameter_buffer, root=0)
        parameters = self.model.buffer_to_tensors(parameter_buffer.view(np.uint8))
        self.model.set_parameters(parameters)
        data = torch.randn(batch_size, 3, 224, 224, device=self.device)
        self.model.zero_grad()
        output = self.model(data)
        loss = torch.mean(output)
        loss.backward()
        gradients = self.model.get_gradients()
        cont_grad = np.concatenate([p.ravel() for p in gradients])
        grad_buffer = np.empty(self.model.n_param, dtype=np.float32)
        comm.Reduce(cont_grad, grad_buffer, op=MPI.SUM, root=0)

parser = argparse.ArgumentParser(description='parameter server')
parser.add_argument('-m', '--model', type=str, default="custom",
                    help='neural network model type')
args = parser.parse_args()


iterations = 50


if rank == 0:
    print("rank == 0")
    ps = ParameterServer(1e-2, model_type=args.model)
    step_start = time.time()
    for i in range(iterations):
        ps.apply_gradients()
        now = time.time()
        print("step time:", now - step_start, flush=True)
        step_start = now

else:
    print("rank > 0")
    worker = DataWorker(model_type=args.model, device='cuda')
    for i in range(iterations):
        worker.compute_gradients()
