
import argparse
import os
import time
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.distributed as dist

import numpy as np

from ps_helper import ConvNet, get_data_loader, evaluate, criterion

class DataWorker(object):
    def __init__(self, model_type="custom", device="cpu"):
        self.device = device
        self.model = ConvNet(model_type).to(device)
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=0.02)

    def compute_gradients(self, batch_size=128):
        data = torch.randn(batch_size, 3, 224, 224, device=self.device)
        self.model.zero_grad()
        output = self.model(data)
        loss = torch.mean(output)
        loss.backward()
        gradients = self.model.get_gradients()
        cont_grad = np.concatenate([p.ravel() for p in gradients])
        t = torch.from_numpy(cont_grad)
        torch.distributed.all_reduce(t)
        summed_gradients = self.model.buffer_to_tensors(t.numpy().view(np.uint8))
        self.optimizer.zero_grad()
        self.model.set_gradients(summed_gradients)
        self.optimizer.step()

parser = argparse.ArgumentParser(description='parameter server')
parser.add_argument('-m', '--model', type=str, default="custom",
                    help='neural network model type')
parser.add_argument('--rank', type=int)
parser.add_argument('--size', type=int)
parser.add_argument('--master_ip', type=str)
args = parser.parse_args()

dist.init_process_group('gloo', init_method=f"tcp://{args.master_ip}:12345", rank=args.rank, world_size=args.size)

iterations = 50

worker = DataWorker(model_type=args.model, device='cuda')
step_start = time.time()
for i in range(iterations):
    worker.compute_gradients()
    now = time.time()
    print("rank:", args.rank, "step time:", now - step_start, flush=True)
    step_start = now
