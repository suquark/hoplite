import os
import torch
from torch import nn
from torch.nn import functional as F
from torchvision import datasets, transforms
from filelock import FileLock
import ray
import numpy as np

import utils
import py_distributed_object_store as store_lib


def get_data_loader():
    """Safely downloads data. Returns training/validation set dataloader."""
    mnist_transforms = transforms.Compose(
        [transforms.ToTensor(),
         transforms.Normalize((0.1307, ), (0.3081, ))])

    # We add FileLock here because multiple workers will want to
    # download data, and this may cause overwrites since
    # DataLoader is not threadsafe.
    with FileLock(os.path.expanduser("~/data.lock")):
        train_loader = torch.utils.data.DataLoader(
            datasets.MNIST(
                "~/data",
                train=True,
                download=True,
                transform=mnist_transforms),
            batch_size=128,
            shuffle=True)
        test_loader = torch.utils.data.DataLoader(
            datasets.MNIST("~/data", train=False, transform=mnist_transforms),
            batch_size=128,
            shuffle=True)
    return train_loader, test_loader


#######################################################################
# Setup: Defining the Neural Network
# ----------------------------------
#
# We define a small neural network to use in training. We provide
# some helper functions for obtaining data, including getter/setter
# methods for gradients and weights.


class ConvNet(nn.Module):
    """Small ConvNet for MNIST."""

    def __init__(self):
        super(ConvNet, self).__init__()
        self.conv1 = nn.Conv2d(1, 3, kernel_size=3)
        self.fc = nn.Linear(192, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 3))
        x = x.view(-1, 192)
        x = self.fc(x)
        return F.log_softmax(x, dim=1)

    def get_weights(self):
        return {k: v.cpu() for k, v in self.state_dict().items()}

    def set_weights(self, weights):
        self.load_state_dict(weights)

    def get_gradients(self):
        grads = []
        for p in self.parameters():
            grad = None if p.grad is None else p.grad.data.cpu().numpy()
            grads.append(grad)
        return grads

    def set_gradients(self, gradients):
        for g, p in zip(gradients, self.parameters()):
            if g is not None:
                p.grad = torch.from_numpy(g)


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
    def __init__(self, args_dict, lr):
        self.store = utils.create_store_using_dict(args_dict)
        self.model = ConvNet()
        self.weights_info = []
        for p in self.model.parameters():
            self.weights_info.append(
                (p.numel() * p.element_size(), tuple(p.shape)))
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=lr)

    def apply_gradients(self, *gradients):
        grouped_gradients = list(zip(*gradients))

        reduced_weights_ids = []
        for gradients_per_layer, (data_size, _) in zip(grouped_gradients, self.weights_info):
            reduced_weights_id = self.store.reduce_async(
                gradients_per_layer, data_size, store_lib.ReduceOp.SUM)
            reduced_weights_ids.append(reduced_weights_id)

        summed_gradients = []
        for reduction_id, (_, shape) in zip(reduced_weights_ids, self.weights_info):
            grad_buffer = self.store.get(reduction_id)
            summed_gradients.append(np.frombuffer(
                grad_buffer, dtype=np.float32).reshape(shape))

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
    def __init__(self, args_dict):
        self.store = utils.create_store_using_dict(args_dict)
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
        loss = F.nll_loss(output, target)
        loss.backward()
        gradients = self.model.get_gradients()
        gradient_ids = []
        for g in gradients:
            buffer = store_lib.Buffer.from_buffer(g)
            gradient_ids.append(self.store.put(buffer))
        return gradient_ids
