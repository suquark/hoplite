import numpy as np
import ray
import torch

import utils
import py_distributed_object_store as store_lib

from ps_helper import get_data_loader, ConvNet, criterion


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
        loss = criterion(output, target)
        loss.backward()
        gradients = self.model.get_gradients()
        gradient_ids = []
        for g in gradients:
            buffer = store_lib.Buffer.from_buffer(g)
            gradient_ids.append(self.store.put(buffer))
        return gradient_ids
