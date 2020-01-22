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
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=lr)

    def apply_gradients(self, *gradients):
        reduced_gradient_id = self.store.reduce_async(
            gradients, self.model.total_gradient_size, store_lib.ReduceOp.SUM)
        grad_buffer = self.store.get(reduced_gradient_id)
        summed_gradients = self.model.buffer_to_tensors(grad_buffer)
        self.optimizer.zero_grad()
        self.model.set_gradients(summed_gradients)
        self.optimizer.step()
        return self.get_parameter_id()

    def get_parameter_id(self):
        new_parameters = [p.cpu().numpy() for p in self.model.parameters()]
        cont_p = np.concatenate([p.ravel().view(np.uint8) for p in new_parameters])
        buffer = store_lib.Buffer.from_buffer(cont_p)
        parameter_id = self.store.put(buffer)
        return parameter_id

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

    def compute_gradients(self, parameter_id):
        parameter_buffer = self.store.get(parameter_id)
        parameters = self.model.buffer_to_tensors(parameter_buffer)
        for w, p in zip(self.model.parameters(), parameters):
            w.data = torch.from_numpy(p)
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
        cont_g = np.concatenate([g.ravel().view(np.uint8) for g in gradients])
        buffer = store_lib.Buffer.from_buffer(cont_g)
        gradient_id = self.store.put(buffer)
        return gradient_id
