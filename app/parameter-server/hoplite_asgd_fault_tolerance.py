#!/usr/bin/env python3

import argparse
import os
import sys
import time

import torch

import numpy as np
import ray

import hoplite
from ps_helper import ConvNet


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
    def __init__(self, args_dict, lr, workers, model_type="custom"):
        os.environ['RAY_BACKEND_LOG_LEVEL'] = 'info'
        self.store = hoplite.create_store_using_dict(args_dict)
        self.model = ConvNet(model_type)
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=lr)
        self.workers = workers

    def apply_gradients(self, gradients, num_reduce_objects):
        reduced_gradient_id = self.store.reduce_async(
            gradients, hoplite.ReduceOp.SUM, num_reduce_objects=num_reduce_objects)
        grad_buffer = self.store.get(reduced_gradient_id)
        ready_ids = self.store.get_reduced_objects(reduced_gradient_id)
        ready_ids.remove(reduced_gradient_id)  # should not include the reduction id

        summed_gradients = self.model.buffer_to_tensors(grad_buffer)
        self.optimizer.zero_grad()
        self.model.set_gradients(summed_gradients)
        self.optimizer.step()
        return self.get_parameter_id(), ready_ids

    def get_parameter_id(self):
        new_parameters = [p.data.cpu().numpy() for p in self.model.parameters()]
        cont_p = np.concatenate([p.ravel().view(np.uint8) for p in new_parameters])
        buffer = hoplite.Buffer.from_buffer(cont_p)
        parameter_id = self.store.put(buffer)
        return parameter_id

    def get_weights(self):
        return self.model.get_weights()

    def set_parameters(self, parameter_id):
        parameter_buffer = self.store.get(parameter_id)
        parameters = self.model.buffer_to_tensors(parameter_buffer)
        self.model.set_parameters(parameters)


###########################################################################
# Defining the Worker
# -------------------
# The worker will also hold a copy of the model. During training. it will
# continuously evaluate data and send gradients
# to the parameter server. The worker will synchronize its model with the
# Parameter Server model weights.


@ray.remote(num_gpus=1, resources={'machine': 1})
class DataWorker(object):
    def __init__(self, index, args_dict, model_type="custom", device="cpu"):
        os.environ['RAY_BACKEND_LOG_LEVEL'] = 'info'
        self.store = hoplite.create_store_using_dict(args_dict)
        self.device = device
        self.model = ConvNet(model_type).to(device)

        if index == 2:
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

    def compute_gradients(self, parameter_id, gradient_id=None, batch_size=128):
        parameter_buffer = self.store.get(parameter_id)
        parameters = self.model.buffer_to_tensors(parameter_buffer)
        self.model.set_parameters(parameters)

        data = torch.randn(batch_size, 3, 224, 224, device=self.device)
        self.model.zero_grad()
        output = self.model(data)
        loss = torch.mean(output)
        loss.backward()
        gradients = self.model.get_gradients()
        cont_g = np.concatenate([g.ravel().view(np.uint8) for g in gradients])
        buffer = hoplite.Buffer.from_buffer(cont_g)
        gradient_id = self.store.put(buffer, gradient_id)
        return gradient_id


parser = argparse.ArgumentParser(description='parameter server')
parser.add_argument('-a', '--num-async', type=int, required=True,
                    help='enable asynchronous training')
parser.add_argument('-n', '--num-workers', type=int, required=True,
                    help='number of parameter server workers')
parser.add_argument('-m', '--model', type=str, default="custom",
                    help='neural network model type')
parser.add_argument('--iterations', type=int, default=50, help='number of iterations')
hoplite.add_arguments(parser)

hoplite.start_location_server()
args = parser.parse_args()
args_dict = hoplite.extract_dict_from_args(args)

ray.init(address='auto', ignore_reinit_error=True)

workers = []
for i in range(args.num_workers):
    workers.append(DataWorker.remote(i, args_dict, model_type=args.model, device='cuda'))

ps = ParameterServer.remote(args_dict, 1e-2, workers=workers, model_type=args.model)

# get initial weights
current_weights = ps.get_parameter_id.remote()

print("Running Asynchronous Parameter Server Training.")
step_start = time.time()
gradients = {}
index = 0

for worker in workers:
    gradient_id = hoplite.object_id_from_int(index)
    index += 1
    gradients[gradient_id] = (worker, worker.compute_gradients.remote(current_weights, gradient_id=gradient_id))

for i in range(args.iterations):
    gradient_ids = list(gradients.keys())
    num_reduce_objects = min(args.num_async, len(gradients))
    current_weights, ready_gradient_list = ray.get(ps.apply_gradients.remote(gradient_ids, num_reduce_objects))
    for ready_gradient_id in ready_gradient_list:
        worker, _ = gradients.pop(ready_gradient_id)
        gradient_id = hoplite.object_id_from_int(index)
        index += 1
        gradients[gradient_id] = (worker, worker.compute_gradients.remote(current_weights, gradient_id=gradient_id))
    now = time.time()
    print("step time:", now - step_start, flush=True)
    step_start = now

# Clean up Ray resources and processes before the next example.
ray.shutdown()
