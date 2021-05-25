#!/usr/bin/env python3

import argparse
import os
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
        self.store = hoplite.HopliteClient(object_directory_address)
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
    def __init__(self, index, object_directory_address, model_type="custom", device="cpu", enable_fail=True):
        os.environ['RAY_BACKEND_LOG_LEVEL'] = 'info'
        self.store = hoplite.HopliteClient(object_directory_address)
        self.device = device
        self.model = ConvNet(model_type).to(device)

        if index == 2 and enable_fail:
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
args = parser.parse_args()

object_directory_address = hoplite.start_location_server()

ray.init(address='auto', ignore_reinit_error=True)

workers = []
for i in range(args.num_workers):
    workers.append(DataWorker.remote(i, object_directory_address, model_type=args.model, device='cuda'))

ps = ParameterServer.remote(object_directory_address, 1e-2, workers=workers, model_type=args.model)

# get initial weights
current_weights = ps.get_parameter_id.remote()

print("Running Asynchronous Parameter Server Training.")
step_start = time.time()
gradients = {}
index = 0

aliveness_map = {}

for worker in workers:
    gradient_id = hoplite.object_id_from_int(index)
    index += 1
    gradients[gradient_id] = (worker, worker.compute_gradients.remote(current_weights, gradient_id=gradient_id))
    aliveness_map[gradient_id] = worker.poll.remote()

backup_workers = {}

record = []

for i in range(args.iterations):
    event = ""
    # check recovery
    rejoined = []
    for worker_index, (w, p) in backup_workers.items():
        q, _ = ray.wait([p], num_returns=1, timeout=0)
        if q:
            event = "rejoin"
            print(f"worker {worker_index} rejoined!")
            workers[worker_index] = w
            gradient_id = hoplite.object_id_from_int(index)
            index += 1
            gradients[gradient_id] = (w, w.compute_gradients.remote(current_weights, gradient_id=gradient_id))
            aliveness_map[gradient_id] = w.poll.remote()
            rejoined.append(worker_index)
    for worker_index in rejoined:
        del backup_workers[worker_index]

    # check failure
    ready_poll_tasks, _ = ray.wait(list(aliveness_map.values()), num_returns=len(aliveness_map), timeout=0)
    for t in ready_poll_tasks:
        try:
            ray.get(t)
        except ray.exceptions.RayActorError:
            event = "fail"
            _inv_aliveness_map = {v:k for k,v in aliveness_map.items()}
            gradient_id = _inv_aliveness_map[t]
            del aliveness_map[gradient_id]
            failed_worker, _ = gradients.pop(gradient_id)
            worker_index = workers.index(failed_worker)
            print(f"worker {worker_index} failed. starting a new one...")
            new_worker = DataWorker.remote(worker_index, object_directory_address, model_type=args.model, device='cuda', enable_fail=False)
            backup_workers[worker_index] = (new_worker, new_worker.poll.remote())

    # actual iteration
    gradient_ids = list(gradients.keys())
    num_reduce_objects = min(args.num_async, len(gradients))
    current_weights, ready_gradient_list = ray.get(ps.apply_gradients.remote(gradient_ids, num_reduce_objects))
    for ready_gradient_id in ready_gradient_list:
        worker, _ = gradients.pop(ready_gradient_id)
        gradient_id = hoplite.object_id_from_int(index)
        index += 1
        gradients[gradient_id] = (worker, worker.compute_gradients.remote(current_weights, gradient_id=gradient_id))
        aliveness_map[gradient_id] = worker.poll.remote()
    now = time.time()
    print("step time:", now - step_start, flush=True)
    record.append({'duration': now - step_start, 'event': event})
    step_start = now

import json
with open("hoplite_asgd_fault_tolerance.json", "w") as f:
    json.dump(record, f)

# Clean up Ray resources and processes before the next example.
ray.shutdown()
