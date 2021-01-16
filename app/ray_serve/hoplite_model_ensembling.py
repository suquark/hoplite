import argparse
import sys
import time

import ray
from ray import serve

import numpy as np
import requests

import torch
import torchvision.models as models

sys.path.insert(0, "../../python")
import py_distributed_object_store as store_lib
import utils

input_shape = (128, 3, 256, 256)
served_models = (
    'resnet18', 'resnet34',
    'mobilenet_v2', 'alexnet',
    'shufflenet_v2_x0_5', 'shufflenet_v2_x1_0',
    'squeezenet1_0', 'squeezenet1_1')


@ray.remote(num_gpus=1)
class ModelWorker:
    def __init__(self, model_name, args_dict):
        self.model = getattr(models, model_name)().cuda().eval()
        self.store = utils.create_store_using_dict(args_dict)

    def inference(self, x_id):
        buffer = self.store.get(x_id)
        x = np.frombuffer(buffer, dtype=np.float32).reshape(input_shape)

        x = torch.from_numpy(x).cuda()
        with torch.no_grad():
            # with torch.cuda.amp.autocast():
            return self.model(x).cpu().numpy()


class InferenceHost:
    def __init__(self, args_dict):
        self.store = utils.create_store_using_dict(args_dict)

        # Imagine this is a data labeling task, and the user have loaded a bunch of images,
        # cached in memory. Because it is interactive,
        # 1) The user can choose any set of images.
        # 2) The user can choose any set of models for ensembling.
        self.images = torch.rand(input_shape)

        self.models = []
        # models.quantization

        # torchvision has an issue of too much loading time of 'inception_v3'
        # VGG16 is super slow compared to other models.
        for model_name in served_models:
            # TODO: distribute workers to different nodes.
            self.models.append(ModelWorker.remote(model_name, args_dict))
        self.request_id = 0

    def __call__(self, request):
        # convert torch tensor to numpy speeds up Ray.
        # The original serialization of pytorch tensor would be way too slow.
        x = self.images.numpy()

        object_id = utils.object_id_from_int(self.request_id)
        buffer = store_lib.Buffer.from_buffer(x)
        self.store.put(buffer, object_id)
        self.request_id += 1

        results = ray.get([m.inference.remote(object_id) for m in self.models])
        cls = np.argmax(sum(results), 1)
        return str(cls.tolist())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ray serve with hoplite')
    # We just use this to extract default configurations
    utils.add_arguments(parser)
    args = parser.parse_args()
    args_dict = utils.extract_dict_from_args(args)
    utils.start_location_server()

    ray.init(address='auto')
    client = serve.start()
    for ep in client.list_endpoints().keys():
        client.delete_endpoint(ep)
    for backend in client.list_backends():
        client.delete_backend(backend)

    # Form a backend from our class and connect it to an endpoint.
    client.create_backend("h_backend", InferenceHost, args_dict, ray_actor_options={"num_gpus":1})
    client.create_endpoint("h_endpoint", backend="h_backend", route="/inference")

    # Query our endpoint in two different ways: from HTTP and from Python.
    print(requests.get("http://127.0.0.1:8000/inference").json())

    # Warmup
    for _ in range(10):
        requests.get("http://127.0.0.1:8000/inference")

    durations = []
    for _ in range(100):
        start = time.time()
        requests.get("http://127.0.0.1:8000/inference")
        durations.append(1/(time.time() - start))

    print(f"{np.mean(durations):.6f} ± {np.std(durations):.6f} requests/s")
    print(ray.get(client.get_handle("h_endpoint").remote()))