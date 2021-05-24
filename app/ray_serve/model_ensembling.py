import argparse
import time

import ray
from ray import serve

import numpy as np
import requests

import torch
import torchvision.models as models
from efficientnet_pytorch import EfficientNet

input_shape = (64, 3, 256, 256)
served_models = (
    'efficientnet-b2', 'resnet34',
    'mobilenet_v2', 'alexnet',
    'shufflenet_v2_x0_5', 'shufflenet_v2_x1_0',
    'efficientnet-b1', 'squeezenet1_1')


@ray.remote(num_gpus=1)
class ModelWorker:
    def __init__(self, model_name):
        if model_name.startswith('efficientnet'):
            self.model = EfficientNet.from_name(model_name).cuda().eval()
        else:
            self.model = getattr(models, model_name)().cuda().eval()

    def inference(self, x):
        x = torch.from_numpy(x).cuda()
        with torch.no_grad():
            # with torch.cuda.amp.autocast():
            return self.model(x).cpu().numpy()


class InferenceHost:
    def __init__(self, scale=1):
        # Imagine this is a data labeling task, and the user have loaded a bunch of images,
        # cached in memory. Because it is interactive,
        # 1) The user can choose any set of images.
        # 2) The user can choose any set of models for ensembling.
        self.images = torch.rand(input_shape)

        self.models = []
        # models.quantization

        # torchvision has an issue of too much loading time of 'inception_v3'
        # VGG16 is super slow compared to other models.
        for _ in range(scale):
            for model_name in served_models:
                self.models.append(ModelWorker.remote(model_name))

    def __call__(self, request):
        # convert torch tensor to numpy speeds up Ray.
        # The original serialization of pytorch tensor would be way too slow.
        x = self.images.numpy()
        results = ray.get([m.inference.remote(x) for m in self.models])
        cls = np.argmax(sum(results), 1)
        return str(cls.tolist())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ray serve without hoplite')
    parser.add_argument("scale", type=int, default=1)
    args = parser.parse_args()

    ray.init(address='auto')
    client = serve.start()
    for ep in client.list_endpoints().keys():
        client.delete_endpoint(ep)
    for backend in client.list_backends():
        client.delete_backend(backend)

    # Form a backend from our class and connect it to an endpoint.
    client.create_backend("ray_backend", InferenceHost, args.scale, ray_actor_options={"num_gpus":1})
    client.create_endpoint("ray_endpoint", backend="ray_backend", route="/inference")

    # Query our endpoint in two different ways: from HTTP and from Python.
    # print(requests.get("http://127.0.0.1:8000/inference").json())

    # Warmup
    for _ in range(10):
        requests.get("http://127.0.0.1:8000/inference")

    durations = []
    for _ in range(100):
        start = time.time()
        requests.get("http://127.0.0.1:8000/inference")
        durations.append(1/(time.time() - start))

    print(f"Troughtput: {np.mean(durations):.6f} ± {np.std(durations):.6f} queries/s")
    # > {"count": 1}
    # print(ray.get(client.get_handle("ray_endpoint").remote()))
