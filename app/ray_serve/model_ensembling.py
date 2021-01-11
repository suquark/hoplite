import ray
from ray import serve
import torchvision.models as models
import requests
import torch
import numpy as np
import time


@ray.remote(num_gpus=1)
class ModelWorker:
    def __init__(self, model_name):
        self.model = getattr(models, model_name)().cuda().eval()

    def inference(self, x):
        x = torch.from_numpy(x).cuda()
        with torch.no_grad():
            # with torch.cuda.amp.autocast():
            return self.model(x).cpu().numpy()


served_models = (
    'resnet18', 'resnet34',
    'mobilenet_v2', 'alexnet',
    'shufflenet_v2_x0_5', 'shufflenet_v2_x1_0',
    'squeezenet1_0', 'squeezenet1_1')


class InferenceHost:
    def __init__(self):
        # Imagine this is a data labeling task, and the user have loaded a bunch of images,
        # cached in memory. Because it is interactive,
        # 1) The user can choose any set of images.
        # 2) The user can choose any set of models for ensembling.
        self.images = torch.rand(128, 3, 256, 256)

        self.models = []
        # models.quantization

        # torchvision has an issue of too much loading time of 'inception_v3'
        # VGG16 is super slow compared to other models.
        for model_name in served_models:
            # TODO: distribute workers to different nodes.
            self.models.append(ModelWorker.remote(model_name))

    def __call__(self, request):
        # convert torch tensor to numpy speeds up Ray.
        # The original serialization of pytorch tensor would be way too slow.
        x = self.images.numpy()
        results = ray.get([m.inference.remote(x) for m in self.models])
        cls = np.argmax(sum(results), 1)
        return str(cls.tolist())


if __name__ == "__main__":
    ray.init(address='auto')
    client = serve.start()
    for ep in client.list_endpoints().keys():
        client.delete_endpoint(ep)
    for backend in client.list_backends():
        client.delete_backend(backend)

    # Form a backend from our class and connect it to an endpoint.
    client.create_backend("ray_backend", InferenceHost)
    client.create_endpoint("ray_endpoint", backend="ray_backend", route="/inference")

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
    # > {"count": 1}
    print(ray.get(client.get_handle("ray_endpoint").remote()))
