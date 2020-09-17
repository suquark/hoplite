import os
import torch
import torchvision
from torch import nn
from torch.nn import functional as F
from torchvision import datasets, transforms
from filelock import FileLock
import numpy as np

MODEL_DICT = {
    "alexnet": torchvision.models.alexnet,
    "vgg16": torchvision.models.vgg16,
    "inception":  torchvision.models.inception_v3,
    "resnet50": torchvision.models.resnet50,
}


def criterion(output, target):
    return F.nll_loss(output, target)


def evaluate(model, test_loader):
    """Evaluates the accuracy of the model on a validation dataset."""
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for batch_idx, (data, target) in enumerate(test_loader):
            # This is only set to finish evaluation faster.
            if batch_idx * len(data) > 1024:
                break
            outputs = model(data)
            _, predicted = torch.max(outputs.data, 1)
            total += target.size(0)
            correct += (predicted == target).sum().item()
    return 100. * correct / total


def get_data_loader():
    """Safely downloads data. Returns training/validation set dataloader."""
    mnist_transforms = transforms.Compose(
        [transforms.ToTensor(),
         transforms.Normalize((0.1307, ), (0.3081, ))])

    # We add FileLock here because multiple workers will want to
    # download data, and this may cause overwrites since
    # DataLoader is not threadsafe.
    with FileLock(os.path.expanduser("~/efs/data.lock")):
        train_loader = torch.utils.data.DataLoader(
            datasets.MNIST(
                "~/efs/dataset",
                train=True,
                download=True,
                transform=mnist_transforms),
            batch_size=8,
            shuffle=True)
        test_loader = torch.utils.data.DataLoader(
            datasets.MNIST("~/efs/dataset", train=False, transform=mnist_transforms),
            batch_size=8,
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

    def __init__(self, model_type="custom"):
        super(ConvNet, self).__init__()
        self.model_type = model_type
        if model_type == "custom":
            self.conv1 = nn.Conv2d(1, 3 * 64, kernel_size=3)
            self.fc1 = nn.Linear(192 * 64, 2048)
            self.fc2 = nn.Linear(2048, 10)
        else:
            self.model = MODEL_DICT[model_type]()

        self.weights_info = []
        for p in self.parameters():
            self.weights_info.append(
                (p.numel() * p.element_size(), tuple(p.shape)))
        self.total_gradient_size = 0
        self.n_param = 0
        for p in self.parameters():
            if p.requires_grad:
                self.n_param += p.numel()
                self.total_gradient_size += p.numel() * p.element_size()
        print("model size:", self.total_gradient_size)

    def buffer_to_tensors(self, buffer):
        tensors = []
        cursor = 0
        view = memoryview(buffer)
        for data_size, data_shape in self.weights_info:
            tensor_view = view[cursor: cursor+data_size]
            t = np.frombuffer(tensor_view, dtype=np.float32).reshape(data_shape)
            tensors.append(t)
            cursor += data_size
        return tensors

    def forward(self, x):
        if self.model_type == "custom":
            x = F.relu(F.max_pool2d(self.conv1(x), 3))
            x = x.view(-1, 192 * 64)
            x = F.relu(self.fc1(x))
            x = self.fc2(x)
        else:
            x = self.model(x)
        return F.log_softmax(x, dim=1)

    def get_weights(self):
        return {k: v.cpu() for k, v in self.state_dict().items()}

    def set_parameters(self, parameters):
        for w, p in zip(self.parameters(), parameters):
            w.data = torch.from_numpy(p).to(w.data.device)

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
                if p.grad is not None:
                    p.grad = torch.from_numpy(g).to(p.grad.device)
                else:
                    p.grad = torch.from_numpy(g)
