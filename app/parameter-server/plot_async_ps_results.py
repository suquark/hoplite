import numpy as np
from matplotlib import pyplot as plt

from .result_parser import parser_ray, parser_async_ps_hoplite

MODELS = ['alexnet', 'vgg16', 'resnet50']


def parse_data(n_nodes):
    ray_mean = []
    ray_std = []
    hoplite_mean = []
    hoplite_std = []
    for model in MODELS:
        mean, std = parser_ray(f"ps-log/async-ps-{n_nodes}-{model}-ray.log")
        ray_mean.append(mean)
        ray_std.append(std)
        mean, std = parser_async_ps_hoplite(f"ps-log/async-ps-{n_nodes}-{model}-hoplite.log")
        hoplite_mean.append(mean)
        hoplite_std.append(std)
    return ray_mean, ray_std, hoplite_mean, hoplite_std


def draw_async_ps_results(n_nodes):
    ray_mean, ray_std, hoplite_mean, hoplite_std = parse_data(n_nodes)
    colors = (
        plt.get_cmap('tab20c')(0 * 4 + 1),
        plt.get_cmap('tab20c')(1 * 4 + 2),
        plt.get_cmap('tab20')(11),
        plt.get_cmap('tab20c')(2 * 4 + 2),
    )

    ind = np.array(range(3))
    width = 0.3

    plt.bar(ind, hoplite_mean, width, label='Hoplite', color=colors[0])
    plt.errorbar(ind, hoplite_mean, yerr=hoplite_std, linewidth=0, elinewidth=1.5, color='#444444', capthick=1.5, capsize=6)

    plt.bar(ind + width, ray_mean, width, label='Ray', color=colors[3])
    plt.errorbar(ind + width, ray_mean, yerr=ray_std, linewidth=0, elinewidth=1.5, color='#444444', capthick=1.5, capsize=6)

    plt.xticks(ind + width/2, ["AlexNet", "VGG-16", "ResNet50"], fontsize=20)
    plt.yticks(fontsize=20)
    plt.ylabel('Throughput\n(samples/s)', fontsize=20)
    plt.ylim(0, 2000)
    plt.legend(fontsize=20)
    plt.tight_layout()
    plt.savefig(f'async_training_{n_nodes}.pdf')


if __name__ == '__main__':
    draw_async_ps_results(16)
    draw_async_ps_results(8)
