import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import ScalarFormatter
from matplotlib.transforms import Bbox
import pandas as pd

COLUMNS_USED = ['Benchmark Name', 'Object Size (in bytes)', '#Nodes', 'Average Time (s)', 'Std Time (s)']
COLUMNS_DTYPE = [np.object, np.int64, np.int64, np.float64, np.float64]

LARGE_OBJECT_SIZES = [2**20, 2**25, 2**30]
SMALL_OBJECT_SIZES = [2**10, 2**15]
TASKS = ['multicast', 'gather', 'reduce', 'allreduce_slow', 'allreduce_fast']

size_dict = {
    1024: "1KB",
    32768: "32KB",
    1048576: "1MB",
    33554432: "32MB",
    1073741824: "1GB",
}

task_dict = {
    "multicast": "Broadcast", 
    "gather": "Gather", 
    "reduce": "Reduce", 
    "allreduce": "Allreduce", 
    "allreduce_slow": "Allreduce(i)", 
    "allreduce_fast": "Allreduce(ii)", 
}


def read_results(path):
    results = pd.read_csv(path, dtype=dict(zip(COLUMNS_USED, COLUMNS_DTYPE)))
    return results[COLUMNS_USED].sort_values(by=COLUMNS_USED)


def read_ray_results(path):
    results = pd.read_csv(
        path,
        names=['Benchmark Name', '#Nodes', 'Object Size (in bytes)', 'Average Time (s)', 'Std Time (s)'],
        dtype=dict(zip(COLUMNS_USED, COLUMNS_DTYPE)))
    return results[COLUMNS_USED].sort_values(by=COLUMNS_USED)


def filter_by_name_and_size(df, name, size):
    f_name = (df['Benchmark Name'] == name)
    f_size = (df['Object Size (in bytes)'] == size)
    return df[f_name & f_size].iloc[:, [2, 3, 4]]


def prepare_plot_data(hoplite_results, mpi_results, gloo_results, ray_results, dask_results):
    plot_data = {}
    for task in TASKS:
        for object_size in SMALL_OBJECT_SIZES + LARGE_OBJECT_SIZES:
            if task == 'multicast':
                all_lines = [
                    ("Hoplite", filter_by_name_and_size(hoplite_results, task, object_size)), 
                    ("OpenMPI", filter_by_name_and_size(mpi_results, task, object_size)), 
                    ("Ray", filter_by_name_and_size(ray_results, 'ray_' + task, object_size)), 
                    ("Dask", filter_by_name_and_size(dask_results, task, object_size)),
                    ("Gloo (Broadcast)", filter_by_name_and_size(gloo_results, 'broadcast_one_to_all', object_size)),
                ]
            elif task == 'gather' or task == 'reduce':
                all_lines = [
                    ("Hoplite", filter_by_name_and_size(hoplite_results, task, object_size)), 
                    ("OpenMPI", filter_by_name_and_size(mpi_results, task, object_size)), 
                    ("Ray", filter_by_name_and_size(ray_results, 'ray_' + task, object_size)), 
                    ("Dask", filter_by_name_and_size(dask_results, task, object_size)),
                ]
            elif task == 'allreduce_slow':
                all_lines = [
                    ("Hoplite", filter_by_name_and_size(hoplite_results, 'allreduce', object_size)), 
                    ("Ray", filter_by_name_and_size(ray_results, 'ray_' + 'allreduce', object_size)), 
                    ("Dask", filter_by_name_and_size(dask_results, 'allreduce', object_size)),
                    # ("Gloo (Ring)", ...)
                ]
            elif task == 'allreduce_fast':
                all_lines = [
                    ("Hoplite", filter_by_name_and_size(hoplite_results, 'allreduce', object_size)), 
                    ("OpenMPI", filter_by_name_and_size(mpi_results, 'allreduce', object_size)), 
                    ("Gloo (Ring Chunked)", filter_by_name_and_size(gloo_results, 'allreduce_ring_chunked', object_size)), 
                    ("Gloo (Halving Doubling)", filter_by_name_and_size(gloo_results, 'allreduce_halving_doubling', object_size)), 
                    # ("Gloo (Bcube)", ...)
                ]
            else:
                assert False
            plot_data[(task, object_size)] = all_lines
    return plot_data


def render(axes, plot_data, tasks, object_sizes):
  color_dict = {}
  n_color = 0
  color_map = plt.get_cmap('tab20')
  # color_list = ["#60ACFC", "#21C2DB", "#62D5B2", "#D4EC59", "#FEB64D", "#FA816D", "#D15B7F"]
  for i, task in enumerate(tasks):
    for j, object_size in enumerate(object_sizes):
      ax = axes[j][i]
      for name, data in plot_data[(task, object_size)]:
        axis = data['#Nodes']
        mean = data['Average Time (s)']
        err = data['Std Time (s)']
        scalar_formatter = ScalarFormatter(useMathText=True)
        scalar_formatter.set_powerlimits((-1, 1))
        ax.yaxis.set_major_formatter(scalar_formatter)
        if name in color_dict:
          ax.errorbar(axis, mean, yerr=err, linewidth=1, elinewidth=1, capsize=2, color=color_dict[name])
        else:
          color_dict[name] = color_map(n_color * 2)
          n_color += 1
          ax.errorbar(axis, mean, yerr=err, linewidth=1, elinewidth=1, capsize=2, label=name, color=color_dict[name])
      ax.set_title(" ".join([task_dict[task], size_dict[object_size]]))


def draw_microbenchmark_large(plot_data):
  fig, axes = plt.subplots(3, 5, figsize=(15.5, 6), sharex='all')
  plt.setp(axes, xticks=[4, 8, 12, 16])
  plt.setp(axes[-1], xlabel="Number of Nodes")
  plt.setp(axes[:, 0], ylabel="Latency (s)")
  render(axes, plot_data, TASKS, LARGE_OBJECT_SIZES)
  fig.legend(loc="upper center", ncol=7, bbox_to_anchor=(0.5, 1.06), fontsize=12.5)
  fig.tight_layout()
  fig.savefig("microbenchmarks-large.pdf", bbox_inches=Bbox([[0, 0], [16, 6.5]]))


def draw_microbenchmark_small(plot_data):
  fig, axes = plt.subplots(2, 5, figsize=(15.5, 4), sharex='all')
  plt.setp(axes, xticks=[4, 8, 12, 16])
  plt.setp(axes[-1], xlabel="Number of Nodes")
  plt.setp(axes[:, 0], ylabel="Latency (s)")
  render(axes, plot_data, TASKS, SMALL_OBJECT_SIZES)
  fig.legend(loc="upper center", ncol=7, bbox_to_anchor=(0.5, 1.08), fontsize=12.5)
  fig.tight_layout()
  fig.savefig("microbenchmarks-small.pdf", bbox_inches=Bbox([[0, 0], [16, 4.5]]))


if __name__ == '__main__':
    hoplite_results = read_results('hoplite-cpp/hoplite_results.csv')
    mpi_results = read_results('mpi-cpp/mpi_results.csv')
    gloo_results = read_results('gloo-cpp/gloo_results.csv')
    ray_results = read_ray_results('ray-python/ray-microbenchmark.csv')
    dask_results = read_results('dask-python/dask_results.csv')

    plot_data = prepare_plot_data(hoplite_results, mpi_results, gloo_results, ray_results, dask_results)

    draw_microbenchmark_large(plot_data)
    draw_microbenchmark_small(plot_data)
