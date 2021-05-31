import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import ScalarFormatter
from matplotlib.transforms import Bbox
import pandas as pd




exit()


def draw_microbenchmark_large():
  fig, axes = plt.subplots(3, 5, figsize=(15.5, 6), sharex='all')
  plt.setp(axes, xticks=[4, 8, 12, 16])
  plt.setp(axes[-1], xlabel="Number of Nodes")
  plt.setp(axes[:, 0], ylabel="Latency (s)")
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
  color_dict = {}
  n_color = 0
  color_map = plt.get_cmap('tab20')
  color_list = ["#60ACFC", "#21C2DB", "#62D5B2", "#D4EC59", "#FEB64D", "#FA816D", "#D15B7F"]
  for i, task in enumerate(['multicast', 'gather', 'reduce', 'allreduce_slow', 'allreduce_fast']):
    for j, object_size in enumerate([1048576, 33554432, 1073741824]):
      if task == 'multicast':
        hoplite = [(l[1], l[3], l[4]) for l in hoplite_results if l[0] == task and l[2] == object_size]
        mpi = [(l[1], l[3], l[4]) for l in mpi_results if l[0] == task and l[2] == object_size]
        gloo = [(l[1], l[3], l[4]) for l in gloo_results if l[0] == task and l[2] == object_size]
        ray = [(l[1], l[3], l[4]) for l in ray_results if l[0] == task and l[2] == object_size]
        dask = [(l[1], l[3], l[4]) for l in dask_results if l[0] == task and l[2] == object_size]
        all_lines = [
          ("Hoplite", sorted(hoplite)), 
          ("OpenMPI", sorted(mpi)), 
          ("Ray", sorted(ray)), 
          ("Dask", sorted(dask)),
          ("Gloo (Broadcast)", sorted(gloo)),
        ]
      elif task == 'gather' or task == 'reduce':
        hoplite = [(l[1], l[3], l[4]) for l in hoplite_results if l[0] == task and l[2] == object_size]
        mpi = [(l[1], l[3], l[4]) for l in mpi_results if l[0] == task and l[2] == object_size]
        ray = [(l[1], l[3], l[4]) for l in ray_results if l[0] == task and l[2] == object_size]
        dask = [(l[1], l[3], l[4]) for l in dask_results if l[0] == task and l[2] == object_size]
        all_lines = [
          ("Hoplite", sorted(hoplite)),
          ("OpenMPI", sorted(mpi)), 
          ("Ray", sorted(ray)), 
          ("Dask", sorted(dask)), 
        ]
      elif task == 'allreduce_slow':
        hoplite = [(l[1], l[3], l[4]) for l in hoplite_results if l[0] == "allreduce" and l[2] == object_size]
        ray = [(l[1], l[3], l[4]) for l in ray_results if l[0] == "allreduce" and l[2] == object_size]
        dask = [(l[1], l[3], l[4]) for l in dask_results if l[0] == "allreduce" and l[2] == object_size]      
        all_lines = [
          ("Hoplite", sorted(hoplite)),
          ("Ray", sorted(ray)), 
          ("Dask", sorted(dask)), 
          # ("Gloo (Ring)", sorted(gloo_ring)), 
        ]
      elif task == 'allreduce_fast':
        hoplite = [(l[1], l[3], l[4]) for l in hoplite_results if l[0] == "allreduce" and l[2] == object_size]
        mpi = [(l[1], l[3], l[4]) for l in mpi_results if l[0] == "allreduce" and l[2] == object_size]
        # gloo_ring = [(l[1], l[3], l[4]) for l in gloo_results if l[0] == "allreduce_ring" and l[2] == object_size]
        gloo_ring_chunked = [(l[1], l[3], l[4]) for l in gloo_results if l[0] == "allreduce_ring_chunked" and l[2] == object_size]
        gloo_halving_doubling = [(l[1], l[3], l[4]) for l in gloo_results if l[0] == "allreduce_halving_doubling" and l[2] == object_size]
        # gloo_bcube = [(l[1], l[3], l[4]) for l in gloo_results if l[0] == "allreduce_bcube" and l[2] == object_size]  
        all_lines = [
          ("Hoplite", sorted(hoplite)),
          ("OpenMPI", sorted(mpi)), 
          ("Gloo (Ring Chunked)", sorted(gloo_ring_chunked)), 
          ("Gloo (Halving Doubling)", sorted(gloo_halving_doubling)), 
          # ("Gloo (Bcube)", sorted(gloo_bcube)), 
        ]
      ax = axes[j][i]
      for name, data in all_lines:
        axis = [x[0] for x in data]
        mean = [x[1] for x in data]
        err = [x[2] for x in data]
        # print(name, object_size, axis, mean)
        # ax.plot(axis, mean, label=name)
        scalar_formatter = ScalarFormatter(useMathText=True)
        scalar_formatter.set_powerlimits((-1, 1))
        ax.yaxis.set_major_formatter(scalar_formatter)
        if name in color_dict:
          ax.errorbar(axis, mean, yerr = err, linewidth = 1, elinewidth = 1, capsize= 2, color = color_dict[name])
        else:
          color_dict[name] = color_map(n_color * 2)
          n_color += 1
          ax.errorbar(axis, mean, yerr = err, linewidth = 1, elinewidth = 1, capsize= 2, label=name, color = color_dict[name])
      ax.set_title(" ".join([task_dict[task], size_dict[object_size]]))
  fig.legend(loc="upper center", ncol=7, bbox_to_anchor=(0.5, 1.06), fontsize=12.5)
  fig.tight_layout()
  fig.savefig("microbenchmarks-large.pdf", bbox_inches=Bbox([[0, 0], [16, 6.5]]))
  plt.show()



def draw_microbenchmark_small():
  fig, axes = plt.subplots(2, 5, figsize=(15.5, 4), sharex='all')
  plt.setp(axes, xticks=[4, 8, 12, 16])
  plt.setp(axes[-1], xlabel="Number of Nodes")
  plt.setp(axes[:, 0], ylabel="Latency (s)")
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
  color_dict = {}
  n_color = 0
  color_map = plt.get_cmap('tab20')
  color_list = ["#60ACFC", "#21C2DB", "#62D5B2", "#D4EC59", "#FEB64D", "#FA816D", "#D15B7F"]
  for i, task in enumerate(['multicast', 'gather', 'reduce', 'allreduce_slow', 'allreduce_fast']):
    for j, object_size in enumerate([1024, 32768]):
      if task == 'multicast':
        hoplite = [(l[1], l[3], l[4]) for l in hoplite_results if l[0] == task and l[2] == object_size]
        mpi = [(l[1], l[3], l[4]) for l in mpi_results if l[0] == task and l[2] == object_size]
        gloo = [(l[1], l[3], l[4]) for l in gloo_results if l[0] == task and l[2] == object_size]
        ray = [(l[1], l[3], l[4]) for l in ray_results if l[0] == task and l[2] == object_size]
        dask = [(l[1], l[3], l[4]) for l in dask_results if l[0] == task and l[2] == object_size]
        all_lines = [
          ("Hoplite", sorted(hoplite)), 
          ("OpenMPI", sorted(mpi)), 
          ("Ray", sorted(ray)), 
          ("Dask", sorted(dask)),
          ("Gloo (Broadcast)", sorted(gloo)),
        ]
      elif task == 'gather' or task == 'reduce':
        hoplite = [(l[1], l[3], l[4]) for l in hoplite_results if l[0] == task and l[2] == object_size]
        mpi = [(l[1], l[3], l[4]) for l in mpi_results if l[0] == task and l[2] == object_size]
        ray = [(l[1], l[3], l[4]) for l in ray_results if l[0] == task and l[2] == object_size]
        dask = [(l[1], l[3], l[4]) for l in dask_results if l[0] == task and l[2] == object_size]
        all_lines = [
          ("Hoplite", sorted(hoplite)),
          ("OpenMPI", sorted(mpi)), 
          ("Ray", sorted(ray)), 
          ("Dask", sorted(dask)), 
        ]
      elif task == 'allreduce_slow':
        hoplite = [(l[1], l[3], l[4]) for l in hoplite_results if l[0] == "allreduce" and l[2] == object_size]
        ray = [(l[1], l[3], l[4]) for l in ray_results if l[0] == "allreduce" and l[2] == object_size]
        dask = [(l[1], l[3], l[4]) for l in dask_results if l[0] == "allreduce" and l[2] == object_size]      
        all_lines = [
          ("Hoplite", sorted(hoplite)),
          ("Ray", sorted(ray)), 
          ("Dask", sorted(dask)), 
          # ("Gloo (Ring)", sorted(gloo_ring)), 
        ]
      elif task == 'allreduce_fast':
        hoplite = [(l[1], l[3], l[4]) for l in hoplite_results if l[0] == "allreduce" and l[2] == object_size]
        mpi = [(l[1], l[3], l[4]) for l in mpi_results if l[0] == "allreduce" and l[2] == object_size]
        gloo_ring = [(l[1], l[3], l[4]) for l in gloo_results if l[0] == "allreduce_ring" and l[2] == object_size]
        gloo_ring_chunked = [(l[1], l[3], l[4]) for l in gloo_results if l[0] == "allreduce_ring_chunked" and l[2] == object_size]
        gloo_halving_doubling = [(l[1], l[3], l[4]) for l in gloo_results if l[0] == "allreduce_halving_doubling" and l[2] == object_size]
        gloo_bcube = [(l[1], l[3], l[4]) for l in gloo_results if l[0] == "allreduce_bcube" and l[2] == object_size]  
        all_lines = [
          ("Hoplite", sorted(hoplite)),
          ("OpenMPI", sorted(mpi)), 
          ("Gloo (Ring Chunked)", sorted(gloo_ring_chunked)), 
          ("Gloo (Halving Doubling)", sorted(gloo_halving_doubling)), 
          # ("Gloo (Bcube)", sorted(gloo_bcube)), 
        ]
      ax = axes[j][i]
      for name, data in all_lines:
        axis = [x[0] for x in data]
        mean = [x[1] for x in data]
        err = [x[2] for x in data]
        # print(name, object_size, axis, mean)
        # ax.plot(axis, mean, label=name)
        scalar_formatter = ScalarFormatter(useMathText=True)
        scalar_formatter.set_powerlimits((-1, 1))
        ax.yaxis.set_major_formatter(scalar_formatter)
        if name in color_dict:
          ax.errorbar(axis, mean, yerr = err, linewidth = 1, elinewidth = 1, capsize= 2, color = color_dict[name])
        else:
          color_dict[name] = color_map(n_color * 2)
          n_color += 1
          ax.errorbar(axis, mean, yerr = err, linewidth = 1, elinewidth = 1, capsize= 2, label=name, color = color_dict[name])
      ax.set_title(" ".join([task_dict[task], size_dict[object_size]]))
  fig.legend(loc="upper center", ncol=7, bbox_to_anchor=(0.5, 1.08), fontsize=12.5)
  fig.tight_layout()
  fig.savefig("microbenchmarks-small.pdf", bbox_inches=Bbox([[0, 0], [16, 4.5]]))
  plt.show()

draw_microbenchmark_large()
draw_microbenchmark_small()
