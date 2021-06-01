import matplotlib.pyplot as plt
import pandas as pd

COLUMNS = ["Method", "Object Size (in bytes)", "Average Time (s)" ,"Std Time (s)"]
LABELS = ['Optimal', 'Hoplite', 'OpenMPI', 'Ray', 'Dask']
COLORS = (
    plt.get_cmap('tab20c')(4 * 4 + 2),
    plt.get_cmap('tab20c')(0 * 4 + 1),
    plt.get_cmap('tab20c')(1 * 4 + 2),
    plt.get_cmap('tab20c')(2 * 4 + 2),
    plt.get_cmap('tab20')(3 * 2 + 1),
)


def draw_rtt_1K(results):
  SIZE = 1024
  results = results[results[COLUMNS[1]] == SIZE]
  Latency, STD = results[COLUMNS[2]], results[COLUMNS[3]]

  plt.figure(figsize=(4, 4))
  ind = range(5)
  width = 0.8
  plt.bar(ind, Latency * 1000, width, label='usr', color=COLORS, linewidth=10)
  plt.errorbar(ind[1:], Latency[1:] * 1000, yerr=STD[1:] * 1000, linewidth=0, elinewidth=1.5, color='#444444', capthick=1.5, capsize=6)
  plt.xticks(ind, LABELS, fontsize=18)
  for label in plt.gca().get_xmajorticklabels():
    label.set_rotation(30)
    label.set_horizontalalignment("right")
  plt.yticks(fontsize=18)
  plt.ylabel('RTT (ms)', fontsize=18)
  plt.annotate("1.7 μs", (-0.55, 0.08), fontsize=15)
  plt.savefig('RTT1K.pdf', bbox_inches="tight")


def draw_rtt_1M(results):
  SIZE = 2 ** 20
  results = results[results[COLUMNS[1]] == SIZE]
  Latency, STD = results[COLUMNS[2]], results[COLUMNS[3]]

  plt.figure(figsize=(4, 4))
  ind = range(5)
  width = 0.8
  plt.bar(ind, Latency * 1000, width, label='usr', color=COLORS, linewidth=10)
  plt.errorbar(ind[1:], Latency[1:] * 1000, yerr=STD[1:]  * 1000, linewidth=0, elinewidth=1.5, color='#444444', capthick=1.5, capsize=6)
  plt.xticks(ind, LABELS, fontsize=18)
  for label in plt.gca().get_xmajorticklabels():
    label.set_rotation(30)
    label.set_horizontalalignment("right")
  plt.yticks(fontsize=18)
  plt.ylabel('RTT (ms)', fontsize=18)
  plt.savefig('RTT1M.pdf', bbox_inches="tight")


def draw_rtt_1G(results):
  SIZE = 2 ** 30
  results = results[results[COLUMNS[1]] == SIZE]
  Latency, STD = results[COLUMNS[2]], results[COLUMNS[3]]

  plt.figure(figsize=(4, 4))
  ind = range(5)
  width = 0.8
  plt.bar(ind, Latency, width, label='usr', color=COLORS, linewidth=10)
  plt.errorbar(ind[1:], Latency[1:], yerr=STD[1:], linewidth=0, elinewidth=1.5, color='#444444', capthick=1.5, capsize=6)
  plt.xticks(ind, LABELS, fontsize=18)
  for label in plt.gca().get_xmajorticklabels():
    label.set_rotation(30)
    label.set_horizontalalignment("right")
  plt.yticks(fontsize=18)
  plt.ylabel('RTT (s)', fontsize=18)
  plt.savefig('RTT1G.pdf', bbox_inches="tight")


if __name__ == '__main__':
    results = pd.read_csv('roundtrip-results.csv')
    results.loc[len(results.index)] = ['optimal', 1024, 0.000031690911909, 0.0]
    results.loc[len(results.index)] = ['optimal', 1048576, 0.001731493794326, 0.0]
    results.loc[len(results.index)] = ['optimal', 1073741824, 1.773049645390071, 0.0]

    cat_method_order = pd.CategoricalDtype(
        ['optimal', 'hoplite', 'mpi', 'ray', 'dask'], 
        ordered=True
    )
    results['Method'] = results['Method'].astype(cat_method_order)
    results = results.sort_values('Method')
    draw_rtt_1K(results)
    draw_rtt_1M(results)
    draw_rtt_1G(results)
