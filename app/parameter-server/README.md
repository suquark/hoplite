# Reproducing Hoplite Parameter Server Experiments on AWS

_(About 55 min)_

## Cluster Setup

_(About 30 min)_

See [cluster-config](../ray_serve/cluster-config) for setting up a cluster to reproduce parameter server experiments in the paper.

## Asynchronous Parameter Server Experiments (Section 5.2, Figure 9)

_(About 15 min)_

After logging in to the configured cluster, *chdir* to the current directory in the hoplite repo.

In the current directory, run

```bash
./parameter-server/run_async_ps_tests.sh
```

After the script completes, results are saved under `ps-log`.

To visualize the results, run

```bash
python plot_async_ps_results.py
```

This generates 2 PDF files: `async_training_8.pdf` corresponds to Figure 9(a), and `async_training_16.pdf` corresponds to Figure 9(b).

You can download PDF files to your local machine using Ray cluster utils, for example:

```bash
ray rsync-down cluster.yaml /home/ubuntu/efs/hoplite/app/parameter-server/async_training_8.pdf .
```

## Asynchronous Parameter Server Fault Tolerance Experiments (Section 5.5, Figure 12b)

_(About 10 min)_

After logging in to the configured cluster, *chdir* to the current directory in the hoplite repo.

In the current directory, run

```bash
./run_async_ps_fault_tolerance.sh
```

The script generates `ray_asgd_fault_tolerance.json` and `hoplite_asgd_fault_tolerance.json` after running.

Run `python analyze_fault_tolerance.py` to compare the failure detection latency (see section 5.5 in the paper).

## Notes

The initial run will be extremely slow on AWS due to python generating caching files etc (about 5 min). This is totally normal.
