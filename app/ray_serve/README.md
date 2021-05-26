# Reproducing ML Model Serving Experiments on AWS

_(About 30 min)_

## Setup 

_(About 15 min)_

See [cluster-config](cluster-config).

## ML model serving experiments (Figure 11)

After logging in to the configured cluster, *chdir* to the current directory in the hoplite repo.

Here is how you run the experiments:

**Baseline** _(2-3 min)_: `python model_ensembling.py ${scale}`

**Hoplite** _(1-2 min)_: `python hoplite_model_ensembling.py ${scale}`

`${scale}` controls the cluster size. `scale=1` corresponds to 8 GPU nodes, `scale=2` corresponds to 16 GPU nodes in the figure.

The script prints the mean and std of throughput (queries/s) at the end.

## ML Model Serving fault tolerance experiments (Figure 12a)

Baseline + fault tolerance test _(About 2 min)_: `python model_ensembling_fault_tolerance.py 1`

With Hoplite + fault tolerance test _(About 2 min)_: `python hoplite_model_ensembling_fault_tolerance.py.py 1`

Run `python analyze_fault_tolerance.py` to compare the failure detection latency (see section 5.5 in the paper).

## Notes

The initial run will be extremely slow on AWS due to python generating caching files etc (about 4 min). This is totally normal.
