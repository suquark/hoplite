# Ray Serve Experiments

## Setup 

_(About 2 min)_

At the root of the hoplite project,
2
```
./fornode pip install -r $(realpath app/ray_serve/requirements.txt)
```

`cluster.yaml` includes Ray cluster settings.

## Run

`${scale}` controls the cluster size (in the paper we use 1 and 2). `${scale}*8+1` GPU nodes are required for experiments. The cluster configuration file includes 9 V100 GPU nodes for `scale=1`.

Baseline _(2-3 min)_: `python model_ensembling.py ${scale}`

With Hoplite _(1-2 min)_: `python hoplite_model_ensembling.py ${scale}`

Baseline + fault tolerance test _(About 2 min)_: `python model_ensembling_fault_tolerance.py 1`

With Hoplite + fault tolerance test _(About 2 min)_: `python hoplite_model_ensembling_fault_tolerance.py.py 1`



## Notes

The initial run will be extremely slow on AWS due to python generating caching files (about 4 min). This might affect the performance measurement by a bit. Run it for a second time if necessary.
