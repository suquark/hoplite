# Ray Serve Experiments

## Setup

At the root of the hoplite project,

```
./fornode pip install -r $(realpath app/ray_serve/requirements.txt)
```

`cluster.yaml` includes Ray cluster settings.

## Run

`${scale}*8+1` GPU nodes are required for experiments

Baseline: `python model_ensembling.py ${scale}`

With Hoplite: `python hoplite_model_ensembling.py ${scale}`

Baseline + fault tolerance test: `python model_ensembling_fault_tolerance.py 1`

With Hoplite + fault tolerance test: `python hoplite_model_ensembling_fault_tolerance.py.py 1`

## Notes

The initial run will be extremely slow on AWS due to python generating caching files. This might affect the performance measurement by a bit. Run it for a second time if necessary.
