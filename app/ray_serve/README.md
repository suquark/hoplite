# Ray Serve Experiments

## Setup

```
pip install -r requirements.txt
```

`cluster.yaml` includes Ray cluster settings.

## Run

`${scale}*8` GPU nodes are required for experiments

Baseline: `python model_ensembling.py ${scale}`

With Hoplite: `python hoplite_model_ensembling.py ${scale}`

Baseline + fault tolerance test: `python model_ensembling_fault_tolerance.py 1`

With Hoplite + fault tolerance test: `python hoplite_model_ensembling_fault_tolerance.py.py 1`
