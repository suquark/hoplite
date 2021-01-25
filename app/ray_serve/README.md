# Ray Serve Experiments

## Setup

```
pip install -r requirements.txt
```

`cluster.yaml` includes Ray cluster settings.

## Run

`${scale}*8+1` nodes are required for experiments

Baseline: `python model_ensembling.py ${scale}`

With Hoplite: `python hoplite_model_ensembling.py ${scale}`
