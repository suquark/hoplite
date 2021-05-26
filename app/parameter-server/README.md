# Reproducing Hoplite Parameter Server Experiments on AWS

## Cluster Setup

_(20-40 min)_

It is the same as [ML Serving](../ray_serve/cluster-config).

## Asynchronous Parameter Server Fault Tolerance Experiments (Figure 12b)

After logging in to the configured cluster, *chdir* to the current directory in the hoplite repo.

In the current directory, run

```bash
./run_async_ps_fault_tolerance.sh
```

The script generates `ray_asgd_fault_tolerance.json` and `hoplite_asgd_fault_tolerance.json` after running.

Run `python analyze_fault_tolerance.py` to compare the failure detection latency (see section 5.5 in the paper).

## Notes

The initial run will be extremely slow on AWS due to python generating caching files etc (about 5 min). This is totally normal.
