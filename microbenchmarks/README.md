# Reproducing Hoplite Microbenchmarks on AWS

_(About 210 min)_

## Cluster Setup

_(About 30 min)_

See [cluster-config](cluster-config) for setting up a cluster to reproduce the microbenchmarks in the paper.

## Roundtrip Microbenchmarks (Figure 6 at Section 5.1)

_(About 5 min)_

We assume your working directory is the directory of the current README file. Run this script to benchmark Hoplite and baselines:

```bash
./roundtrip.sh
```

## Collective Communication Microbenchmarks (Figure 7 at Section 5.1, Figure 13 at Appendix A)

_(About 180 min)_

We assume your working directory is the directory of the current README file. Here is how you benchmark Hoplite and baselines:

### OpenMPI _(about 30 min)_

```bash
pushd mpi-cpp
./auto_test.sh
python parse_result.py --verbose
popd
```

Results are saved in `mpi_results.csv`.

### Hoplite _(about 15 min)_

```bash
pushd hoplite-cpp
./auto_test.sh
python parse_result.py --verbose
popd
```

Results are saved in `hoplite_results.csv`.

### Gloo _(about 20 min)_

```bash
pushd gloo-cpp
./install_gloo.sh
./auto_test.sh
python parse_result.py --verbose
popd
```

Results are saved in `gloo_results.csv`.

### Ray _(about 25 min)_

```bash
pushd ray-python
make
./auto_test.sh
popd
```

Results are saved in `ray-microbenchmark.csv`.

### Dask _(about 80 min)_

```bash
pushd dask-python
./auto_test.sh
python parse_result.py --verbose
popd
```

Results are saved in `dask_results.csv`.
