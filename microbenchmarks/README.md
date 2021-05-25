# Reproducing Hoplite Microbenchmarks on AWS

_(About 180 min)_

This microbenchmark includes Figure 7 at Section 5.1, Figure 13 at Appendix A.

First, run the microbenchmarks and collect data. _(About 30 min)_

We assume your working directory is the directory of the current README file.

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
