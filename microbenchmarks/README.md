# Reproducing Hoplite Microbenchmarks on AWS

_(About 120 min)_

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

### Hoplite _(about 15 min)_

```bash
pushd hoplite-cpp
./auto_test.sh
python parse_result.py --verbose
popd
```

### Gloo _(about 20 min)_

```bash
pushd gloo-cpp
./install_gloo.sh
./auto_test.sh
python parse_result.py --verbose
popd
```

### Ray _(about ? min)_

TODO: fix the problem that Ray stuck in the middle.

### Dask _(about ? min)_

TODO: run dask.
