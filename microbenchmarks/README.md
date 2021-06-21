# Reproducing Hoplite Microbenchmarks on AWS

_(About 210 min)_

## Cluster Setup

_(About 30 min)_

If you are provided with an AWS IAM account & pre-built binaries
* If you just want to review figures & raw experimental data, see [cluster-config-access-results-only](cluster-config-access-results-only).
* If you also want to reproduce all results from the beginning, see [cluster-config-with-ami](cluster-config-with-ami) for setting up a cluster.

If you are not provided with an AWS account or you want to build everything from scratch, see [cluster-config](cluster-config).

## Roundtrip Microbenchmarks (Figure 6 at Section 5.1)

_(About 15 min)_

We assume your working directory is the directory of the current README file. Here is how you benchmark Hoplite and baselines:

### Hoplite _(about 2 min)_

```bash
pushd hoplite-python
for i in `seq 5`; do
./run_test.sh roundtrip 2 $[2**10]
./run_test.sh roundtrip 2 $[2**20]
./run_test.sh roundtrip 2 $[2**30]
done
python parse_roundtrip_result.py --verbose
popd
```

Results are saved in `hoplite-roundtrip.csv`.

### OpenMPI _(about 2 min)_

```bash
pushd mpi-cpp
for i in `seq 5`; do
./run_test.sh roundtrip 2 $[2**10]
./run_test.sh roundtrip 2 $[2**20]
./run_test.sh roundtrip 2 $[2**30]
done
python parse_roundtrip_result.py --verbose
popd
```

Results are saved in `mpi-roundtrip.csv`.

### Dask _(about 5 min)_

```bash
pushd dask-python
./dask_roundtrip.sh  # => dask-roundtrip.csv
popd
```

Results are saved in `dask-roundtrip.csv`.


### Ray _(about 2 min)_

```bash
pushd ray-python
python ray_roundtrip.py  # => ray-roundtrip.csv
popd
```

Results are saved in `ray-roundtrip.csv`.


### Merge results _(about 1 min)_

After generating all results, we can merge them into a single file:

```bash
echo "Method,Object Size (in bytes),Average RTT (s),Std RTT (s)" > roundtrip-results.csv
cat */*-roundtrip.csv >> roundtrip-results.csv
```

All results are saved in `roundtrip-results.csv`.

### Plot Figures _(about 2 min)_

After merging all results, we provide a script to visualize them.

We assume your working directory is the directory of the current README file. Here is how you generate figures in the paper:

```bash
python plot_rtt.py
```

This script generates three PDF files under the working directory. `RTT1K.pdf` corresponds to Figure 6 (a), `RTT1M.pdf` corresponds to Figure 6 (b), and `RTT1G.pdf` corresponds to Figure 6 (c).

You can download PDF files to your local machine using Ray cluster utils, for example:

```bash
ray rsync-down cluster.yaml /home/ubuntu/efs/hoplite/microbenchmarks/RTT1K.pdf .
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

### Plot Figures _(about 2 min)_

After generating all results, we provide a script to visualize them.

We assume your working directory is the directory of the current README file. Here is how you generate figures in the paper:

```bash
python draw_collective_communication.py
```

This script generates two PDF files under the working directory. `microbenchmarks-large.pdf` corresponds to Figure 7 at Section 5.1, and `microbenchmarks-small.pdf` corresponds to Figure 13 at Appendix A.

You can download PDF files to your local machine using Ray cluster utils, for example:

```bash
ray rsync-down cluster.yaml /home/ubuntu/efs/hoplite/microbenchmarks/microbenchmarks-large.pdf .
```
