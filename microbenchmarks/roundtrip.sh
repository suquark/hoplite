#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

pushd dask-python
./dask_roundtrip.sh  # => dask-roundtrip.csv
popd

pushd ray-python
python ray_roundtrip.py  # => ray-roundtrip.csv
popd

pushd hoplite-python
for i in `seq 5`; do
./run_test.sh roundtrip 2 $[2**10]
./run_test.sh roundtrip 2 $[2**20]
./run_test.sh roundtrip 2 $[2**30]
done
popd

pushd mpi-cpp
for i in `seq 5`; do
./mpi_roundtrip.sh $[2**10]
./mpi_roundtrip.sh $[2**20]
./mpi_roundtrip.sh $[2**30]
done
popd
