#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

./cleanup_dask.sh
./run_dask.sh 16 &
python auto_dask_benchmark.py 5
