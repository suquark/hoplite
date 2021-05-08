#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

./cleanup_dask.sh
./run_dask.sh &
python auto_dask_benchmark.py 5
