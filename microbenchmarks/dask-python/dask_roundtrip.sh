#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

./cleanup_dask.sh
./run_dask.sh 2 &
python dask_roundtrip.py
