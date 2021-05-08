#!/bin/bash

for test_name in multicast reduce gather allreduce; do
  for num_nodes in 4 8 12 16; do
    for sz in 10 15 20 25 30; do
        for i in `seq 10`; do
          obj_size=$((2**$sz))
          ./run_test.sh ${test_name} $num_nodes $obj_size
        done
    done
  done
done
