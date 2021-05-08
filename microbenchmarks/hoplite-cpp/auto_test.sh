#!/bin/bash
for test_name in multicast reduce gather allreduce; do
  for num_nodes in 4 8 12 16; do
    for sz in 10 15 20 25 30; do
      obj_size=$((2**$sz))
      ./run_test.sh ${test_name} $num_nodes $obj_size 5
      sleep 1
    done
  done
done
