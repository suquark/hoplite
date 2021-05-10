#!/bin/bash

trap "exit" INT

num_nodes=8

for test_name in multicast reduce gather allreduce allgather; do
  for i in 15 25; do
      obj_size=$((2**$i))
      echo $test_name-$num_nodes-$obj_size
      ./run_test.sh $test_name $num_nodes $obj_size 3
      sleep 1
  done
done
