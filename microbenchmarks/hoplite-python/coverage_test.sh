#!/bin/bash

for test_index in `seq 1 3`; do
./run_test.sh roundtrip 2 $[2**25]
./run_test.sh roundtrip 2 $[2**15]
done

num_nodes=8

for test_name in multicast reduce gather allreduce allgather; do
  for i in 15 25; do
    for test_index in `seq 1 3`; do
      obj_size=$((2**$i))
      echo $test_name-$num_nodes-$obj_size-$test_index
      ./run_test.sh ${test_name} $num_nodes $obj_size
      sleep 1
    done
done

done
