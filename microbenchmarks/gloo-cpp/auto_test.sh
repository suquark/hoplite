#!/bin/bash
if [ "$#" -lt 1 ]; then echo "ERROR: rounds of benchmarks (for calculating mean and std)"; exit; fi
if [ "$#" -gt 2 ]; then echo "ERROR: too many arguments: $#"; exit; fi

for num_nodes in 4 8 12 16
do

for test_name in allreduce_ring allreduce_ring_chunked allreduce_halving_doubling allreduce_bcube broadcast_one_to_all; do
  for i in 10 15 20 25 30; do
    for test_index in `seq 1 $1`; do
      obj_size=$((2**$i))
      ./run_benchmark.sh $test_name $num_nodes $obj_size
      sleep 0.1
    done
  done
done

done
