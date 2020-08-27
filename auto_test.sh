#!/bin/bash
if [ "$#" -lt 1 ]; then echo "ERROR: number of tests"; exit; fi
if [ "$#" -gt 2 ]; then echo "ERROR: too many arguments: $#"; exit; fi

n_trials=$1

mkdir -p mpi_log

for num_nodes in 4 8 12 16
do

for test_name in multicast reduce allreduce gather; do
  for i in 10 15 20 25 30; do
      obj_size=$((2**$i))
      ./run_test.sh ${test_name}_test $num_nodes $obj_size $n_trials
      pushd mpi
      for test_index in `seq 1 $n_trials`; do
        ./mpi_run_test.sh $test_name $num_nodes $obj_size > ../mpi_log/$test_name-$num_nodes-$obj_size-$test_index
      done
      popd
      sleep 0.1
  done
done

done
